using Microsoft.EntityFrameworkCore;
using Order.Api.Data;
using Order.Api.Models;

namespace Order.Api.Messaging;

/// <summary>
/// Drains the transactional outbox to Kafka. Rows are claimed with
/// <c>FOR UPDATE SKIP LOCKED</c> so several API instances can publish concurrently
/// without sending the same event twice.
/// </summary>
public sealed class OutboxPublisher(
    IServiceScopeFactory scopeFactory,
    IKafkaProducer producer,
    ILogger<OutboxPublisher> logger) : BackgroundService
{
    private const int BatchSize = 50;
    private static readonly TimeSpan IdleDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan ErrorDelay = TimeSpan.FromSeconds(5);

    private const string ClaimBatchSql = """
        SELECT * FROM orders.outbox_messages
        WHERE processed_at_utc IS NULL
        ORDER BY created_at_utc
        LIMIT 50
        FOR UPDATE SKIP LOCKED
        """;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Outbox publisher started");
        while (!stoppingToken.IsCancellationRequested)
        {
            int published;
            try
            {
                published = await PublishBatchAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // Nothing is marked processed unless the whole batch commits, so every
                // unpublished row is simply retried on the next pass.
                logger.LogError(ex, "Outbox publish failed; retrying");
                if (!await DelayAsync(ErrorDelay, stoppingToken)) break;
                continue;
            }

            // Drain a backlog without pausing; only idle when there is nothing to send.
            if (published < BatchSize && !await DelayAsync(IdleDelay, stoppingToken))
                break;
        }
    }

    private async Task<int> PublishBatchAsync(CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();

        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);

        var batch = await db.OutboxMessages
            .FromSqlRaw(ClaimBatchSql)
            .ToListAsync(cancellationToken);

        if (batch.Count == 0)
        {
            await transaction.RollbackAsync(cancellationToken);
            return 0;
        }

        foreach (var message in batch)
        {
            await producer.ProduceAsync(
                message.Topic, message.MessageKey, message.Payload, cancellationToken);
            message.ProcessedAtUtc = DateTimeOffset.UtcNow;
            message.Attempts++;
        }

        await db.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return batch.Count;
    }

    private static async Task<bool> DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
