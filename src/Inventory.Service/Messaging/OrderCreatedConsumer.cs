using System.Text.Json;
using Confluent.Kafka;
using Inventory.Service.Data;
using Inventory.Service.Models;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using OrderFlow.Contracts;

namespace Inventory.Service.Messaging;

public sealed class OrderCreatedConsumer(
    IServiceScopeFactory scopeFactory,
    KafkaOptions options,
    ILogger<OrderCreatedConsumer> logger) : BackgroundService
{
    private const int MaxAttempts = 5;

    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    /// <summary>
    /// IConsumer.Consume blocks, and ExecuteAsync runs inline on the host's startup path
    /// until its first await — so consuming directly here would stop Kestrel from ever
    /// starting. The loop gets its own long-running thread instead.
    /// </summary>
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Factory.StartNew(
            () => ConsumeLoopAsync(stoppingToken),
            stoppingToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();

        consumer.Subscribe(TopicNames.OrderCreated);
        logger.LogInformation("Inventory consumer subscribed to {Topic}",
            TopicNames.OrderCreated);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result = null;
                try
                {
                    result = consumer.Consume(stoppingToken);

                    OrderCreated? message;
                    try
                    {
                        message = JsonSerializer.Deserialize<OrderCreated>(
                            result.Message.Value, JsonOptions);
                    }
                    catch (JsonException ex)
                    {
                        // A malformed payload can never succeed on retry, so commit past
                        // it rather than blocking the partition forever.
                        logger.LogError(ex,
                            "Discarding unparseable {Topic} message at offset {Offset}",
                            result.Topic, result.Offset.Value);
                        consumer.Commit(result);
                        continue;
                    }

                    if (message is null)
                    {
                        logger.LogWarning("Ignored an empty order.created message");
                        consumer.Commit(result);
                        continue;
                    }

                    var outcome = await ReserveAsync(message, stoppingToken);

                    await producer.ProduceAsync(outcome.Topic,
                        new Message<string, string>
                        {
                            Key = message.OrderId.ToString(),
                            Value = outcome.Payload
                        }, stoppingToken);

                    consumer.Commit(result);
                    logger.LogInformation(
                        "Processed order {OrderId}; published {OutputTopic}",
                        message.OrderId, outcome.Topic);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Inventory processing failure");

                    // The offset was never committed, but the consumer has already moved
                    // past this message in memory. Rewind so it is retried instead of
                    // being skipped by the next successful commit.
                    Rewind(consumer, result);

                    if (!await DelayAsync(TimeSpan.FromSeconds(2), stoppingToken))
                        break;
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    /// Applies the stock change exactly once for a given event and returns the outcome
    /// to publish. Kafka delivers at least once, so a redelivered event must not
    /// decrement stock twice — but it must still republish its original decision.
    /// </summary>
    private async Task<(string Topic, string Payload)> ReserveAsync(
        OrderCreated message, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            using var scope = scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();

            var applied = await db.ProcessedEvents
                .AsNoTracking()
                .SingleOrDefaultAsync(x => x.EventId == message.EventId, cancellationToken);
            if (applied is not null)
            {
                logger.LogInformation(
                    "Event {EventId} was already applied; republishing stored outcome",
                    message.EventId);
                return (applied.OutcomeTopic, applied.OutcomePayload);
            }

            var item = await db.InventoryItems.SingleOrDefaultAsync(
                x => x.ProductId == message.ProductId, cancellationToken);

            string topic;
            object outcome;
            if (item is not null && item.AvailableQuantity >= message.Quantity)
            {
                item.AvailableQuantity -= message.Quantity;
                item.UpdatedAtUtc = DateTimeOffset.UtcNow;
                topic = TopicNames.InventoryReserved;
                outcome = new InventoryReserved(
                    Guid.NewGuid(), message.OrderId, message.ProductId,
                    message.Quantity, message.CustomerEmail, DateTimeOffset.UtcNow);
            }
            else
            {
                topic = TopicNames.InventoryRejected;
                outcome = new InventoryRejected(
                    Guid.NewGuid(), message.OrderId, message.ProductId,
                    message.Quantity, message.CustomerEmail,
                    item is null ? "Product does not exist." : "Insufficient stock.",
                    DateTimeOffset.UtcNow);
            }

            var payload = JsonSerializer.Serialize(outcome, JsonOptions);
            db.ProcessedEvents.Add(new ProcessedEvent
            {
                EventId = message.EventId,
                OutcomeTopic = topic,
                OutcomePayload = payload,
                ProcessedAtUtc = DateTimeOffset.UtcNow
            });

            try
            {
                // A single SaveChanges is a single transaction: the stock decrement and
                // the dedup record commit together, or neither does.
                await db.SaveChangesAsync(cancellationToken);
                return (topic, payload);
            }
            catch (DbUpdateConcurrencyException) when (attempt < MaxAttempts)
            {
                logger.LogWarning(
                    "Concurrent stock update on {ProductId}; attempt {Attempt} of {Max}",
                    message.ProductId, attempt, MaxAttempts);
            }
            catch (DbUpdateException ex) when (IsDuplicateKey(ex) && attempt < MaxAttempts)
            {
                logger.LogInformation(
                    "Event {EventId} was applied concurrently; reloading its outcome",
                    message.EventId);
            }
        }
    }

    private static bool IsDuplicateKey(DbUpdateException ex) =>
        ex.InnerException is PostgresException
        {
            SqlState: PostgresErrorCodes.UniqueViolation
        };

    private void Rewind(IConsumer<string, string> consumer,
        ConsumeResult<string, string>? result)
    {
        if (result is null) return;
        try
        {
            consumer.Seek(result.TopicPartitionOffset);
        }
        catch (KafkaException ex)
        {
            // The partition may have been revoked; the uncommitted offset means the
            // message is redelivered to whichever consumer picks the partition up.
            logger.LogWarning(ex, "Could not rewind to offset {Offset}",
                result.Offset.Value);
        }
    }

    /// <summary>Delay that reports cancellation instead of throwing out of the loop.</summary>
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
