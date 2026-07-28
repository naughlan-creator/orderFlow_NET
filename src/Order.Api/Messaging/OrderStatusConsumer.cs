using System.Text.Json;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Order.Api.Data;
using Order.Api.Models;
using OrderFlow.Contracts;

namespace Order.Api.Messaging;

/// <summary>
/// Applies inventory decisions back onto the order, so an order actually leaves the
/// Pending state and callers can observe the outcome via GET /api/orders/{id}.
/// </summary>
public sealed class OrderStatusConsumer(
    IServiceScopeFactory scopeFactory,
    KafkaOptions options,
    ILogger<OrderStatusConsumer> logger) : BackgroundService
{
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
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(new[]
        {
            TopicNames.InventoryReserved,
            TopicNames.InventoryRejected
        });
        logger.LogInformation("Order status consumer started");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result = null;
                try
                {
                    result = consumer.Consume(stoppingToken);

                    if (!TryReadOutcome(result, out var orderId, out var status))
                    {
                        consumer.Commit(result);
                        continue;
                    }

                    await ApplyStatusAsync(orderId, status, stoppingToken);
                    consumer.Commit(result);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to apply an inventory outcome");
                    Rewind(consumer, result);
                    if (!await DelayAsync(TimeSpan.FromSeconds(2), stoppingToken)) break;
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private bool TryReadOutcome(ConsumeResult<string, string> result,
        out Guid orderId, out OrderStatus status)
    {
        orderId = Guid.Empty;
        status = OrderStatus.Pending;
        try
        {
            if (result.Topic == TopicNames.InventoryReserved)
            {
                var reserved = JsonSerializer.Deserialize<InventoryReserved>(
                    result.Message.Value, JsonOptions);
                if (reserved is null) return false;
                orderId = reserved.OrderId;
                status = OrderStatus.InventoryReserved;
                return true;
            }

            if (result.Topic == TopicNames.InventoryRejected)
            {
                var rejected = JsonSerializer.Deserialize<InventoryRejected>(
                    result.Message.Value, JsonOptions);
                if (rejected is null) return false;
                orderId = rejected.OrderId;
                status = OrderStatus.InventoryRejected;
                return true;
            }

            logger.LogWarning("Ignoring message from unexpected topic {Topic}", result.Topic);
            return false;
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Discarding unparseable {Topic} message at offset {Offset}",
                result.Topic, result.Offset.Value);
            return false;
        }
    }

    /// <summary>
    /// Idempotent by construction: only a Pending order is moved, so a redelivered
    /// event is a no-op rather than a status flip-flop.
    /// </summary>
    private async Task ApplyStatusAsync(Guid orderId, OrderStatus status,
        CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();

        var order = await db.Orders.SingleOrDefaultAsync(x => x.Id == orderId, cancellationToken);
        if (order is null)
        {
            logger.LogWarning("Received an inventory outcome for unknown order {OrderId}", orderId);
            return;
        }

        if (order.Status != OrderStatus.Pending)
        {
            logger.LogDebug("Order {OrderId} is already {Status}; ignoring duplicate",
                orderId, order.Status);
            return;
        }

        order.Status = status;
        await db.SaveChangesAsync(cancellationToken);
        logger.LogInformation("Order {OrderId} moved to {Status}", orderId, status);
    }

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
            logger.LogWarning(ex, "Could not rewind to offset {Offset}", result.Offset.Value);
        }
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
