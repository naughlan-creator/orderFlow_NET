using System.Text.Json;
using Confluent.Kafka;
using OrderFlow.Contracts;
namespace Notification.Worker;
public sealed class Worker(
    IConfiguration configuration,
    ILogger<Worker> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);
    /// <summary>
    /// IConsumer.Consume blocks, and ExecuteAsync runs inline on the host's startup path
    /// until its first await — so consuming directly here would leave the host stuck in
    /// StartAsync. The loop gets its own long-running thread instead.
    /// </summary>
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Factory.StartNew(
            () => ConsumeLoopAsync(stoppingToken),
            stoppingToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        var bootstrapServers = configuration["Kafka:BootstrapServers"]
            ?? "localhost:19092";
        var groupId = configuration["Kafka:GroupId"]
            ?? "notification-worker";
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(new[]
        {
            TopicNames.InventoryReserved,
            TopicNames.InventoryRejected
        });
        logger.LogInformation("Notification worker started");
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);
                    Notify(result);
                    consumer.Commit(result);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // Without a catch-all here a single bad message escapes ExecuteAsync
                    // and .NET's default StopHost behaviour takes the whole worker down.
                    logger.LogError(ex, "Notification processing failure");
                    if (!await DelayAsync(TimeSpan.FromSeconds(2), stoppingToken)) break;
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private void Notify(ConsumeResult<string, string> result)
    {
        try
        {
            if (result.Topic == TopicNames.InventoryReserved)
            {
                var message = JsonSerializer.Deserialize<InventoryReserved>(
                    result.Message.Value, JsonOptions);
                if (message is not null)
                {
                    logger.LogInformation(
                        "NOTIFICATION to {Email}: inventory reserved for order {OrderId}. " +
                        "Product {ProductId}, quantity {Quantity}.",
                        message.CustomerEmail, message.OrderId,
                        message.ProductId, message.Quantity);
                }
            }
            else if (result.Topic == TopicNames.InventoryRejected)
            {
                var message = JsonSerializer.Deserialize<InventoryRejected>(
                    result.Message.Value, JsonOptions);
                if (message is not null)
                {
                    logger.LogWarning(
                        "NOTIFICATION to {Email}: order {OrderId} could not reserve inventory. " +
                        "Reason: {Reason}",
                        message.CustomerEmail, message.OrderId, message.Reason);
                }
            }
            else
            {
                logger.LogWarning("Ignoring message from unexpected topic {Topic}", result.Topic);
            }
        }
        catch (JsonException ex)
        {
            // Notifications are best effort; a malformed payload must not block the
            // partition, so log it and let the offset advance.
            logger.LogError(ex, "Discarding unparseable {Topic} message at offset {Offset}",
                result.Topic, result.Offset.Value);
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
