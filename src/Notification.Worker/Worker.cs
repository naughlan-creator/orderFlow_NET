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
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrapServers = configuration["Kafka:BootstrapServers"]
            ?? "localhost:9092";
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
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
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
                else
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
                consumer.Commit(result);
            }
            catch (ConsumeException ex)
            {
                logger.LogError(ex, "Kafka consume failure");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
        consumer.Close();
    }
}