using System.Text.Json;
using Confluent.Kafka;
using Inventory.Service.Data;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Contracts;

namespace Inventory.Service.Messaging;
public sealed class OrderCreatedConsumer(
    IServiceScopeFactory scopeFactory,
    KafkaOptions options,
    ILogger<OrderCreatedConsumer> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)

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
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                var message = JsonSerializer.Deserialize<OrderCreated>(
                    result.Message.Value, JsonOptions);
                if (message is null)
                {
                    logger.LogWarning("Ignored an unreadable order.created message");
                    consumer.Commit(result);
                    continue;
                }
                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
                var item = await db.InventoryItems.SingleOrDefaultAsync(
                    x => x.ProductId == message.ProductId, stoppingToken);
                string outputTopic;
                object outputEvent;
                if (item is not null && item.AvailableQuantity >= message.Quantity)
                {
                    item.AvailableQuantity -= message.Quantity;
                    item.UpdatedAtUtc = DateTimeOffset.UtcNow;
                    await db.SaveChangesAsync(stoppingToken);
                    outputTopic = TopicNames.InventoryReserved;
                    outputEvent = new InventoryReserved(
                        Guid.NewGuid(), message.OrderId, message.ProductId,
                        message.Quantity, message.CustomerEmail,
                        DateTimeOffset.UtcNow);
                }
                else
                {
                    outputTopic = TopicNames.InventoryRejected;
                    outputEvent = new InventoryRejected(
                        Guid.NewGuid(), message.OrderId, message.ProductId,
                        message.Quantity, message.CustomerEmail,
                        item is null ? "Product does not exist." : "Insufficient stock.",
                        DateTimeOffset.UtcNow);
                }
                await producer.ProduceAsync(outputTopic,
                    new Message<string, string>
                    {
                        Key = message.OrderId.ToString(),
                        Value = JsonSerializer.Serialize(outputEvent, JsonOptions)
                    }, stoppingToken);
                consumer.Commit(result);
                logger.LogInformation(
                    "Processed order {OrderId}; published {OutputTopic}",
                    message.OrderId, outputTopic);
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
            catch (Exception ex)
            {
                logger.LogError(ex, "Inventory processing failure");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
        consumer.Close();
    }
}