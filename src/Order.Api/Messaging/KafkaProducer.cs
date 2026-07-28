using System.Text.Json;
using Confluent.Kafka;
namespace Order.Api.Messaging;
public interface IKafkaProducer
{
    Task ProduceAsync<T>(string topic, string key, T message,
        CancellationToken cancellationToken = default);
}
public sealed class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducer> _logger;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    public KafkaProducer(KafkaOptions options, ILogger<KafkaProducer> logger)
    {
        _logger = logger;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = 5
        }).Build();
    }
    public async Task ProduceAsync<T>(string topic, string key, T message,
        CancellationToken cancellationToken = default)
    {
        var payload = JsonSerializer.Serialize(message, JsonOptions);
        var result = await _producer.ProduceAsync(
            topic,
            new Message<string, string> { Key = key, Value = payload },
            cancellationToken);
             _logger.LogInformation(
            "Published event to {Topic} partition {Partition} offset {Offset}",
            result.Topic, result.Partition.Value, result.Offset.Value);
    }
    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
