using Confluent.Kafka;
using OrderFlow.Contracts;
namespace Order.Api.Messaging;

public interface IKafkaProducer
{
    /// <summary>Publishes an already-serialised payload; the outbox owns serialisation.</summary>
    Task ProduceAsync(string topic, string key, string payload,
        CancellationToken cancellationToken = default);
}

public sealed class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducer> _logger;
    public KafkaProducer(KafkaOptions options, ILogger<KafkaProducer> logger)
    {
        _logger = logger;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            // MessageSendMaxRetries is deliberately left at its default (int.MaxValue);
            // lowering it weakens the guarantee EnableIdempotence is here to provide.
            EnableIdempotence = true
        }).Build();
    }
    public async Task ProduceAsync(string topic, string key, string payload,
        CancellationToken cancellationToken = default)
    {
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
