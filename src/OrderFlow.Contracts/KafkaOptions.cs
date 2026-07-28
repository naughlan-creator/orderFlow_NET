namespace OrderFlow.Contracts;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";
    public string BootstrapServers { get; init; } = "localhost:19092";
    public string GroupId { get; init; } = "orderflow";
}
