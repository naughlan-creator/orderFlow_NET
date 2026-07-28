namespace Order.Api.Models;

/// <summary>
/// Transactional outbox row. The order and its outgoing event are written in one
/// database transaction, so an event can never be lost because Kafka happened to be
/// unreachable at the moment the order was accepted.
/// </summary>
public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public string Topic { get; set; } = string.Empty;
    public string MessageKey { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTimeOffset CreatedAtUtc { get; set; }
    public DateTimeOffset? ProcessedAtUtc { get; set; }
    public int Attempts { get; set; }
}
