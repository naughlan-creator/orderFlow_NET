namespace Inventory.Service.Models;

/// <summary>
/// Deduplication record for Kafka's at-least-once delivery. Written in the same
/// transaction as the stock change, so a redelivered <c>order.created</c> can never
/// decrement inventory twice. The outcome is stored so a replay can republish the
/// original decision instead of silently dropping it.
/// </summary>
public sealed class ProcessedEvent
{
    public Guid EventId { get; set; }
    public string OutcomeTopic { get; set; } = string.Empty;
    public string OutcomePayload { get; set; } = string.Empty;
    public DateTimeOffset ProcessedAtUtc { get; set; }
}
