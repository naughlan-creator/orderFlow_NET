namespace OrderFlow.Triage.Models;

// Every snapshot carries an explicit Found flag instead of returning null, so a
// caller (human or model) reading the serialised result can tell "no such row"
// apart from "row exists but the field is empty". Ages are precomputed because
// "how long has it been stuck" is the question these tools exist to answer.

public sealed record OrderSnapshot(
    bool Found,
    Guid OrderId,
    Guid? ProductId = null,
    int? Quantity = null,
    string? CustomerEmail = null,
    string? Status = null,
    DateTimeOffset? CreatedAtUtc = null,
    double? AgeSeconds = null)
{
    public static OrderSnapshot NotFound(Guid orderId) => new(false, orderId);
}

public sealed record OutboxEntry(
    Guid Id,
    string Topic,
    string MessageKey,
    Guid? EventId,
    bool Published,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset? ProcessedAtUtc,
    int Attempts,
    double? PendingForSeconds);

public sealed record OutboxSnapshot(
    bool Found,
    Guid OrderId,
    IReadOnlyList<OutboxEntry> Entries)
{
    public static OutboxSnapshot NotFound(Guid orderId) =>
        new(false, orderId, Array.Empty<OutboxEntry>());
}

public sealed record ProcessedEventSnapshot(
    bool Found,
    Guid OrderId,
    Guid? EventId = null,
    string? OutcomeTopic = null,
    string? RejectionReason = null,
    DateTimeOffset? ProcessedAtUtc = null)
{
    public static ProcessedEventSnapshot NotFound(Guid orderId) => new(false, orderId);
}

public sealed record StockSnapshot(
    bool Found,
    Guid ProductId,
    string? Name = null,
    int? AvailableQuantity = null,
    DateTimeOffset? UpdatedAtUtc = null)
{
    public static StockSnapshot NotFound(Guid productId) => new(false, productId);
}

public sealed record PartitionLag(
    string Topic,
    int Partition,
    long CommittedOffset,
    long HighWatermark,
    long Lag);

public sealed record ConsumerGroupLag(
    string GroupId,
    bool Found,
    long TotalLag,
    IReadOnlyList<PartitionLag> Partitions,
    string? Note = null);

public sealed record ConsumerLagSnapshot(IReadOnlyList<ConsumerGroupLag> Groups);

public sealed record RecentOrder(
    Guid OrderId,
    int Quantity,
    string Status,
    DateTimeOffset CreatedAtUtc);

public sealed record RecentOrdersSnapshot(
    Guid ProductId,
    int OrderCount,
    int QuantityReserved,
    int QuantityPending,
    int QuantityRejected,
    IReadOnlyList<RecentOrder> Orders);
