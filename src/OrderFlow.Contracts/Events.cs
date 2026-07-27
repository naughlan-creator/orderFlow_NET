namespace OrderFlow.Contracts;

public sealed record OrderCreated(
    Guid EventId,
    Guid OrderId,
    Guid ProductId,
    int Quantity,
    string CustomerEmail,
    DateTimeOffset CreatedAtUtc
);

public sealed record InventoryReserved(
    Guid EventId,
    Guid OrderId,
    Guid ProductId,
    int Quantity,
    string CustomerEmail,
    DateTimeOffset ReservedAtUtc
);

public sealed record InventoryRejected(
    Guid EventId,
    Guid OrderId,
    Guid ProductId,
    int Quantity,
    string CustomerEmail,
    string Reason,
    DateTimeOffset RejectedAtUtc
 );
