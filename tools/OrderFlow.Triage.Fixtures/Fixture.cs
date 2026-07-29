namespace OrderFlow.Triage.Fixtures;

/// <summary>What the seeder should leave behind for one scenario.</summary>
public enum OutboxState
{
    None,
    Unpublished,
    Published,
}

/// <summary>What the inventory service should look like it did with the event.</summary>
public enum InventoryOutcome
{
    NotProcessed,
    Reserved,
    Rejected,
}

/// <summary>
/// One seeded failure mode. <see cref="ExpectedDiagnosis"/> is the label an eval
/// grades against, so the fixture set doubles as the answer key.
/// </summary>
public sealed record Fixture(
    string Name,
    Guid OrderId,
    Guid EventId,
    Guid ProductId,
    int Quantity,
    string OrderStatus,
    OutboxState Outbox,
    InventoryOutcome Inventory,
    string ExpectedDiagnosis,
    bool Seeded = true)
{
    public const string Keyboard = "11111111-1111-1111-1111-111111111111";
    public const string Dock = "22222222-2222-2222-2222-222222222222";
    public const string GhostProduct = "99999999-9999-9999-9999-999999999999";

    private static Guid OrderIdFor(int n) => Guid.Parse($"fee00000-0000-0000-0000-{n:D12}");
    private static Guid EventIdFor(int n) => Guid.Parse($"fee00001-0000-0000-0000-{n:D12}");

    public static IReadOnlyList<Fixture> All { get; } =
    [
        new(
            Name: "healthy-reserved",
            OrderId: OrderIdFor(1), EventId: EventIdFor(1),
            ProductId: Guid.Parse(Keyboard), Quantity: 2,
            OrderStatus: "InventoryReserved",
            Outbox: OutboxState.Published,
            Inventory: InventoryOutcome.Reserved,
            ExpectedDiagnosis: "Healthy. The order completed end to end and reserved stock."),

        new(
            Name: "rejected-insufficient-stock",
            OrderId: OrderIdFor(2), EventId: EventIdFor(2),
            ProductId: Guid.Parse(Dock), Quantity: 9999,
            OrderStatus: "InventoryRejected",
            Outbox: OutboxState.Published,
            Inventory: InventoryOutcome.Rejected,
            ExpectedDiagnosis: "Working as designed. Inventory rejected the order for insufficient stock; this is a business outcome, not a fault."),

        new(
            Name: "outbox-stuck",
            OrderId: OrderIdFor(3), EventId: EventIdFor(3),
            ProductId: Guid.Parse(Keyboard), Quantity: 1,
            OrderStatus: "Pending",
            Outbox: OutboxState.Unpublished,
            Inventory: InventoryOutcome.NotProcessed,
            ExpectedDiagnosis: "The outbox publisher has not drained this row. Order.Api is down, or it cannot reach Kafka. No stock was touched, so it is safe to retry by restarting Order.Api."),

        new(
            Name: "awaiting-inventory",
            OrderId: OrderIdFor(4), EventId: EventIdFor(4),
            ProductId: Guid.Parse(Keyboard), Quantity: 3,
            OrderStatus: "Pending",
            Outbox: OutboxState.Published,
            Inventory: InventoryOutcome.NotProcessed,
            ExpectedDiagnosis: "The event was published but Inventory.Service never applied it. That consumer is down or lagging; check order.created lag for the inventory-service group."),

        new(
            Name: "awaiting-status",
            OrderId: OrderIdFor(5), EventId: EventIdFor(5),
            ProductId: Guid.Parse(Keyboard), Quantity: 4,
            OrderStatus: "Pending",
            Outbox: OutboxState.Published,
            Inventory: InventoryOutcome.Reserved,
            ExpectedDiagnosis: "Stock was already reserved but the order is still Pending, so Order.Api's status consumer is down or lagging. Do not re-submit — that would double-decrement stock."),

        new(
            Name: "unknown-order",
            OrderId: OrderIdFor(6), EventId: EventIdFor(6),
            ProductId: Guid.Parse(GhostProduct), Quantity: 1,
            OrderStatus: "(none)",
            Outbox: OutboxState.None,
            Inventory: InventoryOutcome.NotProcessed,
            ExpectedDiagnosis: "No such order. The ID is wrong, or the order was never accepted.",
            Seeded: false),
    ];
}
