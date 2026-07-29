using Npgsql;
using OrderFlow.Contracts;
using OrderFlow.Triage.Models;

namespace OrderFlow.Triage;

/// <summary>
/// The six read-only diagnostic reads that answer "what happened to this order".
/// <para>
/// Read-only is enforced twice: every statement here is a SELECT, and each
/// connection sets <c>default_transaction_read_only</c> so that a write which
/// slipped in would be rejected by PostgreSQL rather than by code review.
/// </para>
/// <para>
/// These queries deliberately cross the orders/inventory service boundary. That
/// is a considered exception for read-only diagnostics — the alternative is
/// adding debug endpoints to both services, which widens their public surface
/// for the benefit of an operator tool.
/// </para>
/// </summary>
public interface ITriageTools
{
    Task<OrderSnapshot> GetOrderAsync(Guid orderId, CancellationToken cancellationToken = default);
    Task<OutboxSnapshot> GetOutboxStateAsync(Guid orderId, CancellationToken cancellationToken = default);
    Task<ProcessedEventSnapshot> GetProcessedEventAsync(Guid orderId, CancellationToken cancellationToken = default);
    Task<StockSnapshot> GetStockAsync(Guid productId, CancellationToken cancellationToken = default);
    Task<ConsumerLagSnapshot> GetConsumerLagAsync(CancellationToken cancellationToken = default);
    Task<RecentOrdersSnapshot> ListRecentOrdersForProductAsync(
        Guid productId, int limit = 20, CancellationToken cancellationToken = default);
}

public sealed class TriageTools(TriageOptions options) : ITriageTools
{
    private readonly KafkaLagReader _lagReader = new(options);

    public async Task<OrderSnapshot> GetOrderAsync(
        Guid orderId, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenReadOnlyAsync(
            options.OrdersConnectionString, cancellationToken);

        const string sql = """
            SELECT "ProductId", "Quantity", "CustomerEmail", "Status", "CreatedAtUtc"
            FROM orders.orders
            WHERE "Id" = @orderId
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("orderId", orderId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return OrderSnapshot.NotFound(orderId);

        var createdAt = reader.GetFieldValue<DateTimeOffset>(4);
        return new OrderSnapshot(
            Found: true,
            OrderId: orderId,
            ProductId: reader.GetGuid(0),
            Quantity: reader.GetInt32(1),
            CustomerEmail: reader.GetString(2),
            Status: reader.GetString(3),
            CreatedAtUtc: createdAt,
            AgeSeconds: Round((DateTimeOffset.UtcNow - createdAt).TotalSeconds));
    }

    public async Task<OutboxSnapshot> GetOutboxStateAsync(
        Guid orderId, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenReadOnlyAsync(
            options.OrdersConnectionString, cancellationToken);

        // Outbox rows are keyed by ProductId (so same-product orders share a
        // partition), so the order is found through the payload, not the key.
        const string sql = """
            SELECT id, topic, message_key, created_at_utc, processed_at_utc, attempts,
                   payload::jsonb ->> 'eventId' AS event_id
            FROM orders.outbox_messages
            WHERE payload::jsonb ->> 'orderId' = @orderId
            ORDER BY created_at_utc
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("orderId", orderId.ToString());

        var entries = new List<OutboxEntry>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var createdAt = reader.GetFieldValue<DateTimeOffset>(3);
            var processedAt = await reader.IsDBNullAsync(4, cancellationToken)
                ? (DateTimeOffset?)null
                : reader.GetFieldValue<DateTimeOffset>(4);
            var rawEventId = await reader.IsDBNullAsync(6, cancellationToken)
                ? null
                : reader.GetString(6);

            entries.Add(new OutboxEntry(
                Id: reader.GetGuid(0),
                Topic: reader.GetString(1),
                MessageKey: reader.GetString(2),
                EventId: Guid.TryParse(rawEventId, out var eventId) ? eventId : null,
                Published: processedAt is not null,
                CreatedAtUtc: createdAt,
                ProcessedAtUtc: processedAt,
                Attempts: reader.GetInt32(5),
                PendingForSeconds: processedAt is null
                    ? Round((DateTimeOffset.UtcNow - createdAt).TotalSeconds)
                    : null));
        }

        return entries.Count == 0
            ? OutboxSnapshot.NotFound(orderId)
            : new OutboxSnapshot(true, orderId, entries);
    }

    public async Task<ProcessedEventSnapshot> GetProcessedEventAsync(
        Guid orderId, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenReadOnlyAsync(
            options.InventoryConnectionString, cancellationToken);

        // processed_events is keyed by EventId; the stored outcome payload is what
        // ties it back to an order.
        const string sql = """
            SELECT "EventId", "OutcomeTopic", "ProcessedAtUtc",
                   "OutcomePayload"::jsonb ->> 'reason' AS reason
            FROM inventory.processed_events
            WHERE "OutcomePayload"::jsonb ->> 'orderId' = @orderId
            ORDER BY "ProcessedAtUtc" DESC
            LIMIT 1
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("orderId", orderId.ToString());

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return ProcessedEventSnapshot.NotFound(orderId);

        return new ProcessedEventSnapshot(
            Found: true,
            OrderId: orderId,
            EventId: reader.GetGuid(0),
            OutcomeTopic: reader.GetString(1),
            ProcessedAtUtc: reader.GetFieldValue<DateTimeOffset>(2),
            RejectionReason: await reader.IsDBNullAsync(3, cancellationToken)
                ? null
                : reader.GetString(3));
    }

    public async Task<StockSnapshot> GetStockAsync(
        Guid productId, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenReadOnlyAsync(
            options.InventoryConnectionString, cancellationToken);

        const string sql = """
            SELECT "Name", "AvailableQuantity", "UpdatedAtUtc"
            FROM inventory.inventory_items
            WHERE "ProductId" = @productId
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("productId", productId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return StockSnapshot.NotFound(productId);

        return new StockSnapshot(
            Found: true,
            ProductId: productId,
            Name: reader.GetString(0),
            AvailableQuantity: reader.GetInt32(1),
            UpdatedAtUtc: reader.GetFieldValue<DateTimeOffset>(2));
    }

    public Task<ConsumerLagSnapshot> GetConsumerLagAsync(CancellationToken cancellationToken = default) =>
        _lagReader.ReadAsync(cancellationToken);

    public async Task<RecentOrdersSnapshot> ListRecentOrdersForProductAsync(
        Guid productId, int limit = 20, CancellationToken cancellationToken = default)
    {
        if (limit is < 1 or > 200) limit = 20;

        await using var connection = await OpenReadOnlyAsync(
            options.OrdersConnectionString, cancellationToken);

        const string sql = """
            SELECT "Id", "Quantity", "Status", "CreatedAtUtc"
            FROM orders.orders
            WHERE "ProductId" = @productId
            ORDER BY "CreatedAtUtc" DESC
            LIMIT @limit
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("productId", productId);
        command.Parameters.AddWithValue("limit", limit);

        var orders = new List<RecentOrder>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            orders.Add(new RecentOrder(
                OrderId: reader.GetGuid(0),
                Quantity: reader.GetInt32(1),
                Status: reader.GetString(2),
                CreatedAtUtc: reader.GetFieldValue<DateTimeOffset>(3)));
        }

        // Pre-aggregated because "stock is lower than our orders account for" is a
        // question this tool exists to answer, and summing rows is not the model's job.
        int SumWhere(string status) => orders
            .Where(x => string.Equals(x.Status, status, StringComparison.OrdinalIgnoreCase))
            .Sum(x => x.Quantity);

        return new RecentOrdersSnapshot(
            ProductId: productId,
            OrderCount: orders.Count,
            QuantityReserved: SumWhere(nameof(OrderStatusNames.InventoryReserved)),
            QuantityPending: SumWhere(nameof(OrderStatusNames.Pending)),
            QuantityRejected: SumWhere(nameof(OrderStatusNames.InventoryRejected)),
            Orders: orders);
    }

    private static async Task<NpgsqlConnection> OpenReadOnlyAsync(
        string connectionString, CancellationToken cancellationToken)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        // Defence in depth: PostgreSQL rejects any write on this session, so the
        // read-only guarantee does not rest on every future edit staying disciplined.
        await using var readOnly = new NpgsqlCommand("SET default_transaction_read_only = on;", connection);
        await readOnly.ExecuteNonQueryAsync(cancellationToken);

        return connection;
    }

    private static double Round(double seconds) => Math.Round(seconds, 1);

    /// <summary>Status strings as persisted by Order.Api's string enum conversion.</summary>
    private enum OrderStatusNames
    {
        Pending,
        InventoryReserved,
        InventoryRejected,
    }

    /// <summary>Topics the lag reader inspects, kept in sync with the services.</summary>
    internal static readonly string[] Topics =
    [
        TopicNames.OrderCreated,
        TopicNames.InventoryReserved,
        TopicNames.InventoryRejected,
    ];
}
