using System.Text.Json;
using Npgsql;
using OrderFlow.Contracts;

namespace OrderFlow.Triage.Fixtures;

/// <summary>
/// Writes the database state for each failure mode. This is the only component in
/// the triage stack that writes — the diagnostic tools are strictly read-only.
/// </summary>
public sealed class FixtureSeeder(string ordersConnectionString, string inventoryConnectionString)
{
    // Payloads are built from the real contract records so a fixture can never
    // drift from what the services actually publish.
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public async Task ResetAsync(CancellationToken cancellationToken = default)
    {
        var orderIds = Fixture.All.Select(f => f.OrderId).ToArray();
        var orderIdText = orderIds.Select(id => id.ToString()).ToArray();
        var eventIds = Fixture.All.Select(f => f.EventId).ToArray();

        await using (var orders = new NpgsqlConnection(ordersConnectionString))
        {
            await orders.OpenAsync(cancellationToken);

            await ExecuteAsync(orders,
                "DELETE FROM orders.outbox_messages WHERE payload::jsonb ->> 'orderId' = ANY(@ids)",
                ("ids", orderIdText), cancellationToken);

            await ExecuteAsync(orders,
                "DELETE FROM orders.orders WHERE \"Id\" = ANY(@ids)",
                ("ids", orderIds), cancellationToken);
        }

        await using var inventory = new NpgsqlConnection(inventoryConnectionString);
        await inventory.OpenAsync(cancellationToken);
        await ExecuteAsync(inventory,
            "DELETE FROM inventory.processed_events WHERE \"EventId\" = ANY(@ids)",
            ("ids", eventIds), cancellationToken);
    }

    public async Task SeedAsync(CancellationToken cancellationToken = default)
    {
        await ResetAsync(cancellationToken);

        var createdAt = DateTimeOffset.UtcNow.AddMinutes(-5);

        await using var orders = new NpgsqlConnection(ordersConnectionString);
        await orders.OpenAsync(cancellationToken);

        await using var inventory = new NpgsqlConnection(inventoryConnectionString);
        await inventory.OpenAsync(cancellationToken);

        foreach (var fixture in Fixture.All.Where(f => f.Seeded))
        {
            var email = $"{fixture.Name}@example.com";

            await ExecuteAsync(orders, """
                INSERT INTO orders.orders ("Id","ProductId","Quantity","CustomerEmail","Status","CreatedAtUtc")
                VALUES (@id,@productId,@quantity,@email,@status,@createdAt)
                """,
                cancellationToken,
                ("id", fixture.OrderId),
                ("productId", fixture.ProductId),
                ("quantity", fixture.Quantity),
                ("email", email),
                ("status", fixture.OrderStatus),
                ("createdAt", createdAt));

            if (fixture.Outbox is not OutboxState.None)
            {
                var orderCreated = new OrderCreated(
                    fixture.EventId, fixture.OrderId, fixture.ProductId,
                    fixture.Quantity, email, createdAt);

                await ExecuteAsync(orders, """
                    INSERT INTO orders.outbox_messages
                        (id, topic, message_key, payload, created_at_utc, processed_at_utc, attempts)
                    VALUES (@id,@topic,@key,@payload,@createdAt,@processedAt,@attempts)
                    """,
                    cancellationToken,
                    ("id", Guid.NewGuid()),
                    ("topic", TopicNames.OrderCreated),
                    ("key", fixture.ProductId.ToString()),
                    ("payload", JsonSerializer.Serialize(orderCreated, JsonOptions)),
                    ("createdAt", createdAt),
                    ("processedAt", fixture.Outbox is OutboxState.Published
                        ? createdAt.AddSeconds(1)
                        : (object)DBNull.Value),
                    ("attempts", fixture.Outbox is OutboxState.Published ? 1 : 0));
            }

            if (fixture.Inventory is not InventoryOutcome.NotProcessed)
            {
                var processedAt = createdAt.AddSeconds(2);
                var (topic, payload) = fixture.Inventory is InventoryOutcome.Reserved
                    ? (TopicNames.InventoryReserved, JsonSerializer.Serialize(
                        new InventoryReserved(Guid.NewGuid(), fixture.OrderId, fixture.ProductId,
                            fixture.Quantity, email, processedAt), JsonOptions))
                    : (TopicNames.InventoryRejected, JsonSerializer.Serialize(
                        new InventoryRejected(Guid.NewGuid(), fixture.OrderId, fixture.ProductId,
                            fixture.Quantity, email, "Insufficient stock.", processedAt), JsonOptions));

                await ExecuteAsync(inventory, """
                    INSERT INTO inventory.processed_events
                        ("EventId","OutcomeTopic","OutcomePayload","ProcessedAtUtc")
                    VALUES (@eventId,@topic,@payload,@processedAt)
                    """,
                    cancellationToken,
                    ("eventId", fixture.EventId),
                    ("topic", topic),
                    ("payload", payload),
                    ("processedAt", processedAt));
            }
        }
    }

    /// <summary>
    /// Re-reads the fixtures that depend on a consumer *not* running. If the app
    /// services are up they will drain the stuck outbox row and complete the pending
    /// order within seconds, silently invalidating the fixture set.
    /// </summary>
    public async Task<IReadOnlyList<string>> VerifyHeldAsync(CancellationToken cancellationToken = default)
    {
        var broken = new List<string>();

        await using var orders = new NpgsqlConnection(ordersConnectionString);
        await orders.OpenAsync(cancellationToken);

        foreach (var fixture in Fixture.All.Where(f => f.Seeded))
        {
            await using var command = new NpgsqlCommand(
                "SELECT \"Status\" FROM orders.orders WHERE \"Id\" = @id", orders);
            command.Parameters.AddWithValue("id", fixture.OrderId);
            var status = await command.ExecuteScalarAsync(cancellationToken) as string;

            if (!string.Equals(status, fixture.OrderStatus, StringComparison.Ordinal))
                broken.Add($"{fixture.Name}: expected status {fixture.OrderStatus}, found {status ?? "(deleted)"}");
        }

        await using var stuck = new NpgsqlCommand("""
            SELECT count(*) FROM orders.outbox_messages
            WHERE payload::jsonb ->> 'orderId' = @orderId AND processed_at_utc IS NOT NULL
            """, orders);
        stuck.Parameters.AddWithValue("orderId",
            Fixture.All.Single(f => f.Name == "outbox-stuck").OrderId.ToString());

        if (Convert.ToInt64(await stuck.ExecuteScalarAsync(cancellationToken)) > 0)
            broken.Add("outbox-stuck: the row was published — the outbox publisher is running.");

        return broken;
    }

    private static Task ExecuteAsync(
        NpgsqlConnection connection, string sql,
        (string Name, object Value) parameter, CancellationToken cancellationToken) =>
        ExecuteAsync(connection, sql, cancellationToken, parameter);

    private static async Task ExecuteAsync(
        NpgsqlConnection connection, string sql, CancellationToken cancellationToken,
        params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
            command.Parameters.AddWithValue(name, value);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
