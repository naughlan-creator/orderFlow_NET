namespace OrderFlow.Triage;

/// <summary>
/// Connection settings for the triage tools.
/// <para>
/// Orders and inventory are separate services that happen to share a PostgreSQL
/// instance here, so their connection strings are kept separate: in a real
/// deployment each would point at its own read replica, with a role granted
/// SELECT and nothing else.
/// </para>
/// </summary>
public sealed class TriageOptions
{
    public string OrdersConnectionString { get; init; } = DefaultDatabase;
    public string InventoryConnectionString { get; init; } = DefaultDatabase;
    public string KafkaBootstrapServers { get; init; } = "localhost:19092";

    /// <summary>How long to wait on Kafka admin/metadata calls.</summary>
    public TimeSpan KafkaTimeout { get; init; } = TimeSpan.FromSeconds(10);

    private const string DefaultDatabase =
        "Host=localhost;Port=5432;Database=orderflow;Username=orderflow;Password=orderflow_dev";

    /// <summary>
    /// Reads settings from environment variables, falling back to the local
    /// docker-compose defaults.
    /// </summary>
    public static TriageOptions FromEnvironment() => new()
    {
        OrdersConnectionString =
            Environment.GetEnvironmentVariable("TRIAGE_ORDERS_DB") ?? DefaultDatabase,
        InventoryConnectionString =
            Environment.GetEnvironmentVariable("TRIAGE_INVENTORY_DB") ?? DefaultDatabase,
        KafkaBootstrapServers =
            Environment.GetEnvironmentVariable("TRIAGE_KAFKA") ?? "localhost:19092",
    };
}
