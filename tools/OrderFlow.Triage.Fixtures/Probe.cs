using System.Text.Json;

namespace OrderFlow.Triage.Fixtures;

/// <summary>
/// Calls all six read-only tools for one order and prints the combined result.
/// This is the exact payload the agent will later reason over, so it doubles as a
/// way to check the tools by hand before any model is involved.
/// </summary>
public static class Probe
{
    private static readonly JsonSerializerOptions Json =
        new(JsonSerializerDefaults.Web) { WriteIndented = true };

    public static async Task<string> RunAsync(
        ITriageTools tools, Guid orderId, CancellationToken cancellationToken = default)
    {
        var order = await tools.GetOrderAsync(orderId, cancellationToken);
        var outbox = await tools.GetOutboxStateAsync(orderId, cancellationToken);
        var processed = await tools.GetProcessedEventAsync(orderId, cancellationToken);
        var lag = await tools.GetConsumerLagAsync(cancellationToken);

        // The product-scoped reads only make sense once the order tells us the product.
        var stock = order.Found && order.ProductId is { } productId
            ? await tools.GetStockAsync(productId, cancellationToken)
            : null;
        var recent = order.Found && order.ProductId is { } p
            ? await tools.ListRecentOrdersForProductAsync(p, 20, cancellationToken)
            : null;

        return JsonSerializer.Serialize(new
        {
            get_order = order,
            get_outbox_state = outbox,
            get_processed_event = processed,
            get_stock = stock,
            get_consumer_lag = lag,
            list_recent_orders_for_product = recent,
        }, Json);
    }
}
