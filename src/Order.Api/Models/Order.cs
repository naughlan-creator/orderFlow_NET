namespace Order.Api.Models;

public enum OrderStatus
{
    Pending = 0,
    InventoryReserved = 1,
    InventoryRejected = 2
}

public sealed class OrderEntity
{
    public Guid Id { get; set; }
    public Guid ProductId { get; set; }
    public int Quantity { get; set; }
    public string CustomerEmail { get; set; } = string.Empty;
    public OrderStatus Status { get; set; } = OrderStatus.Pending;
    public DateTimeOffset CreatedAtUtc { get; set; }
}