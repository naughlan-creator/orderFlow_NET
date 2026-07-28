namespace Inventory.Service.Models;
public sealed class InventoryItem
{
    public Guid ProductId { get; set; }
    public string Name { get; set; } = string.Empty;
    public int AvailableQuantity { get; set; }
    public DateTimeOffset UpdatedAtUtc { get; set; }
}