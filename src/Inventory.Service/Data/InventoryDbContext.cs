using Inventory.Service.Models;
using Microsoft.EntityFrameworkCore;
namespace Inventory.Service.Data;
public sealed class InventoryDbContext(DbContextOptions<InventoryDbContext> options)
    : DbContext(options)
{
    public DbSet<InventoryItem> InventoryItems => Set<InventoryItem>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("inventory");
        modelBuilder.Entity<InventoryItem>(entity =>
        {
            entity.ToTable("inventory_items");
            entity.HasKey(x => x.ProductId);
            entity.Property(x => x.Name).HasMaxLength(160).IsRequired();
        });
    }
}