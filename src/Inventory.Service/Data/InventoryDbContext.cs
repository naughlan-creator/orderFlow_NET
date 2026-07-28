using Inventory.Service.Models;
using Microsoft.EntityFrameworkCore;
namespace Inventory.Service.Data;
public sealed class InventoryDbContext(DbContextOptions<InventoryDbContext> options)
    : DbContext(options)
{
    public DbSet<InventoryItem> InventoryItems => Set<InventoryItem>();
    public DbSet<ProcessedEvent> ProcessedEvents => Set<ProcessedEvent>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("inventory");
        modelBuilder.Entity<InventoryItem>(entity =>
        {
            entity.ToTable("inventory_items");
            entity.HasKey(x => x.ProductId);
            entity.Property(x => x.Name).HasMaxLength(160).IsRequired();
            // PostgreSQL's xmin system column as an optimistic concurrency token, so
            // two consumers cannot both decrement this row from the same stale read.
            entity.Property<uint>("xmin")
                .HasColumnType("xid")
                .ValueGeneratedOnAddOrUpdate()
                .IsConcurrencyToken();
        });
        modelBuilder.Entity<ProcessedEvent>(entity =>
        {
            entity.ToTable("processed_events");
            entity.HasKey(x => x.EventId);
            entity.Property(x => x.OutcomeTopic).HasMaxLength(120).IsRequired();
            entity.Property(x => x.OutcomePayload).IsRequired();
        });
    }
}
