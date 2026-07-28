using Microsoft.EntityFrameworkCore;
using Order.Api.Models;
namespace Order.Api.Data;

public sealed class OrdersDbContext(DbContextOptions<OrdersDbContext> options)
    : DbContext(options)
{
    public DbSet<OrderEntity> Orders => Set<OrderEntity>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("orders");
        modelBuilder.Entity<OrderEntity>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.CustomerEmail).HasMaxLength(320).IsRequired();
            entity.Property(x => x.Status).HasConversion<string>().HasMaxLength(40);
            entity.HasIndex(x => x.CreatedAtUtc);
        });
        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            // Column names are set explicitly because the publisher reads this table
            // with raw SQL (FOR UPDATE SKIP LOCKED).
            entity.ToTable("outbox_messages");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Topic).HasColumnName("topic")
                .HasMaxLength(120).IsRequired();
            entity.Property(x => x.MessageKey).HasColumnName("message_key")
                .HasMaxLength(120).IsRequired();
            entity.Property(x => x.Payload).HasColumnName("payload").IsRequired();
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(x => x.ProcessedAtUtc).HasColumnName("processed_at_utc");
            entity.Property(x => x.Attempts).HasColumnName("attempts");
            entity.HasIndex(x => new { x.ProcessedAtUtc, x.CreatedAtUtc })
                .HasDatabaseName("ix_outbox_unprocessed");
        });
    }
}
