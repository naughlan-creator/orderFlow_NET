using Inventory.Service.Data;
using Inventory.Service.Messaging;
using Inventory.Service.Models;
using Inventory.Service.Services;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddGrpc();
builder.Services.AddHealthChecks()
    .AddDbContextCheck<InventoryDbContext>();
builder.Services.AddDbContext<InventoryDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("InventoryDatabase")));
var kafkaOptions = builder.Configuration
    .GetSection(KafkaOptions.SectionName)
    .Get<KafkaOptions>() ?? new KafkaOptions();
builder.Services.AddSingleton(kafkaOptions);
builder.Services.AddHostedService<OrderCreatedConsumer>();
var app = builder.Build();
app.MapGrpcService<InventoryGrpcService>();
app.MapGet("/", () => "Inventory gRPC service is running.");
app.MapHealthChecks("/health");
await InitialiseDatabaseAsync(app);
await app.RunAsync();
static async Task InitialiseDatabaseAsync(WebApplication app)
{
    using var scope = app.Services.CreateScope();
    var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
    await db.Database.MigrateAsync();
    if (!await db.InventoryItems.AnyAsync())
    {
        db.InventoryItems.AddRange(
            new InventoryItem
            {
                ProductId = Guid.Parse("11111111-1111-1111-1111-111111111111"),
                Name = "Mechanical Keyboard",
                AvailableQuantity = 25,
                UpdatedAtUtc = DateTimeOffset.UtcNow
            },
            new InventoryItem
            {
                ProductId = Guid.Parse("22222222-2222-2222-2222-222222222222"),
                Name = "USB-C Dock",
                AvailableQuantity = 10,
                UpdatedAtUtc = DateTimeOffset.UtcNow
            });
        await db.SaveChangesAsync();
    }
}
