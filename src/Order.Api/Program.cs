using Microsoft.EntityFrameworkCore;
using Order.Api.Data;
using Order.Api.Messaging;
using OrderFlow.Inventory.Grpc;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHealthChecks().AddDbContextCheck<OrdersDbContext>();

builder.Services.AddDbContext<OrdersDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("OrdersDatabase")));

var kafkaOptions = builder.Configuration.GetSection(KafkaOptions.SectionName)
    .Get<KafkaOptions>() ?? new KafkaOptions();

builder.Services.AddSingleton(kafkaOptions);
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();

var inventoryAddress = builder.Configuration["InventoryGrpc:Address"] ?? "http://localhost:5002";

builder.Services.AddGrpcClient<InventoryGrpc.InventoryGrpcClient>(options =>
{
    options.Address = new Uri(inventoryAddress);
});

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();
app.MapControllers();
app.MapHealthChecks("/health");

await ApplyMigrationsAsync(app);
await app.RunAsync();

static async Task ApplyMigrationsAsync(WebApplication app)
{
    using var scope = app.Services.CreateScope();
    var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    await db.Database.MigrateAsync();
}

public partial class Program;
