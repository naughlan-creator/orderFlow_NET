using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Order.Api.Contracts;
using Order.Api.Data;
using Order.Api.Messaging;
using Order.Api.Models;
using OrderFlow.Contracts;
using OrderFlow.Inventory.Grpc;
namespace Order.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public sealed class OrdersController(
    OrdersDbContext dbContext,
    InventoryGrpc.InventoryGrpcClient inventoryClient,
    IKafkaProducer producer,
    ILogger<OrdersController> logger) : ControllerBase
{
    [HttpPost]
    [ProducesResponseType<OrderResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<ActionResult<OrderResponse>> Create(
        CreateOrderRequest request,
        CancellationToken cancellationToken)
    {
        if (request.ProductId == Guid.Empty)
            return BadRequest("ProductId is required.");
        if (request.Quantity <= 0)
            return BadRequest("Quantity must be greater than zero.");
        if (string.IsNullOrWhiteSpace(request.CustomerEmail) ||
            !request.CustomerEmail.Contains('@'))
            return BadRequest("A valid customer email is required.");
        var availability = await inventoryClient.CheckAvailabilityAsync(
            new CheckAvailabilityRequest
            {
                ProductId = request.ProductId.ToString(),
                Quantity = request.Quantity
            },
            cancellationToken: cancellationToken);
        if (!availability.Available)
        {
            return Conflict(new
            {
                message = availability.Message,
                availableQuantity = availability.AvailableQuantity
            });
        }
        var order = new OrderEntity
        {
            Id = Guid.NewGuid(),
            ProductId = request.ProductId,
            Quantity = request.Quantity,
            CustomerEmail = request.CustomerEmail.Trim(),
            Status = OrderStatus.Pending,
            CreatedAtUtc = DateTimeOffset.UtcNow
        };

        dbContext.Orders.Add(order);
        await dbContext.SaveChangesAsync(cancellationToken);
        var orderCreated = new OrderCreated(
            Guid.NewGuid(), order.Id, order.ProductId, order.Quantity,
            order.CustomerEmail, order.CreatedAtUtc);
        await producer.ProduceAsync(
            TopicNames.OrderCreated,
            order.Id.ToString(),
            orderCreated,
            cancellationToken);
        logger.LogInformation("Created order {OrderId}", order.Id);
        var response = ToResponse(order);
        return CreatedAtAction(nameof(GetById), new { id = order.Id }, response);
    }
    [HttpGet("{id:guid}")]
    [ProducesResponseType<OrderResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<OrderResponse>> GetById(
        Guid id, CancellationToken cancellationToken)
    {
        var order = await dbContext.Orders
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.Id == id, cancellationToken);
        return order is null ? NotFound() : Ok(ToResponse(order));
    }
    [HttpGet]
    public async Task<ActionResult<IReadOnlyList<OrderResponse>>> GetAll(
        CancellationToken cancellationToken)
    {
        var orders = await dbContext.Orders
            .AsNoTracking()
            .OrderByDescending(x => x.CreatedAtUtc)
            .Take(100)
            .Select(x => new OrderResponse(
                x.Id, x.ProductId, x.Quantity, x.CustomerEmail,
                x.Status, x.CreatedAtUtc))
            .ToListAsync(cancellationToken);
        return Ok(orders);
    }
    private static OrderResponse ToResponse(OrderEntity order) =>
        new(order.Id, order.ProductId, order.Quantity,
            order.CustomerEmail, order.Status, order.CreatedAtUtc);
}