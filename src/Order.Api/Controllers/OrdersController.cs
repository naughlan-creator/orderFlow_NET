using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using Grpc.Core;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Order.Api.Contracts;
using Order.Api.Data;
using Order.Api.Models;
using OrderFlow.Contracts;
using OrderFlow.Inventory.Grpc;
namespace Order.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public sealed class OrdersController(
    OrdersDbContext dbContext,
    InventoryGrpc.InventoryGrpcClient inventoryClient,
    ILogger<OrdersController> logger) : ControllerBase
{
    private static readonly TimeSpan InventoryTimeout = TimeSpan.FromSeconds(5);
    private static readonly EmailAddressAttribute EmailValidator = new();
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    [HttpPost]
    [ProducesResponseType<OrderResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public async Task<ActionResult<OrderResponse>> Create(
        CreateOrderRequest request,
        CancellationToken cancellationToken)
    {
        if (request.ProductId == Guid.Empty)
            return BadRequest("ProductId is required.");
        if (request.Quantity <= 0)
            return BadRequest("Quantity must be greater than zero.");
        if (string.IsNullOrWhiteSpace(request.CustomerEmail) ||
            request.CustomerEmail.Length > 320 ||
            !EmailValidator.IsValid(request.CustomerEmail))
            return BadRequest("A valid customer email is required.");

        CheckAvailabilityReply availability;
        try
        {
            availability = await inventoryClient.CheckAvailabilityAsync(
                new CheckAvailabilityRequest
                {
                    ProductId = request.ProductId.ToString(),
                    Quantity = request.Quantity
                },
                deadline: DateTime.UtcNow.Add(InventoryTimeout),
                cancellationToken: cancellationToken);
        }
        catch (RpcException ex) when (ex.StatusCode != Grpc.Core.StatusCode.Cancelled)
        {
            // Without this the request surfaces as an opaque 500 whenever the inventory
            // service is down or slow. A deadline also stops it hanging indefinitely.
            logger.LogError(ex, "Inventory availability check failed ({StatusCode})",
                ex.StatusCode);
            return StatusCode(StatusCodes.Status503ServiceUnavailable,
                new { message = "Inventory service is unavailable. Please retry." });
        }

        if (!availability.ProductFound)
            return BadRequest($"Product {request.ProductId} does not exist.");

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
        var orderCreated = new OrderCreated(
            Guid.NewGuid(), order.Id, order.ProductId, order.Quantity,
            order.CustomerEmail, order.CreatedAtUtc);

        dbContext.Orders.Add(order);
        dbContext.OutboxMessages.Add(new OutboxMessage
        {
            Id = Guid.NewGuid(),
            Topic = TopicNames.OrderCreated,
            // Partitioning by product serialises all orders for the same item onto one
            // partition, so concurrent orders cannot race on the same stock row.
            MessageKey = order.ProductId.ToString(),
            Payload = JsonSerializer.Serialize(orderCreated, JsonOptions),
            CreatedAtUtc = DateTimeOffset.UtcNow
        });

        // One SaveChanges, one transaction: the order and its event are committed
        // together, so a Kafka outage can never strand an order with no event.
        await dbContext.SaveChangesAsync(cancellationToken);

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
