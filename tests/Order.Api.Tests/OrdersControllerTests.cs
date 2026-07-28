using System.Text.Json;
using FluentAssertions;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Order.Api.Contracts;
using Order.Api.Controllers;
using Order.Api.Data;
using Order.Api.Models;
using OrderFlow.Contracts;

namespace Order.Api.Tests;

public sealed class OrdersControllerTests
{
    private static readonly Guid ProductId = Guid.Parse("11111111-1111-1111-1111-111111111111");

    private static OrdersDbContext NewDbContext() =>
        new(new DbContextOptionsBuilder<OrdersDbContext>()
            .UseInMemoryDatabase($"orders-{Guid.NewGuid()}")
            .Options);

    private static OrdersController NewController(OrdersDbContext db, FakeInventoryClient inventory) =>
        new(db, inventory, NullLogger<OrdersController>.Instance);

    private static CreateOrderRequest ValidRequest() =>
        new(ProductId, 2, "customer@example.com");

    [Fact]
    public async Task Empty_product_id_is_rejected()
    {
        using var db = NewDbContext();
        var controller = NewController(db, FakeInventoryClient.Returns(true, true, 10));

        var result = await controller.Create(
            ValidRequest() with { ProductId = Guid.Empty }, CancellationToken.None);

        result.Result.Should().BeOfType<BadRequestObjectResult>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Non_positive_quantity_is_rejected(int quantity)
    {
        using var db = NewDbContext();
        var controller = NewController(db, FakeInventoryClient.Returns(true, true, 10));

        var result = await controller.Create(
            ValidRequest() with { Quantity = quantity }, CancellationToken.None);

        result.Result.Should().BeOfType<BadRequestObjectResult>();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not-an-email")]
    [InlineData("missing-domain@")]
    [InlineData("@missing-local")]
    public async Task Invalid_email_is_rejected(string email)
    {
        using var db = NewDbContext();
        var controller = NewController(db, FakeInventoryClient.Returns(true, true, 10));

        var result = await controller.Create(
            ValidRequest() with { CustomerEmail = email }, CancellationToken.None);

        result.Result.Should().BeOfType<BadRequestObjectResult>();
    }

    [Fact]
    public async Task Unknown_product_is_a_bad_request_not_a_conflict()
    {
        using var db = NewDbContext();
        var controller = NewController(db,
            FakeInventoryClient.Returns(productFound: false, available: false, availableQuantity: 0));

        var result = await controller.Create(ValidRequest(), CancellationToken.None);

        result.Result.Should().BeOfType<BadRequestObjectResult>();
    }

    [Fact]
    public async Task Insufficient_stock_is_a_conflict()
    {
        using var db = NewDbContext();
        var controller = NewController(db,
            FakeInventoryClient.Returns(productFound: true, available: false, availableQuantity: 1));

        var result = await controller.Create(ValidRequest(), CancellationToken.None);

        result.Result.Should().BeOfType<ConflictObjectResult>();
    }

    [Fact]
    public async Task Unavailable_inventory_service_returns_503_not_500()
    {
        using var db = NewDbContext();
        var controller = NewController(db, FakeInventoryClient.Throws(StatusCode.Unavailable));

        var result = await controller.Create(ValidRequest(), CancellationToken.None);

        result.Result.Should().BeOfType<ObjectResult>()
            .Which.StatusCode.Should().Be(StatusCodes.Status503ServiceUnavailable);
    }

    [Fact]
    public async Task Valid_order_is_persisted_as_pending()
    {
        using var db = NewDbContext();
        var controller = NewController(db, FakeInventoryClient.Returns(true, true, 10));

        var result = await controller.Create(ValidRequest(), CancellationToken.None);

        result.Result.Should().BeOfType<CreatedAtActionResult>();
        var stored = await db.Orders.SingleAsync();
        stored.Status.Should().Be(OrderStatus.Pending);
        stored.ProductId.Should().Be(ProductId);
        stored.Quantity.Should().Be(2);
        stored.CustomerEmail.Should().Be("customer@example.com");
    }

    [Fact]
    public async Task Valid_order_writes_its_event_to_the_outbox_in_the_same_save()
    {
        using var db = NewDbContext();
        var controller = NewController(db, FakeInventoryClient.Returns(true, true, 10));

        await controller.Create(ValidRequest(), CancellationToken.None);

        var outbox = await db.OutboxMessages.SingleAsync();
        outbox.Topic.Should().Be(TopicNames.OrderCreated);
        outbox.ProcessedAtUtc.Should().BeNull();

        // Partitioning by product is what keeps concurrent orders for the same item
        // on a single partition, so it must not silently change.
        outbox.MessageKey.Should().Be(ProductId.ToString());

        var order = await db.Orders.SingleAsync();
        var published = JsonSerializer.Deserialize<OrderCreated>(
            outbox.Payload, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        published.Should().NotBeNull();
        published!.OrderId.Should().Be(order.Id);
        published.ProductId.Should().Be(ProductId);
        published.Quantity.Should().Be(2);
        published.EventId.Should().NotBe(Guid.Empty);
    }

    [Fact]
    public async Task Rejected_order_writes_nothing_at_all()
    {
        using var db = NewDbContext();
        var controller = NewController(db,
            FakeInventoryClient.Returns(productFound: true, available: false, availableQuantity: 0));

        await controller.Create(ValidRequest(), CancellationToken.None);

        (await db.Orders.CountAsync()).Should().Be(0);
        (await db.OutboxMessages.CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task GetById_returns_404_for_an_unknown_order()
    {
        using var db = NewDbContext();
        var controller = NewController(db, FakeInventoryClient.Returns(true, true, 10));

        var result = await controller.GetById(Guid.NewGuid(), CancellationToken.None);

        result.Result.Should().BeOfType<NotFoundResult>();
    }
}
