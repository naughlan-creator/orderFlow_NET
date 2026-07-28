using FluentAssertions;
using Order.Api.Models;
namespace Order.Api.Tests;
public sealed class OrderModelTests
{
    [Fact]
    public void New_order_should_default_to_pending()
    {
        var order = new OrderEntity();
        order.Status.Should().Be(OrderStatus.Pending);
    }
    [Fact]
    public void Order_should_store_customer_and_quantity()
    {
        var order = new OrderEntity
        {
            Id = Guid.NewGuid(),
            ProductId = Guid.NewGuid(),
            Quantity = 2,
            CustomerEmail = "customer@example.com",
            CreatedAtUtc = DateTimeOffset.UtcNow
        };
        order.Quantity.Should().Be(2);
        order.CustomerEmail.Should().Be("customer@example.com");
    }
    [Fact]
    public void Outbox_message_should_start_unprocessed()
    {
        var message = new OutboxMessage { Id = Guid.NewGuid() };
        message.ProcessedAtUtc.Should().BeNull();
        message.Attempts.Should().Be(0);
    }
}
