using Order.Api.Models;
namespace Order.Api.Contracts;
public sealed record OrderResponse(
    Guid Id,
    Guid ProductId,
    int Quantity,
    string CustomerEmail,
    OrderStatus Status,
    DateTimeOffset CreatedAtUtc
);