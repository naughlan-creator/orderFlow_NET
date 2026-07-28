namespace Order.Api.Contracts;
public sealed record CreateOrderRequest(
    Guid ProductId,
    int Quantity,
    string CustomerEmail
);