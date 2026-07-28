using Grpc.Core;
using OrderFlow.Inventory.Grpc;

namespace Order.Api.Tests;

/// <summary>
/// Stands in for the generated gRPC client so controller behaviour can be tested
/// without a running Inventory service.
/// </summary>
public sealed class FakeInventoryClient : InventoryGrpc.InventoryGrpcClient
{
    private readonly Func<CheckAvailabilityReply> _respond;

    private FakeInventoryClient(Func<CheckAvailabilityReply> respond) => _respond = respond;

    public static FakeInventoryClient Returns(bool productFound, bool available,
        int availableQuantity, string message = "") =>
        new(() => new CheckAvailabilityReply
        {
            ProductFound = productFound,
            Available = available,
            AvailableQuantity = availableQuantity,
            Message = message
        });

    public static FakeInventoryClient Throws(StatusCode statusCode) =>
        new(() => throw new RpcException(new Status(statusCode, "inventory is down")));

    public override AsyncUnaryCall<CheckAvailabilityReply> CheckAvailabilityAsync(
        CheckAvailabilityRequest request, CallOptions options)
    {
        Task<CheckAvailabilityReply> response;
        try
        {
            response = Task.FromResult(_respond());
        }
        catch (RpcException ex)
        {
            response = Task.FromException<CheckAvailabilityReply>(ex);
        }

        return new AsyncUnaryCall<CheckAvailabilityReply>(
            response,
            Task.FromResult(new Metadata()),
            () => Status.DefaultSuccess,
            () => new Metadata(),
            () => { });
    }
}
