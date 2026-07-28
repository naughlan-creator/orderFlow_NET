using Grpc.Core;
using Inventory.Service.Data;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Inventory.Grpc;
namespace Inventory.Service.Services;
public sealed class InventoryGrpcService(
    InventoryDbContext dbContext,
    ILogger<InventoryGrpcService> logger)
    : InventoryGrpc.InventoryGrpcBase
{
    public override async Task<CheckAvailabilityReply> CheckAvailability(
        CheckAvailabilityRequest request,
         ServerCallContext context)
    {
        if (!Guid.TryParse(request.ProductId, out var productId))
        {
            return new CheckAvailabilityReply
            {
                Available = false,
                AvailableQuantity = 0,
                Message = "Product ID is invalid."
            };
        }
        var item = await dbContext.InventoryItems
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.ProductId == productId,
                context.CancellationToken);
        if (item is null)
        {
            return new CheckAvailabilityReply
            {
                Available = false,
                AvailableQuantity = 0,
                Message = "Product does not exist."
            };
        }
        var available = item.AvailableQuantity >= request.Quantity;
        logger.LogInformation(
            "Availability check for {ProductId}: requested {Requested}, available {Available}",
            productId, request.Quantity, item.AvailableQuantity);
        return new CheckAvailabilityReply
        {
            Available = available,
            AvailableQuantity = item.AvailableQuantity,
            Message = available ? "Stock is available." : "Insufficient stock."
        };
    }
}