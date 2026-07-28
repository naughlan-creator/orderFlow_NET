FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY src/OrderFlow.Contracts/OrderFlow.Contracts.csproj src/OrderFlow.Contracts/
COPY src/Order.Api/Order.Api.csproj src/Order.Api/
# Order.Api compiles the inventory .proto as a gRPC client, so the contract has to be
# present before restore even though there is no project reference.
COPY src/Inventory.Service/Protos/ src/Inventory.Service/Protos/
RUN dotnet restore src/Order.Api/Order.Api.csproj

COPY src/OrderFlow.Contracts/ src/OrderFlow.Contracts/
COPY src/Order.Api/ src/Order.Api/
RUN dotnet publish src/Order.Api/Order.Api.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
# curl is used by the compose healthcheck.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "Order.Api.dll"]
