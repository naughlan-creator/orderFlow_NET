FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY src/OrderFlow.Contracts/OrderFlow.Contracts.csproj src/OrderFlow.Contracts/
COPY src/Inventory.Service/Inventory.Service.csproj src/Inventory.Service/
COPY src/Inventory.Service/Protos/ src/Inventory.Service/Protos/
RUN dotnet restore src/Inventory.Service/Inventory.Service.csproj

COPY src/OrderFlow.Contracts/ src/OrderFlow.Contracts/
COPY src/Inventory.Service/ src/Inventory.Service/
RUN dotnet publish src/Inventory.Service/Inventory.Service.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
# curl is used by the compose healthcheck.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "Inventory.Service.dll"]
