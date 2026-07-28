FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY OrderFlow.sln ./
COPY src/OrderFlow.Contracts/OrderFlow.Contracts.csproj src/OrderFlow.Contracts/
COPY src/Inventory.Service/Inventory.Service.csproj src/Inventory.Service/
RUN dotnet restore src/Inventory.Service/Inventory.Service.csproj

COPY src/OrderFlow.Contracts/ src/OrderFlow.Contracts/
COPY src/Inventory.Service/ src/Inventory.Service/
RUN dotnet publish src/Inventory.Service/Inventory.Service.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "Inventory.Service.dll"]