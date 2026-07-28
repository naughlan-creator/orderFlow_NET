FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY OrderFlow.sln ./
COPY src/OrderFlow.Contracts/OrderFlow.Contracts.csproj src/OrderFlow.Contracts/
COPY src/Notification.Worker/Notification.Worker.csproj src/Notification.Worker/
RUN dotnet restore src/Notification.Worker/Notification.Worker.csproj

COPY src/OrderFlow.Contracts/ src/OrderFlow.Contracts/
COPY src/Notification.Worker/ src/Notification.Worker/
RUN dotnet publish src/Notification.Worker/Notification.Worker.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Notification.Worker.dll"]