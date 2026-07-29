using OrderFlow.Triage;
using OrderFlow.Triage.Fixtures;

var options = TriageOptions.FromEnvironment();
var seeder = new FixtureSeeder(options.OrdersConnectionString, options.InventoryConnectionString);
var tools = new TriageTools(options);

var command = args.Length > 0 ? args[0].ToLowerInvariant() : "help";

try
{
    switch (command)
    {
        case "seed":
            await SeedAsync();
            return 0;

        case "reset":
            await seeder.ResetAsync();
            Console.WriteLine("Fixture rows removed.");
            return 0;

        case "list":
            ListFixtures();
            return 0;

        case "probe":
            if (args.Length < 2 || !Guid.TryParse(args[1], out var orderId))
            {
                Console.Error.WriteLine("Usage: orderflow-fixtures probe <orderId>");
                return 1;
            }
            Console.WriteLine(await Probe.RunAsync(tools, orderId));
            return 0;

        case "probe-all":
            foreach (var fixture in Fixture.All)
            {
                Console.WriteLine($"=== {fixture.Name}  ({fixture.OrderId}) ===");
                Console.WriteLine(await Probe.RunAsync(tools, fixture.OrderId));
                Console.WriteLine();
            }
            return 0;

        default:
            Console.WriteLine("""
                orderflow-fixtures — seed and inspect triage fixtures

                  seed       Reset, then seed every failure mode
                  reset      Remove all fixture rows
                  list       Show fixture IDs and their expected diagnosis
                  probe <id> Run all six read-only tools against one order
                  probe-all  Run the probe for every fixture

                Environment: TRIAGE_ORDERS_DB, TRIAGE_INVENTORY_DB, TRIAGE_KAFKA
                """);
            return command == "help" ? 0 : 1;
    }
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Failed: {ex.Message}");
    return 1;
}

async Task SeedAsync()
{
    await seeder.SeedAsync();
    Console.WriteLine($"Seeded {Fixture.All.Count(f => f.Seeded)} fixtures.");

    // Two of these fixtures represent a consumer that has not run. If the app
    // services are up they will repair that state within seconds, so check rather
    // than let an eval silently grade against fixtures that no longer hold.
    await Task.Delay(TimeSpan.FromSeconds(3));
    var broken = await seeder.VerifyHeldAsync();

    if (broken.Count == 0)
    {
        Console.WriteLine("All fixtures held after 3s.");
        return;
    }

    Console.WriteLine();
    Console.WriteLine("WARNING — fixtures were modified after seeding:");
    foreach (var line in broken) Console.WriteLine($"  - {line}");
    Console.WriteLine();
    Console.WriteLine("The app services are running and repairing the seeded state. Stop them first:");
    Console.WriteLine("  docker compose stop order-api inventory-service notification-worker");
}

void ListFixtures()
{
    foreach (var fixture in Fixture.All)
    {
        Console.WriteLine($"{fixture.Name}");
        Console.WriteLine($"  order    {fixture.OrderId}");
        Console.WriteLine($"  state    status={fixture.OrderStatus}, outbox={fixture.Outbox}, inventory={fixture.Inventory}");
        Console.WriteLine($"  expected {fixture.ExpectedDiagnosis}");
        Console.WriteLine();
    }
}
