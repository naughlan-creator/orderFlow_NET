using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Inventory.Service.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddProcessedEventsAndConcurrencyToken : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            // The scaffolder emits an AddColumn for "xmin" here. It is deliberately
            // removed: xmin is a PostgreSQL system column that already exists on every
            // table, and CREATE/ALTER against it fails with "column name \"xmin\"
            // conflicts with a system column name". The model snapshot still maps it,
            // so EF treats the schema as up to date and uses it as a concurrency token.

            migrationBuilder.CreateTable(
                name: "processed_events",
                schema: "inventory",
                columns: table => new
                {
                    EventId = table.Column<Guid>(type: "uuid", nullable: false),
                    OutcomeTopic = table.Column<string>(type: "character varying(120)", maxLength: 120, nullable: false),
                    OutcomePayload = table.Column<string>(type: "text", nullable: false),
                    ProcessedAtUtc = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_processed_events", x => x.EventId);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "processed_events",
                schema: "inventory");

            // No DropColumn for "xmin" — see the note in Up().
        }
    }
}
