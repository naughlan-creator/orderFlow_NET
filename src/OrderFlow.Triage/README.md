# OrderFlow.Triage

Read-only diagnostic tools for answering *"what happened to this order?"* — and a
fixture generator that reproduces each failure mode on demand.

No LLM here. These are the deterministic tools an agent would later orchestrate;
they are independently useful for debugging, and they are what makes an eval set
cheap to produce.

---

## The six tools

| Tool | Answers |
|---|---|
| `get_order` | Does the order exist, what status, how old |
| `get_outbox_state` | Was the event published, how long has it been pending, how many attempts |
| `get_processed_event` | Did inventory apply it, what did it decide, why was it rejected |
| `get_stock` | Current stock for the product |
| `get_consumer_lag` | Committed offset vs high watermark per group — tells "down" from "behind" |
| `list_recent_orders_for_product` | Recent orders plus quantities by status, for cross-order questions |

Each returns an explicit `Found` flag rather than a null, so absence can't be
misread as an empty value.

## Read-only, enforced twice

Every statement is a `SELECT`, **and** each connection issues
`SET default_transaction_read_only = on`, so a write that slipped past review is
rejected by PostgreSQL:

```
ERROR:  cannot execute DELETE in a read-only transaction
```

The tools deliberately read across the orders/inventory service boundary. That's a
considered exception for read-only diagnostics — the alternative is adding debug
endpoints to both services and widening their public surface for an operator tool.
In a real deployment each connection string would point at a read replica with a
role granted `SELECT` and nothing else.

---

## Fixtures

Five seeded failure modes plus one unseeded (unknown order). Each carries an
`ExpectedDiagnosis` string, so the fixture set doubles as an eval answer key.

| Fixture | Status | Outbox | Inventory |
|---|---|---|---|
| `healthy-reserved` | InventoryReserved | published | reserved |
| `rejected-insufficient-stock` | InventoryRejected | published | rejected |
| `outbox-stuck` | Pending | **unpublished** | — |
| `awaiting-inventory` | Pending | published | — |
| `awaiting-status` | **Pending** | published | reserved |
| `unknown-order` | *(not seeded)* | — | — |

Every fixture has a unique signature across those columns, which is what lets an
eval grade a diagnosis unambiguously.

### Seeding

Two fixtures represent a consumer that *hasn't run*. Stop the app services first,
or they'll repair that state within seconds:

```bash
docker compose up -d postgres kafka kafka-init
```

```bash
docker compose stop order-api inventory-service notification-worker
```

```bash
dotnet run --project tools/OrderFlow.Triage.Fixtures -- seed
```

`seed` re-reads the fixtures after 3 seconds and warns if anything moved, so a
stale fixture set can't silently invalidate an eval run.

### Commands

```bash
dotnet run --project tools/OrderFlow.Triage.Fixtures -- list
```

```bash
dotnet run --project tools/OrderFlow.Triage.Fixtures -- probe fee00000-0000-0000-0000-000000000003
```

`probe` calls all six tools for one order and prints the combined JSON — the exact
payload an agent would reason over. `probe-all` runs it for every fixture;
`reset` removes the fixture rows.

Config via `TRIAGE_ORDERS_DB`, `TRIAGE_INVENTORY_DB`, `TRIAGE_KAFKA`.

---

## Why tools first

The failure taxonomy above is deterministic — a `switch` statement covers all six
modes. The tools implement those checks; a model's job is the multi-step
investigation, the cross-order reasoning, and explaining the answer to someone who
doesn't know what an outbox is. The model orchestrates deterministic tools; it
doesn't replace them.

Building the tools and fixtures first means the eval set is nearly free, and the
agent can be measured from its first commit rather than demoed.
