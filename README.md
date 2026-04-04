# HeerRanjId SQL Specification

This repository contains reusable SQL assets for the HeerId and RanjId
identifier system.

The goal of this SQL layer is to keep the identifier model, schema, and
database behavior portable across multiple application stacks by placing as
much backend-specific logic as possible in shared SQL files.

## Overview

HeerId and RanjId are time-ordered identifiers designed to be database-native.

- `HeerId` is a 64-bit identifier intended for standard entity primary keys.
- `RanjId` is a 128-bit identifier intended for high-precision event and log
  streams.
- Both are sortable in database-native form.
- Both support single-node deployments first, while allowing expansion to
  multi-node deployments without schema changes.

## Design Goals

- feel simple in single-node applications
- scale without schema changes
- avoid centralized ID services
- remain database-native
- stay portable across database backends
- support long system lifetimes

## Core Principles

- single-node by default
- distributed-ready by design
- fail fast on misconfiguration
- database enforces correctness
- application configuration defines node identity
- no runtime coordination required
- stable format over time
- database-native ergonomics

## Identifier Format

### HeerId

HeerId is a signed 64-bit integer with 63 usable bits.

| 41-bit timestamp | 9-bit node_id | 13-bit sequence |

- timestamp is measured in milliseconds since a custom epoch
- node_id supports up to 512 nodes
- sequence supports up to 8192 IDs per millisecond per node

### RanjId

RanjId is a 128-bit identifier structured for UUIDv7 RFC 4122 compliance.
Its effective payload is `90-bit timestamp + 16-bit node_id + 16-bit sequence`
with version and variant bits reserved for UUID semantics.

| Bit Range | Length | Content | Note |
| :--- | :--- | :--- | :--- |
| 0 - 47 | 48 bits | Timestamp (High) | Part 1 of 96-bit microsecond timestamp |
| 48 - 51 | 4 bits | Version (`0111`) | UUIDv7 marker |
| 52 - 63 | 12 bits | Timestamp (Mid) | Part 2 of timestamp |
| 64 - 65 | 2 bits | Variant (`10`) | RFC 4122 marker |
| 66 - 95 | 30 bits | Timestamp (Low) | Part 3 of timestamp |
| 96 - 111 | 16 bits | Node ID | Supports 65,536 nodes |
| 112 - 127 | 16 bits | Sequence | Supports 65,536 IDs per microsecond |

## Field Definitions

### Timestamp

- HeerId uses a 41-bit millisecond timestamp
- RanjId uses a 96-bit physical microsecond timestamp with 90 effective payload bits
- the epoch is not hardcoded by this SQL repository
- the active epoch is stored in `heer_config`

### Node ID

- node identity must be unique per writer
- node identity must exist in `heer_nodes`
- node IDs may be statically assigned for long-lived infrastructure
- ephemeral infrastructure may lease and recycle node IDs through the registry

### Sequence

- HeerId sequence range: `0..8191`
- RanjId sequence range: `0..65535`

## Schema

The shared PostgreSQL schema lives in
[`postgres/schema.sql`](./postgres/schema.sql).

The core tables are:

- `heer_nodes`: registry of valid nodes
- `heer_config`: singleton configuration row containing the custom epoch
- `heer_node_state`: state used for HeerId generation
- `heer_ranj_node_state`: state used for RanjId generation

### Default Seed Values

- `node_id = 1`
- `name = "default"`

### Table Semantics

`heer_nodes` records valid node identifiers and supports node recycling through
`is_active`.

`heer_config` contains exactly one logical configuration row and defines the
epoch used for timestamp encoding.

`heer_node_state` stores the last generated millisecond and sequence for each
node.

`heer_ranj_node_state` stores the last generated microsecond timestamp and
sequence for each node.

## Node Identity

The application is responsible for supplying the active node identity.

That node identity must:

- exist
- be valid
- be unique per writer
- exist in `heer_nodes`

## Session-Based Node Configuration

The SQL API assumes a session-scoped node configuration mechanism:

```sql
set_heer_node_id(node_id INTEGER);
current_heer_node_id() RETURNS INTEGER;
```

This allows callers to bind a node identity to the current connection or
session before generating identifiers.

## Generation API

The SQL API is intended to expose these entry points:

```sql
generate_id() RETURNS BIGINT;
generate_id(node_id INTEGER) RETURNS BIGINT;
```

For bulk allocation:

```sql
generate_ids(count INTEGER) RETURNS TABLE(id BIGINT);

generate_ids(
    count INTEGER,
    allow_spanning BOOLEAN DEFAULT true
) RETURNS TABLE(id BIGINT);

generate_ids(
    node_id INTEGER,
    count INTEGER,
    allow_spanning BOOLEAN DEFAULT true
) RETURNS TABLE(id BIGINT);
```

### Bulk Behavior

- returns exactly `count` IDs
- strictly increasing within a batch
- fully concurrency-safe
- uses a read-once, compute, write-once state update
- performs exactly one update to `heer_node_state` for the full batch
- may span multiple milliseconds when needed
- should keep locks only for the read and write window

## Column Defaults

The SQL layer is intended to support native database defaults such as:

```sql
id BIGINT PRIMARY KEY DEFAULT generate_id();
```

## Guarantees

### Provided

- HeerId provides millisecond precision
- HeerId is K-sortable by node
- RanjId provides microsecond precision
- RanjId is strictly sortable by Time -> Node -> Sequence
- RanjId provides deterministic uniqueness across 65,536 nodes
- both identifiers are database-native and sortable in stored form

### Not Provided

- anonymity
- strict global ordering
- exact timestamp equality

Both identifiers intentionally expose creation time and node identity.

## Scaling Model

A deployment may start with a single node and later add more nodes without
changing the schema.

The important scaling rule is simple:

- each writer must use a unique node ID
- each node ID must be registered
- schema changes are not required when adding nodes

## Clock and Sequence Edge Cases

### Clock Rollback

- minor drift under 50ms should raise an error so the caller can retry
- major drift over 50ms should fail fast
- database logic should not sleep or stall while handling rollback conditions

### Sequence Overflow

- advance the timestamp by one unit
- reset sequence to zero

## Clock Skew Between Nodes

- expected in distributed deployments
- slight ordering differences across nodes are acceptable
- use `created_at` when strict external ordering is required

## Failure Modes

| Issue | Cause | Mitigation |
|------|------|-----------|
| ID collision | duplicate node_id | enforce registry |
| startup failure | invalid node_id | fail fast |
| ordering drift | clock skew | expected |
| generator stall | overflow or rollback | brief blocking |
| clock rollback error | severe drift | error plus NTP |
| session missing | node not set | runtime error |

## Best Practices

- store `created_at` alongside generated IDs for auditing
- treat `node_id` as infrastructure, not user data
- use `generate_ids()` for bulk work to reduce contention
- use NTP in slew mode to avoid clock jumps
- use native database column types for each identifier format
- prevent node ID reuse collisions through the `heer_nodes` registry
- set the session node explicitly for each connection
- avoid relying on strict global ordering across nodes

## Summary

- simple defaults
- raw SQL compatibility
- high throughput
- safe concurrency
- scalable architecture

| Feature | HeerId | RanjId |
| :--- | :--- | :--- |
| Bit Width | 64-bit | 128-bit |
| PostgreSQL Type | `BIGINT` | `UUID` |
| Precision | Millisecond | Microsecond |
| Timestamp Bits | 41 bits | 96 bits (90 effective) |
| Node ID Bits | 9 bits (512) | 16 bits (65,536) |
| Sequence Bits | 13 bits (8,192/ms) | 16 bits (65,536 per microsecond) |
| Max Lifespan | ~69 years | ~2.5 trillion years |
