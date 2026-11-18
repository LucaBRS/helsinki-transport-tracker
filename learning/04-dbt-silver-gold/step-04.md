# Step 04: dbt, from bronze to silver and gold

**Goal**: remove duplicates, reason about time, and produce tables ready to query.

```
bronze (raw Parquet) ──► silver (clean, deduplicated) ──► gold (aggregates)
```

## Theory (to expand)

- **Deduplication** on the key `(oper, veh, tsi)`: every HSL event arrives several times.
- **Event time vs processing time**: `tst` vs `received_at`.
- **Late data**: a recompute window (lookback) for messages that arrive late.
- **Time windows**: aggregate per minute, per hour, per trip.
- **Incremental models**: process only the new data.
- **dbt tests**: key uniqueness, non-null values, plausible ranges.
- **Example gold tables**: average speed per line and hour, average delay per stop.

## Exercise

**Part 0: infrastructure first, automated.** The step starts from scratch, as if nothing existed
(no container, no volume, no topics). Instead of the manual commands of step 03, the local
infrastructure is created by code: Docker (`docker compose up -d`) plus a local IaC script,
`local_iac_create_topics.sh`, that creates the topics with their settings and can be run any
number of times (idempotent). The script is a local-only stand-in for real IaC (Terraform,
Fabric deployment) and says so in a comment at the top. Idea to learn: infrastructure is
created automatically first, then the code that runs on it.

Then producer and consumer fill bronze, and the dbt part begins.

_(the dbt exercises are to be defined when we get here)_

## Status

To do.

## My notes

_(to fill in)_