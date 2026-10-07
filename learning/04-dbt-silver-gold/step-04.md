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

_(to be defined when we get here)_

## Status

To do.

## My notes

_(to fill in)_