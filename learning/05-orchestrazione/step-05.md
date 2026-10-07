# Step 05: orchestration

**Goal**: run and monitor the whole pipeline, not just the batch jobs.

## Theory (to expand)

- **Always-on services vs batch jobs**: the MQTT/Kafka consumer never finishes, dbt does.
- **Who restarts whom**: what happens if the consumer dies.
- **Freshness and lag checks**: "is the latest data less than 2 minutes old?"
- **Compaction**: merging many small files into larger ones.
- **Dagster vs Airflow**: choice still open, to be decided with a concrete example.
- **Dependencies and retries**: order of steps, what to do if one fails.

## Exercise

_(to be defined when we get here)_

## Status

To do.

## My notes

_(to fill in)_