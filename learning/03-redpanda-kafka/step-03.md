# Step 03: Redpanda / Kafka, a log that doesn't lose data

**Goal**: put a durable "record" between the HSL broker and the files, so if the script stops the messages are not lost.

```
HSL broker ──► producer ──► Redpanda (log) ──► consumer ──► Parquet
```

## Theory (to expand)

- **Log, not database**: an append-only sequence of messages that can be re-read from the start.
- **Topic and partitions**: how the log is split, and why the key (e.g. `veh`) decides the partition.
- **Offset**: a consumer's read position.
- **Consumer group**: several consumers sharing the work.
- **At-least-once**: a message may arrive more than once, so you must deduplicate.
- **Replay**: re-reading the past after an error.
- **Consumer lag**: how far behind the log the consumer is.
- **Dead letter queue**: where messages that can't be processed end up.

## Exercise

_(to be defined when we get here)_

## Status

To do.

## My notes

_(to fill in)_