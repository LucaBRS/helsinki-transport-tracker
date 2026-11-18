# PROGRESS

Last updated: 2026-11-18 (end of the chat session; first session in VS Code comes next).

## Where we are

Step 03 in progress: the producer works, the consumer is half done.
Next concrete step: write `consumer.py` (part 2), run it, verify the Parquet files and the committed offset.

## Steps

| Step | Folder | Status |
|---|---|---|
| 01 MQTT connection | `learning/01-mqtt-connessione/` | done (explore_hsl.py, explore_two_routes.py, notes and questions) |
| 02 Parquet micro-batch | `learning/02-parquet-microbatch/` | done; exercises 4-7 deferred (duplicate count moves to step 04) |
| 03 Redpanda / Kafka | `learning/03-redpanda-kafka/` | theory done; Redpanda running; `producer.py` works; `consumer_read.py` works; **`consumer.py` to do** |
| 04 dbt silver/gold | `learning/04-dbt-silver-gold/` | not started (stub) |
| 05 Orchestration | `learning/05-orchestrazione/` | not started (stub); Dagster vs Airflow undecided |
| 06 Fabric | `learning/06-fabric/` | not started (stub) |

## What exists in step 03

- `docker-compose.yml`: Redpanda (`redpanda-0`, external address `localhost:19092`) + console (`http://localhost:8080`), volume `redpanda-0` keeps the log.
- Topic `hsl.tram.positions` (1 partition, retention 7 days). On 2026-11-18 it is empty: retention removed
  offsets 0-904, the next message gets offset 905 (Q28). Run `producer.py` to fill it again.
- `producer.py`: MQTT -> envelope `{"mqtt_topic", "received_at", "payload"}`, key = `operator/vehicle`,
  async `produce()`, `poll()` loop with counters, `flush(10)` at exit. Tested on the real setup: works.
- `consumer_read.py`: reads 10 messages with group `debug-reader`, never commits. Works.
- `step-03.md`, `step-03-questions.md` (Q1-Q29), `step-03-commands.md`, `threads.png` (three-thread diagram).
- `messages.json`: export from the Redpanda console of 4 records of the internal topic `__consumer_offsets`
  (group `debug-reader` joining/leaving, no committed offsets). Committed in git; keep or delete: to decide.

## Design agreed for `consumer.py` (not written yet)

- One thread, one loop: `poll(1.0)` -> parse envelope -> parse inner payload with the step 02 `build_row`
  -> buffer. No lock needed.
- Buffer due at 500 messages or 60 s (also checked when `poll` returns `None`).
- Order on every batch: **1) write Parquet (atomic, .tmp then rename), 2) `flush()` the dead letter
  producer, 3) `consumer.commit(asynchronous=False)`**.
- Settings: `group.id = "parquet-writer"`, `enable.auto.commit = False`, `auto.offset.reset = "earliest"`.
- Bad messages (broken envelope, broken payload, missing `tst`/`veh`/`oper`) -> topic
  `hsl.tram.positions.dead` with the reason; the offset moves on.
- Reuse from `learning/02-parquet-microbatch/mqtt_to_parquet.py`: `SCHEMA`, `build_row`,
  `group_by_partition`, `write_batch` (atomic write).
- `received_at` column: take it from the envelope (written by the producer), not from the consumer clock.
- Ctrl+C: write the remaining buffer, flush, commit, `close()`.
- Create the dead letter topic: `docker exec redpanda-0 rpk topic create hsl.tram.positions.dead -p 1`.
- At startup check that both topics exist (like `producer.py` does), because auto-creation is on (see Open problems).

## Decisions (and why)

- Redpanda between MQTT and files: if the writer stops, messages wait in the log. Producer stays tiny and "dumb".
- At-least-once (write, then commit): lost data cannot be recovered, duplicates can be removed in silver.
- Explicit Parquet schema (data contract) because `stop` is often null and type inference breaks between batches.
- Bronze partitioned by **event time** (UTC) `date=YYYY-MM-DD/hour=HH`; keeps `raw_json`; keeps duplicates.
- Partition key = `operator/vehicle` so each tram's events stay in order.
- Port 1883 without TLS for the HSL broker (8883 certificate error on this PC).

## Open problems

- **4 copies of every event.** In `consumer_read.py` output, 4 messages with the same key and speed
  arrive less than 1 ms apart. Hypothesis: HSL publishes the same event on several MQTT topics.
  Test: run `producer.py` again, then print `envelope["mqtt_topic"]` for 4 consecutive copies
  (offsets 0-3 no longer exist). Write the result at the end of Q26 in
  `step-03-questions.md`. Deduplication itself happens in step 04.
- **Topic auto-creation is on** (`--mode dev-container` -> `auto_create_topics_enabled = true`): writing to a
  missing topic creates it silently with default settings. Always create topics by hand; scripts check at startup (Q29).
- Redpanda TRIAL license expires around 2026-11-20 (warning printed by `rpk`). Probably harmless for one broker;
  if Redpanda behaves strangely after that date, start from here.
- `producer.py` has not been tested for failures yet (Redpanda stopped while running, etc.).
  The table in `step-03-commands.md` lists the tests.

## Next steps

1. Write and run `consumer.py`; check the Parquet files and `rpk group describe parquet-writer`.
2. Failure tests: stop the consumer for a minute (lag grows then falls), kill it right after a write
   (do duplicates appear?), stop the producer (what is lost and why the log cannot help).
3. Update `step-03.md` (hands-on part, status) and `step-03-questions.md`.
4. Step 04: dbt-duckdb silver (dedup on `(oper, veh, tsi)`), then gold.
5. Step 05: Dagster vs Airflow decision, lag and freshness alerts, compaction of small files.
6. Step 06: Fabric (Eventstream accepts Kafka clients, so the producer code carries over).

## Housekeeping

- The root `README.md` is not updated yet on purpose; only `learning/learning_readme.md` is current.
- `learning/.gitignore` ignores `**/data/`, `**/samples/`, `**/failed/`, `*.jsonl`, `*.parquet`, `*.tmp`, `__pycache__/`.
  The root `.gitignore` is the standard Python template.
- `CLAUDE.md` and `PROGRESS.md` are not committed yet.
