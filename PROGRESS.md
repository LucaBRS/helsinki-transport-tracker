# PROGRESS

Last updated: 2026-11-19.

## Where we are

Step 03 in progress: producer works, `consumer.py` parts 1-2 written and tested by Luca (raw bronze design).
Next concrete step: part 4 (write Parquet -> flush dead letters -> commit), theory first.

## Steps

| Step | Folder | Status |
|---|---|---|
| 01 MQTT connection | `learning/01-mqtt-connessione/` | done (explore_hsl.py, explore_two_routes.py, notes and questions) |
| 02 Parquet micro-batch | `learning/02-parquet-microbatch/` | done; typed bronze **kept on purpose** as a learning exercise (note at the top of `step-02.md`); exercises 4-7 deferred |
| 03 Redpanda / Kafka | `learning/03-redpanda-kafka/` | producer works; `consumer_1_just_read_first_10.py` works; `consumer_2_just_test_broken_msg.py` works; `consumer_3_just_poll_and_buffer.py` works (part 3) |
| 04 dbt silver/gold | `learning/04-dbt-silver-gold/` | not started; part 0 = local IaC planned |
| 05 Orchestration | `learning/05-orchestrazione/` | not started (stub); Dagster vs Airflow undecided |
| 06 Fabric | `learning/06-fabric/` | not started (stub) |

## What exists in step 03

- `docker-compose.yml`: Redpanda (`redpanda-0`, external address `localhost:19092`) + console (`http://localhost:8080`), volume `redpanda-0` keeps the log.
- Topic `hsl.tram.positions` (1 partition, 7 days) with messages from offset 905 (older ones removed by retention, Q28).
- Topic `hsl.tram.positions.dead` (1 partition, 30 days), created by hand.
- `producer.py`: MQTT -> envelope `{"mqtt_topic", "received_at", "payload"}`, key = `operator/vehicle`,
  async `produce()`, `poll()` loop with counters, `flush(10)` at exit. Tested on the real setup: works.
- `consumer_1_just_read_first_10.py` (renamed from `consumer_read.py`): reads N messages with group `debug-reader`,
  never commits, prints `tsi` and `mqtt_topic`. Works.
- `consumer_2_just_test_broken_msg.py` (was `consumer.py` after parts 1-2, renamed 2026-11-19): topic check +
  raw bronze `parse_message` on 6 hand-made examples. Tested by Luca: 3x bad envelope, 1x schema mismatch,
  non-JSON payload kept, good payload kept. Kept as a test bench.
- `consumer_3_just_poll_and_buffer.py` (part 3, 2026-11-19): subscribe, `poll(1.0)`, parse, buffer, batch due at
  500 rows / 60 s (only printed), WARNING on an empty window. No write, no DLQ, no commit; own group `debug-poll-buffer`.
  Tested by Luca 2026-11-19 (producer off): offsets 905-8192 = 14 batches of 500 + 1 of 288, 0 broken, then WARNING.
  One batch spanned 2026-11-18 14:33 and 2026-11-19 16:17 (two producer runs) -> part 4 must split a batch by partition folder.
- `consumer.py`: name reserved for the complete consumer (parts 4-5). Until then each part is a numbered snapshot `consumer_N_just_*.py` (N = order of creation).
- `step-03.md`, `step-03-questions.md` (Q1-Q29), `step-03-commands.md`, `threads.png` (three-thread diagram).
- `messages.json`: export from the Redpanda console of 4 records of `__consumer_offsets`
  (group `debug-reader` joining/leaving, no commits). Committed in git; keep or delete: to decide.

## Design agreed for `consumer.py`

- **Raw bronze.** The HSL payload is never opened in the consumer. Bronze contract (`BRONZE_SCHEMA`):
  `received_at`, `mqtt_topic`, `kafka_key`, `kafka_partition`, `kafka_offset`, `payload` (text, untouched).
- Two checks per message: 1) envelope readable (JSON, keys present, `received_at` valid) -> else `"bad envelope"`;
  2) the row respects `BRONZE_SCHEMA`, checked one message at a time -> else `"schema mismatch"`.
  Broken messages -> `hsl.tram.positions.dead` with the reason; the offset moves on.
- One thread, one loop: `poll(1.0)` -> `parse_message` -> buffer. No lock needed.
- Buffer due at 500 messages or 60 s (also checked when `poll` returns `None`, timer with `time.monotonic()`).
- Empty window (60 s with no messages): **no empty Parquet file**, print a warning line instead (standard split:
  the consumer only REPORTS silence; deciding if it is a problem is a freshness alert in step 05, which knows the tram hours; consumer lag is a separate alert).
- Order on every batch: **1) write Parquet (atomic, .tmp then rename), 2) `flush()` the dead letter
  producer, 3) `consumer.commit(asynchronous=False)`**.
- Settings: `group.id = "parquet-writer"`, `enable.auto.commit = False`, `auto.offset.reset = "earliest"`.
- Output folder computed from the location of `consumer.py` (not the current directory).
- No import from step 02: pieces are copied and adapted (each step is self-contained).
- The script only CHECKS that both topics exist; creating them is setup (`step-03-commands.md`, section 2).
- Ctrl+C: write the remaining buffer, flush, commit, `close()`.
- Built in 5 parts: 1) config + topic check (done), 2) parse (written), 3) poll loop + buffer,
  4) write -> flush -> commit, 5) Ctrl+C + manual broken-message test. Tests: one command at a time, failure first.

## Decisions (and why)

- Redpanda between MQTT and files: if the writer stops, messages wait in the log. Producer stays tiny and "dumb".
- At-least-once (write, then commit): lost data cannot be recovered, duplicates can be removed in silver.
- **Bronze is raw** (2026-11-18): Redpanda keeps 7 days, after that bronze is the only copy, so it keeps
  everything. Bronze only rejects what it cannot store (broken envelope); it never judges the content.
  Parsing, the full typed contract (oper, veh, tst, ...; dbt model contract) and an error table go to silver.
- **Bronze partitioned by `received_at`** (arrival time, UTC) `date=YYYY-MM-DD/hour=HH`, because the payload
  is not opened. Event time (`tst`) and late data are handled in silver. (Replaces "partitioned by event time".)
- Schemas are always declared by hand (never inferred): small one in bronze, full one in silver.
- Partition key = `operator/vehicle` so each tram's events stay in order.
- Port 1883 without TLS for the HSL broker (8883 certificate error on this PC).
- **4 copies of every event: confirmed real duplicates from HSL** (same MQTT topic, same `tsi`, interleaved
  with other trams; Q26). Cause on the HSL side unknown, outside our code. Bronze keeps them, silver dedups on `(oper, veh, tsi)`.
- DLQ topic retention 30 days (dead letters are evidence and may be noticed late).

## Open problems

- **Topic auto-creation is on** (`--mode dev-container` -> `auto_create_topics_enabled = true`): writing to a
  missing topic creates it silently with default settings. Always create topics by hand; scripts check at startup (Q29).
- Redpanda TRIAL license expires around 2026-11-20 (warning printed by `rpk`). Probably harmless for one broker;
  if Redpanda behaves strangely after that date, start from here.
- `producer.py` has not been tested for failures yet (Redpanda stopped while running, etc.).
  The table in `step-03-commands.md` lists the tests.
- `step-03.md` theory still describes the typed bronze in places (e.g. "write Parquet" with schema/partitioning
  from step 02): to align with the raw bronze decision.

## Next steps

1. ~~Luca runs `consumer.py` part 2~~ done 2026-11-19.
2. ~~Change step 02 to raw bronze~~ dropped 2026-11-19: step 02 stays typed as a learning exercise;
   the real bronze/silver/gold structure starts from step 03 (note added at the top of `step-02.md`).
3. `consumer.py` parts 3-5; run it; check the Parquet files and `rpk group describe parquet-writer`.
4. Failure tests: stop the consumer for a minute (lag grows then falls), kill it right after a write
   (do duplicates appear?), stop the producer (what is lost and why the log cannot help).
5. Update `step-03.md` (hands-on part, status, raw bronze) and `step-03-questions.md`.
6. **Start of step 04: local IaC.** Step 03 stays manual on purpose (learn by hand first, automate after).
   Step 04 starts from scratch, as if nothing existed (no container, no volume, no topics), and rebuilds
   everything with Docker + one setup for the whole local infrastructure (Redpanda up + both topics), so
   step 04 can focus on dbt. Separate idempotent script, NOT inside `docker-compose.yml`, named
   `local_iac_create_topics.sh` (name agreed; may grow to cover all local setup). A comment at the top
   must say it is a local-only stand-in for real IaC (Terraform, Fabric deployment), not for the cloud.
   Then `consumer.py`, `producer.py` and `step-03-commands.md` point to it instead of repeating the settings.
7. Step 04: dbt-duckdb silver: parse the raw payload, typed contract, error table, dedup on `(oper, veh, tsi)`; then gold.
8. Step 05: Dagster vs Airflow decision, lag and freshness alerts, **DLQ alert** (notify when
   `hsl.tram.positions.dead` grows), compaction of small files.
9. Step 06: Fabric (Eventstream accepts Kafka clients, so the producer code carries over).
   In Fabric the DLQ becomes an error table where the parsing happens (to verify in step 06).
10. Later, after Fabric: a connection with Databricks too (wish, not planned in detail yet).

## Housekeeping

- The root `README.md` is not updated yet on purpose; only `learning/learning_readme.md` is current.
- `learning/.gitignore` ignores `**/data/`, `**/samples/`, `**/failed/`, `*.jsonl`, `*.parquet`, `*.tmp`, `__pycache__/`.
  The root `.gitignore` is the standard Python template.
