# Step 02: from message to Parquet file

**Goal**: save messages in a format suited to analysis, without writing one file per message.

```
broker ──► on_message ──► buffer (memory) ──► .parquet file ──► data/bronze/date=.../hour=.../
```

Full flow, including the failure case:

```
message → buffer → (500 messages or 60 s) → group by event hour → write .tmp → rename → .parquet
                                                         ↘ if it fails: data/failed/ as JSONL
```

## Theory

### C1. Rows vs columns

In step 01 we wrote one JSON line per message. That is fine for 200 messages, but awkward at scale.

JSON stores data **row by row**: all the fields of message 1, then all the fields of message 2.
To compute the average speed of all trams, you must read every line in full (`lat`, `long`, `odo`...)
just to pick out `spd`.

Parquet stores data **column by column**: all the `spd` together, all the `lat` together.
For the average speed it reads one column and skips the other 20. Values in the same column
look alike, so they also compress much better: a Parquet file is usually many times smaller
than the same data as JSON.

### C2. A Parquet file cannot grow

A JSON Lines file grows one line at a time, so we could write each message immediately.
A Parquet file keeps its description (schema and column positions) at the **end** of the file,
so once written it is closed.

So we cannot write one message at a time. Instead we collect messages in memory (the **buffer**)
and write them all as one file when the buffer is "full". Writing in small blocks like this is a
**micro-batch**.

**When is the buffer full?** The rule is double: "every N messages **or** every N seconds,
whichever comes first" (starting values: 500 messages, 60 seconds). Each rule alone fails:

- messages only: at night, with few trams, 500 messages could take hours. The data would sit
  in memory, invisible to readers, and lost if the script dies;
- seconds only: with many trams a lot of messages arrive per minute, so the buffer grows big
  and each file gets bigger and bigger.

With both: a busy period is triggered by the count, a quiet period by the timer.

**The risk.** The buffer lives in memory. If the script crashes or the PC switches off,
whatever is in the buffer and not yet written is lost (up to 60 seconds with our limit).
With Ctrl+C the script flushes the buffer before exiting, so nothing is lost. Only an abrupt
death loses data. For learning this is acceptable; step 03 (Redpanda) is where we make it safe.

### C3. The schema is a contract

A Parquet file has a fixed type for every column: `veh` is an integer, `lat` a decimal number,
`tst` a timestamp, `desi` text. That description is the **schema**.

The tricky column is `stop`: it is `null` while the tram is moving and a stop ID when it stands
at a stop. So the column must **allow nulls** and must have the type of a stop ID (an integer),
even in a batch with no stop at all.

**The trap.** If you let the library *guess* the schema from the first batch, and in that batch
every tram is moving, all `stop` values are `null`. There is nothing to infer a type from, so the
library picks a "null type". The next batch, with real stop IDs, doesn't match, and the files
become incompatible: tools reading them later fail or behave oddly.

So we **declare the schema by hand** (`SCHEMA` in the script), once, and write every batch with it.

**Is this specific to our case?** No, it is standard data engineering practice: a **data contract**,
or *schema-on-write*. You decide the structure when you write, and what doesn't fit is rejected
or flagged. You meet it in warehouses (BigQuery and SQL tables have declared column types),
in Kafka/Redpanda (a schema registry in front of the topic), in dbt (model contracts)
and in Delta/Iceberg tables.

The opposite is *schema-on-read*: store things as they come and interpret them when reading.
Letting a library guess is a version of it. It is fine for **exploring** (like `explore_hsl.py`),
but a pipeline that other things depend on should declare its types.

**The cost.** If HSL adds a field tomorrow, our fixed schema ignores it until we update the script.
That is why we keep the `raw_json` column (see below): nothing is lost, and we can add the new
column later and recompute from the original.

### C4. Folders matter

Files go into folders like:

```
data/bronze/date=2026-10-07/hour=16/part-....parquet
```

When you later ask "line 9 between 16:00 and 17:00", DuckDB or dbt read only the `hour=16`
folder and **skip all the others without opening them**. With millions of rows that is the
difference between reading a slice and reading everything. Splitting data into folders by a column
value is called **partitioning**.

The date and hour come from the **event time** (`tst`, UTC), not from the arrival time: the
location of a file depends on *when it happened*, not on when we received it.

Two consequences:

- A late message still lands in the right hour. A buffer flushed at 17:00:05 that holds a
  message with `tst` = 16:59:58 puts that message in `hour=16`.
- One batch can produce **two files**: if the buffer holds messages from 16:59:58 and 17:00:02,
  they go to `hour=16` and `hour=17`.

### Three more details

- **Atomic write.** Writing a file takes a moment. A reader (DuckDB, dbt) looking at that exact
  moment could find a half-written file, which is broken because its description is at the end.
  So the file is written as `part-123.parquet.tmp` and **renamed** to `part-123.parquet` only when
  complete. A rename on the same disk is instant, so a reader sees either no file or a finished
  one, never anything in between. Readers only look at `*.parquet`, so they ignore `.tmp` files.
- **`raw_json` column.** Besides the clean columns, we store the original message as text. If HSL
  adds a field, or we find a bug in our parsing, we can recompute from the original instead of
  losing data. It costs some disk space, a good trade at this size.
- **Bronze keeps everything, duplicates included.** Each event arrives 4 times (open question from
  step 01). Bronze, the raw layer, is a faithful record of what arrived, so we write all of them.
  If our deduplication has a bug we can still fix it, because the raw data is intact.
  Deduplication happens later, in silver (dbt, step 04), with the key `(oper, veh, tsi)`.

### If writing a batch fails

The disk could be full or a folder not writable. The rows are not dropped: they are saved to
`data/failed/` as JSONL (the format from step 01), so we can inspect and reprocess them.

## Pyarrow basic commands (for the exercises)

*Written without running pyarrow in the authoring environment: if a command fails or behaves differently, note it in "My notes".*

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path
```

**Find the files** (exercises 1, 5, 6, 7)

```python
files = sorted(Path("data/bronze").rglob("*.parquet"))   # every finished file, in all subfolders
print(len(files))                                          # how many files
for f in files:
    print(f.name, f.stat().st_size)                        # name and size in bytes
```

**Read one file** (exercise 2)

```python
t = pq.read_table(files[0])        # a Table
t.schema                           # column names and types (look for stop)
t.num_rows                         # number of rows
t.column_names                     # list of column names
t.slice(0, 5).to_pylist()          # first 5 rows, as a list of dictionaries
t.select(["veh", "tst", "stop"])   # a new table with only these columns
```

**Look at one column**

```python
t["stop"]                          # the column (a ChunkedArray)
t["stop"].null_count               # how many nulls
t["spd"][0].as_py()                # first value, as a normal Python number
pc.mean(t["spd"]).as_py()          # average speed
pc.max(t["spd"]).as_py()           # maximum speed
pc.value_counts(t["route"])        # how many rows per route
t.filter(pc.equal(t["route"], "1009"))   # only the rows of route 1009
```

**Look at a file without loading it** (fast, reads only the end of the file)

```python
pf = pq.ParquetFile(files[0])
pf.metadata.num_rows
pf.schema_arrow
```

**All files together** (exercises 3 and 4)

```python
all_t = pq.read_table("data/bronze")      # one table with every row; adds date and hour columns
print(all_t.num_rows)                     # compare with 3105 from the log

# or, adding up the rows file by file:
print(sum(pq.ParquetFile(f).metadata.num_rows for f in files))
```

**Duplicates** (exercise 4, through pandas)

```python
df = all_t.to_pandas()
keys = ["oper", "veh", "tsi"]
print(df.duplicated(subset=keys).sum())            # how many rows are extra copies
print(df.groupby(keys).size().value_counts())      # how many events appear 1x, 2x, 3x, 4x
```

**Size: Parquet vs JSONL** (exercise 5)

```python
t = pq.read_table(files[0])
raw = t["raw_json"].to_pylist()                    # the original messages, as text
jsonl = Path("compare.jsonl")
jsonl.write_text("\n".join(raw) + "\n", encoding="utf-8")
print("parquet:", files[0].stat().st_size, "bytes")
print("jsonl:  ", jsonl.stat().st_size, "bytes")
```

**Rows per file** (exercises 6 and 7)

```python
for f in files:
    print(f.name, pq.ParquetFile(f).metadata.num_rows)
```

Remember: a pyarrow Table is immutable. Every command above returns something new and leaves
the original untouched.

## Exercise

Install and run:

```bash
pip install paho-mqtt pyarrow      # or: uv add paho-mqtt pyarrow
python mqtt_to_parquet.py
```

Let it run for 3-5 minutes, then Ctrl+C. Then, in another script or the Python shell:

1. Look at the `data/bronze/` tree: which folders and how many files are there?
2. Read a file with `pyarrow.parquet.read_table(...)` and print the schema. Where is `null`?
3. Count the rows and compare them with what the log says it wrote.
4. Count the duplicates: how many rows share the same `(oper, veh, tsi)`?
5. Compare the size of a Parquet file with the JSONL holding the same messages.
6. Change `BATCH_MAX_SECONDS` to 10: what changes in the number of files?
7. Press Ctrl+C right after a flush and check the last file: does it contain what was in the buffer?

## Status

First real run, 9 October 2026, on my PC (real paho, pyarrow and the HSL broker):

- 3105 messages received, 3105 written, 0 unusable, in 7 files.
- Six batches of 500-556 messages, one every ~9 seconds, so the **count** rule fired each time
  (about 55 messages per second for the two lines). The 60-second rule never had to.
- The batches are slightly above 500 because the main loop checks the buffer every 0.5 s,
  so 500 is a minimum, not an exact size.
- After Ctrl+C the final flush wrote the last message in its own 1-message file: a clean
  shutdown works, but it leaves a tiny file (the "small files" problem; compaction comes in step 05).

Exercises:

- **1 (folder tree):** done. 7 files in one folder, `date=2026-10-09/hour=17`. The hour is UTC, while my clock said 19:17.
- **2 (schema):** done. Types match the contract; `stop` is `int64` in pyarrow (pandas shows `float64` because of the nulls). Only `stop` has nulls in the sample file.
- **3 (row count):** done. 3105 rows in the files = 3105 in the log.
- **4-7:** skipped for now. The duplicate count (4) is done in step 04, when dbt deduplicates.

## My notes

_(to fill in)_