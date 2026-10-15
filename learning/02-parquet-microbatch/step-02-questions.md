# Step 02: questions and answers

Questions that came up while studying `mqtt_to_parquet.py`.

---

## 1. How does pyarrow work? What is it for, what is it not for, how is it used?

(`pa.` is just the nickname from `import pyarrow as pa`, and `pq` is `pyarrow.parquet`.)

*Note: the code snippets below come from the library's standard API; they were not run in the environment where this was written.*

### What pyarrow is

**pyarrow** is the Python library for **Apache Arrow**, a standard for holding tabular data in memory **column by column**. It has two jobs:

1. **Arrow in memory:** typed columns, tables and schemas, plus fast operations on them.
2. **Reading and writing files**, mainly **Parquet** (also CSV, JSON and others).

Arrow and Parquet are different things that fit together:

| | Arrow | Parquet |
|---|---|---|
| Where | in memory (RAM) | on disk |
| Purpose | fast to work with | small and fast to store and scan |
| Layout | columnar | columnar, compressed |

### Why it exists

Before Arrow, every tool (pandas, Spark, databases) held tables in its own internal format. Moving data between them meant converting and copying everything. Arrow is a common format that many tools understand: pandas, Polars, DuckDB, Spark and others can pass Arrow tables to each other with little or no copying. In practice, pyarrow is often the glue between tools and the engine that reads and writes Parquet underneath them.

### The main pieces

**1. Types and schema (`pa`)**

```python
pa.int32(), pa.float64(), pa.string(), pa.timestamp("ms", tz="UTC")
pa.field("veh", pa.int32())              # one named column with a type
pa.schema([field1, field2, ...])         # the full contract
```

This is what we use in `SCHEMA`.

**2. Arrays and tables (`pa`)**

- An **Array** is one typed column.
- A **Table** is a set of named columns, i.e. what you would call a DataFrame.

```python
table = pa.table({"veh": [629, 630], "spd": [7.2, 0.0]})
table = pa.Table.from_pylist(list_of_dicts, schema=SCHEMA)   # what we use
table.schema          # column names and types
table.num_rows        # number of rows
table.column("spd")   # one column
table.to_pylist()     # back to a list of dictionaries
table.to_pandas()     # to a pandas DataFrame
```

Important: Arrow arrays are **immutable**. You don't edit a value in place, you build a new table.

**3. Parquet files (`pq`)**

```python
pq.write_table(table, "file.parquet", compression="snappy")    # write
table = pq.read_table("file.parquet")                           # read all
table = pq.read_table("file.parquet", columns=["veh", "spd"])  # only some columns
```

Reading only some columns is the columnar advantage from theory C1 in practice: the other columns are not read from disk.

You can also inspect a file without loading it:

```python
pq.ParquetFile("file.parquet").schema_arrow
pq.ParquetFile("file.parquet").metadata.num_rows
```

**4. Datasets: many files as one table (`pyarrow.dataset`)**

Our `data/bronze/` folder has many files in `date=/hour=` folders. A dataset treats all of them as one table:

```python
import pyarrow.dataset as ds
dataset = ds.dataset("data/bronze", format="parquet", partitioning="hive")
table = dataset.to_table(filter=ds.field("hour") == 16)
```

`partitioning="hive"` makes it understand the `date=...` / `hour=...` folder names as columns. Folders that don't match the filter are skipped without being opened, which is the benefit of the partitioning from C4.

**5. Compute functions (`pyarrow.compute`)**

```python
import pyarrow.compute as pc
pc.mean(table["spd"])
pc.value_counts(table["route"])
```

Fast filters, aggregations and string/date operations on columns.

### What it is good for

- Writing Parquet with an explicit schema, as in our script.
- Reading Parquet quickly, only the columns and files you need.
- Moving data between tools without copying (pandas, Polars, DuckDB).
- Checking a file's schema and metadata.

### What it is not

- **Not a database.** No SQL, no indexes, no transactions, no server.
- **Not for editing data in place.** Tables are immutable and Parquet files are closed once written. To "change" something you write a new table or file.
- **Not for appending row by row.** It works on blocks. That is why we use a buffer and micro-batches.
- **Not a full analysis environment.** `pyarrow.compute` covers the basics, but for real analysis or SQL you normally hand the data to pandas, Polars or DuckDB.
- **Not light on memory by default.** A table is held in RAM. For data bigger than memory you use datasets with filters and batches.
- **Verbose.** The API is low level: more typing than pandas for simple tasks.

### Pyarrow vs the tools around it

| Tool | What it is | When |
|---|---|---|
| **pyarrow** | Arrow tables and the Parquet reader/writer | the writing layer, schemas, glue |
| **pandas** | DataFrames with a friendly API | small to medium analysis in Python |
| **Polars** | fast DataFrame library | larger data in Python |
| **DuckDB** | in-process SQL engine; reads Parquet directly | SQL on our bronze files |

### How we use it in this project

- **Now (step 2):** schema + `from_pylist` + `write_table`. We use pyarrow only as the Parquet writer.
- **Step 4:** we will mostly *not* read the files with pyarrow. DuckDB (through dbt) reads the Parquet files with SQL, and pyarrow stays in the background as the format bridge.
- **Exercises 2 and 3 in step-02.md:** you will use `pq.read_table` and `.schema` to look at the files by hand.

### Quick things to try in the exercises

```python
import pyarrow.parquet as pq

t = pq.read_table("path/to/a/file.parquet")
print(t.schema)                    # exercise 2: look for the nullable columns
print(t.num_rows)                  # exercise 3: compare with the log
print(t.slice(0, 5).to_pylist())   # first 5 rows as dictionaries
```

---

## 2. Is `parse_iso_utc` a data-type transformation? Should it be done in silver?

Yes, it turns text (`"2026-10-07T13:51:00.255Z"`) into a real datetime. Whether that belongs in bronze or silver depends on the team. Here is how it was decided:

- **We must do it in bronze anyway.** The folder of each file is chosen from `tst`, and we can't compute `date=` and `hour=` from text without parsing it.
- **Nothing is lost.** The original text stays in `raw_json`.

So in bronze we do only the **light typing** needed to store the data well in Parquet. Real cleaning and logic go to silver: deduplication, removing impossible values, joins, business rules.

**Known limit:** if a message is unusable (for example `tst` is missing), `build_row` returns `None`. The script counts it in `bad` but **does not save it anywhere**. A stricter pipeline would also write those to `data/failed/`.

---

## 3. Why `payload_bytes.decode("utf-8")`? Isn't `msg.payload` already JSON?

No. MQTT carries raw **bytes**. The content happens to be JSON text, but paho gives it to us as bytes, like `b'{"VP": {...}}'`. Two conversions are needed:

```
bytes  --decode("utf-8")-->  text (str)  --json.loads-->  dictionary
```

In step 01 we passed the bytes straight to `json.loads`, which also accepts bytes. Here we decode first because we want to keep the **text** for the `raw_json` column.

---

## 4. What do the `veh` and `oper` checks do?

```python
if vp.get("veh") is None:
    return None
```

It checks that the message says *which vehicle* it comes from. `vp.get("veh")` returns `None` if the field is missing or null, and then we discard the message.

Why: our deduplication key is `(oper, veh, tsi)`. A row without a vehicle or operator can't be identified or deduplicated.

It does **not** prove the message is 100% valid, it only checks those two fields. (`tst` is checked indirectly, because parsing it fails if it is missing. `tsi`, the third part of the key, is not checked explicitly.)

---

## 5. How does `base / ("date=" + date_text) / ("hour=" + hour_text)` work?

It **does not create** folders, it only builds a path.

- `"date=" + date_text` joins two pieces of text: `"date="` and `"2026-10-07"` become `"date=2026-10-07"`.
- `/` on `Path` objects means "join path segments". It is not division. `Path("data/bronze") / "date=2026-10-07" / "hour=16"` gives `data/bronze/date=2026-10-07/hour=16`.

The folders are created later, in `write_batch`, by `folder.mkdir(parents=True, exist_ok=True)`: `parents=True` creates all missing levels at once, `exist_ok=True` means no error if it already exists.

---

## 6. Is C4 just a temporary dictionary for edge cases?

Half right. The dictionary from `group_by_partition` is temporary: built for this batch, thrown away after the files are written. But it is not only for boundary cases.

**Every batch** goes through it, because we always need to decide in which folder each row goes. Normally everything lands in one folder (one group, one file). Only at an hour boundary do you get two groups and two files.

The real point of C4 is the folder structure. The dictionary is the tool that sorts rows into those folders.

---

## 7. Where is the `writer(group, tmp_path)` function?

`writer` is a **parameter** of `write_batch`:

```python
def write_batch(rows, base=OUTPUT_DIR, writer=write_parquet_file):
```

`=write_parquet_file` is its default value. In Python, functions can be passed around like any other value. So `writer(group, tmp_path)` is a call to `write_parquet_file(group, tmp_path)`.

It is written this way so tests can swap in a fake writer and simulate failures. In real use it always means `write_parquet_file`.

---

## 8. Is the `Buffer` class there because of the two threads?

Partly. The two threads are the real reason for the **lock** (question 10).

The class itself is a convenience: it bundles the list of rows, the timer, the lock and the rules (`add`, `is_due`, `drain`). We could use global variables and loose functions, but a class keeps it tidy.

The buffer doesn't save anything. It only **holds** rows in memory. The saving is done by `write_batch`, called from the main thread.

---

## 9. What does `self.last_flush = time.monotonic()` do? How does `time` work?

`last_flush` stores "the moment of the last write". `is_due` computes `now - self.last_flush` (seconds elapsed) and compares it with 60. When the buffer is created, the timer starts from that moment.

The `time` module gives us three things:

- `time.sleep(0.5)` pauses the script for half a second;
- `time.time()` is the normal clock, in seconds since 1970. It can **jump** (clock sync, manual change);
- `time.monotonic()` **only moves forward** and never jumps. Its absolute value means nothing, but the difference between two readings is always a real duration.

So `monotonic` is the right clock for "how many seconds since X".

Example: `last_flush` = 1000.0, now = 1061.5, so `61.5 >= 60` and the buffer is due.

---

## 10. How does `self.lock` work? What are threads?

A **thread** is a line of execution inside a program. Our script has two running at the same time, sharing the same memory:

- **Thread A (paho, background):** receives messages and calls `buffer.add(row)`;
- **Thread B (main):** every 0.5 seconds calls `buffer.is_due()` and `buffer.drain()`.

The danger is both touching the same list at the same moment. `drain` does three steps: take the list, replace it with an empty one, reset the timer. If a message is added exactly between those steps, a row could land on the old list while it is already being written, and be lost or counted wrongly. This is a **race condition**. It is rare, so it passes tests and then fails once in a while.

A **lock** is a room with a single key:

- `with self.lock:` means "take the key and enter";
- if the other thread holds the key, you **wait**;
- when the block ends, the key is returned automatically.

So `add` and `drain` can never run at the same time. The wait is tiny.

Details:

- The lock is created once, `threading.Lock()` in `__init__`. After that you use it as `with self.lock:`, **without** parentheses.
- `drain` holds the lock only for the quick part (grabbing the rows). The slow disk writing happens **outside** the lock, so the MQTT thread never waits during a write.
- In standard Python, a plain `list.append` is already protected internally, so this script would probably work without the lock. The lock makes the safety explicit and is the right habit for any shared state.

---

## 11. Can pyarrow read all the Parquet files in a folder?

Yes. Point `pq.read_table` at the folder instead of a file, and it reads every Parquet file below it, subfolders included, as one table.

```python
import pyarrow.parquet as pq

table = pq.read_table("data/bronze")
print(table.num_rows)        # all files together
```

- The `date=...` and `hour=...` folder names come back as extra columns (`date`, `hour`). This works because pyarrow understands that folder style, called Hive partitioning.
- You can read only some columns or only some folders, and the rest is skipped, not loaded and then dropped:

```python
import pyarrow.dataset as ds

dataset = ds.dataset("data/bronze", format="parquet", partitioning="hive")
table = dataset.to_table(
    columns=["veh", "spd", "tst"],
    filter=ds.field("hour") == 17,     # only the hour=17 folder
)
```

- **The safe way:** the atomic write exists so readers only look at finished `*.parquet` files. If the script ever crashed halfway through a write, a leftover `.tmp` file could stay in a folder, and it is not certain how the folder reader treats it. To control which files go in, list them yourself:

```python
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

files = sorted(Path("data/bronze").rglob("*.parquet"))
table = pa.concat_tables([pq.read_table(f) for f in files])
```

This gives the same result, but without the `date` and `hour` columns.

- **Memory:** either way, the table is loaded into RAM. For millions of rows, use `columns=` and `filter=` so you only load what you need.

---

## 12. Does it create one table or a list of tables? What is a ChunkedArray?

**One single table**, with one total row count.

- `pq.read_table("data/bronze")` returns one table directly.
- In the `rglob` version, `[pq.read_table(f) for f in files]` is a list of tables (one per file), and `pa.concat_tables(...)` joins them into one.

Inside the table, each column is stored in **chunks**, roughly one per file. A normal pyarrow **Array** is one column stored as one continuous block of memory. A **ChunkedArray** is one column stored as several such blocks placed one after the other, and it behaves like a single column:

```
file 1, column spd:  [7.2, 0.0, 3.1]      <- chunk 1
file 2, column spd:  [4.5, 8.0]           <- chunk 2

table["spd"]  ->  ChunkedArray with 2 chunks, 5 values, used as one column
```

Why: joining two tables would normally mean copying all the values into one new block. With chunks, pyarrow puts the existing blocks side by side, so combining many files costs almost nothing.

In practice you rarely notice it:

```python
col = table["spd"]
col.num_chunks          # how many blocks
len(col)                # total values, as if it were one
col[3]                  # indexing works across chunks
col.to_pylist()         # all values as a Python list
col.combine_chunks()    # merge into ONE contiguous Array (this does copy)
```

You only need `combine_chunks()` if some function insists on a plain Array.

---

## 13. What happens if the Parquet files do not have the same schema?

Reading many files into one table is like **stacking spreadsheets on top of each other**. It works smoothly only if the columns line up.

Take two tiny files:

```
File A                    File B
veh   stop                veh   stop
629   null                630   1234
629   null                630   1234
```

In File A every `stop` is `null`, so if the file had been written without a declared schema, its type would be "null". In File B the same column has numbers (`int64`). What is the type of `stop` in the stacked table? There are three situations:

**1. Everything matches** (our case). Same columns, same types, so stacking just works. This is why our script always writes with the same `SCHEMA`.

**2. Same column, different types.** The result depends on the tool:
- sometimes it converts one to the other if that is safe (a "null" column can become `int64`, nothing is lost);
- sometimes it stops with an **error**, when the types really clash (text vs number).

**3. A column exists in one file but not in the other** (for example, we add a new column next month). Some tools fill the gap with `null`. Others use only the columns of the first file they see and **quietly ignore** the new one. The second behavior is the dangerous one, because there is no warning that a column disappeared.

The details depend on the tool and the pyarrow version, so check before relying on them. `pa.concat_tables` has options to merge schemas (`promote_options="default"` or `"permissive"` in recent versions, `promote=True` in older ones).

**The safe approach:** tell the reader what the table should look like, instead of letting it guess from whichever file comes first:

```python
dataset = ds.dataset("data/bronze", format="parquet", partitioning="hive", schema=SCHEMA)
```

Now every file is checked against our contract. Differences that can be fixed safely are fixed, and real conflicts produce an error, so you find out instead of getting wrong data silently.

**Does it matter for us now?** No. All our files come from the same script with the same `SCHEMA`. It matters when the schema **changes over time** (*schema evolution*): we add a column and old files lack it. DuckDB, which we use in step 4, handles it with an option: `read_parquet('...', union_by_name=true)` matches columns by name and fills the gaps with nulls.

---

## My notes

_(to fill in)_