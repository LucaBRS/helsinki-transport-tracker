# Step 02: questions and answers

Questions that came up while studying `mqtt_to_parquet.py`.

---

## 1. What is `pa.`, and how does pyarrow work?

At the top we wrote `import pyarrow as pa`. That gives the library a short nickname, so `pa.schema(...)` means `pyarrow.schema(...)`. In the same way, `pq` is `pyarrow.parquet`.

**Apache Arrow** is a standard way to hold a table in memory column by column, instead of row by row. **pyarrow** is the Python library for Arrow, and it also reads and writes Parquet files. Arrow is the columnar table in memory, Parquet is the columnar table on disk.

In our script it plays three roles:

- `pa.schema(...)`, `pa.field(...)`, `pa.int32()` describe the table's structure (the contract);
- `pa.Table.from_pylist(rows, schema=SCHEMA)` builds an Arrow table in memory from our list of dictionaries;
- `pq.write_table(table, path)` writes that table to a Parquet file.

pandas, Polars and DuckDB can all read Arrow tables directly, so the same format moves between tools.

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

## My notes

_(to fill in)_
