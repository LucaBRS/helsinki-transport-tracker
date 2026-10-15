# PyArrow Summary — the basics

## 1. What pandas, PyArrow, PySpark (and `pd`) are

| | What it is | When to use it |
|---|---|---|
| **pandas** | In-memory tables with many convenient analysis methods (`groupby`, `merge`, plots…) | Exploring and analyzing data that fits in RAM on one computer |
| **PyArrow** | Columnar in-memory format + reading/writing files (Parquet). Few analysis methods, precise types, very fast | Defining schemas, converting data, reading and writing Parquet |
| **PySpark** | An engine that distributes work across many machines/cores, with "lazy" execution | Data too big for one computer, data engineering pipelines |

- `pd`, `pa`, `pq` **are not libraries**: they are nicknames chosen at import time.
  ```python
  import pandas as pd
  import pyarrow as pa
  import pyarrow.parquet as pq
  ```
- The three libraries work together: pandas can use PyArrow underneath, and Spark reads the Parquet files written with PyArrow.

## 2. The PyArrow used in the MQTT → Parquet script

The script uses **only PyArrow** (no pandas, no PySpark), in four places:

1. **`pa.schema([...])` and `pa.field(name, type)`**: declare the name and type of each column. This is the "contract".
2. **`pa.Table.from_pylist(rows, schema=SCHEMA)`**: turns the list of dictionaries (one per row) into a **Table**, stored by columns. If a value doesn't match the declared type → error (the script catches it and saves the batch to `failed/`).
3. **`pq.write_table(table, path, compression="snappy")`**: writes the Table as a compressed Parquet file. A written Parquet file is **closed**: you can't append rows → that's why the script uses a buffer and writes a new file each time.
4. **The types**: `pa.int32()`, `pa.float64()`, `pa.string()`, `pa.timestamp("ms", tz="UTC")` (`"ms"`/`"us"` = precision, `tz="UTC"` = time zone).

Flow: MQTT message → `build_row` (dictionary) → `Buffer` (list) → `write_batch` → `pa.Table` → `.parquet` file in `date=…/hour=…`.

## 3. Creating a table: two ways

**Dictionary of columns — `from_pydict`** (key = column, value = list of values):
```python
table = pa.Table.from_pydict({
    "A": [3, 7, 1, 9],
    "B": [2.5, 0.8, 4.1, 3.3],
    "C": ["red", "blue", "green", "yellow"],
})
```

**List of rows — `from_pylist`** (each dictionary = one row; this is the script's case, because MQTT messages arrive one at a time):
```python
table = pa.Table.from_pylist([
    {"A": 3, "B": 2.5, "C": "red"},
    {"A": 7, "B": 0.8, "C": "blue"},
    {"A": 1, "B": 4.1, "C": "green"},
    {"A": 9, "B": 3.3, "C": "yellow"},
])
```

The two tables are identical, and in both cases the data is stored **by column**.

## 4. The schema is optional (but worth it)

- The two ways create the **table**; a schema can be added to **both** with `schema=SCHEMA`.
- **With a schema**: you decide the types.
- **Without a schema**: PyArrow guesses them from the data (e.g. `int64`, `double`, `string`).
- Why it matters in the script: without a schema, if a column (e.g. `stop`) is all `null` in one batch, it gets type `null`; in the next batch it becomes `int64` → Parquet files with different types for the same column, hard to read together.

## 5. Reading values

```python
table["A"]                # the whole column A
table["A"][2]             # third value -> a PyArrow object (Scalar)
table["A"][2].as_py()     # the same value as a plain Python number -> 1
table.to_pylist()[1]      # second row as a dictionary
table.column_names        # ['A', 'B', 'C']
table.num_rows            # 4
```

Watch out for `.as_py()`: without it you get a PyArrow object, not a Python number.

## 6. Modifying: the table is immutable

A PyArrow table **cannot be changed**. Every "change" creates a **new** table; the old one stays the same. You don't need to start again from the dictionaries, and untouched columns are shared, not copied.

```python
import pyarrow.compute as pc

new = table.set_column(0, "A", pc.multiply(table["A"], 2))   # replace a column
new = table.append_column("D", pa.array([1, 2, 3, 4]))       # add a column
new = table.drop_columns(["C"])                               # remove a column
new = table.filter(pc.greater(table["A"], 2))                 # keep only some rows
```

**Changing a single value**: there is no direct command. If you need it, you're using the wrong tool → switch to pandas:
```python
df = table.to_pandas()
df.loc[2, "A"] = 99
```

## Rule of thumb

- **PyArrow** → moving and storing data (schema, Parquet).
- **pandas** → working on data (exploring, changing, analyzing).
- **PySpark** → when the data is too big for one computer.
