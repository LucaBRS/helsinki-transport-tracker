"""
Step 02 - From MQTT messages to Parquet files (micro-batch).

Subscribes to two HSL tram routes (1009 = line 9, 1006 = line 6), keeps the
messages in a memory buffer, and writes the buffer to a Parquet file whenever
it holds BATCH_MAX_MESSAGES messages OR BATCH_MAX_SECONDS seconds have passed.

Concepts from the theory (step-02.md), marked in the code as [C1]..[C4]:
  [C1] columnar format      -> Parquet, one column per field
  [C2] a Parquet file is closed once written -> buffer in memory, write blocks
  [C3] schema is a contract -> explicit schema, nullable columns
  [C4] folders matter       -> date=YYYY-MM-DD/hour=HH, from the EVENT time (tst)
Extras: atomic write (.tmp then rename), clean shutdown, time-based flush,
raw JSON kept in a column.

Install:  pip install paho-mqtt pyarrow
Run:      python mqtt_to_parquet.py        (Ctrl+C to stop: the buffer is flushed)
"""

import json
import os
import ssl
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
import pyarrow as pa
import pyarrow.parquet as pq


# ----------------------------------------------------------------------------
# 1. Configuration
# ----------------------------------------------------------------------------
BROKER = "mqtt.hsl.fi"

# False = port 1883, no encryption (avoids the certificate error on your PC).
# True  = port 8883, encrypted.
USE_TLS = False

if USE_TLS:
    PORT = 8883
else:
    PORT = 1883

# After "tram" the topic levels are: operator / vehicle / ROUTE / direction / ...
# so "+/+/1009" = any operator, any vehicle, route 1009. (Route is not the
# number passengers see: route 1009 is line 9, 1006 is line 6.)
TOPICS = [
    "/hfp/v2/journey/ongoing/vp/tram/+/+/1009/#",
    "/hfp/v2/journey/ongoing/vp/tram/+/+/1006/#",
]

# [C2] When do we write a file? Whichever comes first.
BATCH_MAX_MESSAGES = 500
BATCH_MAX_SECONDS = 60

# Data lives next to this script (add "data/" to .gitignore).
SCRIPT_FOLDER = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_FOLDER / "data" / "bronze"
FAILED_DIR = SCRIPT_FOLDER / "data" / "failed"


# ----------------------------------------------------------------------------
# 2. [C3] The schema is a contract.
# Every column is nullable (the default): HSL sends null for `stop` while the
# tram is moving, and `drst`, `lat`, `long` can be null too.
# ----------------------------------------------------------------------------
SCHEMA = pa.schema(
    [
        pa.field("oper", pa.int32()),
        pa.field("veh", pa.int32()),
        pa.field("tsi", pa.int64()),                         # event time, whole seconds (dedup key)
        pa.field("tst", pa.timestamp("ms", tz="UTC")),       # event time, set by the tram
        pa.field("received_at", pa.timestamp("us", tz="UTC")),  # arrival time, set by us
        pa.field("route", pa.string()),
        pa.field("desi", pa.string()),                       # line number shown to passengers
        pa.field("dir", pa.string()),
        pa.field("line", pa.int32()),
        pa.field("jrn", pa.int32()),
        pa.field("start", pa.string()),                      # local Helsinki time, NOT UTC
        pa.field("oday", pa.string()),
        pa.field("spd", pa.float64()),
        pa.field("hdg", pa.int32()),
        pa.field("lat", pa.float64()),
        pa.field("long", pa.float64()),
        pa.field("acc", pa.float64()),
        pa.field("dl", pa.int32()),
        pa.field("odo", pa.int32()),
        pa.field("drst", pa.int32()),
        pa.field("stop", pa.int64()),
        pa.field("loc", pa.string()),
        pa.field("occu", pa.int32()),
        pa.field("topic", pa.string()),
        # Extra: the original message, so a field HSL adds tomorrow is not lost.
        pa.field("raw_json", pa.string()),
    ]
)

# The fields we copy as they are from the message (inside the "VP" key).
# tst, received_at, topic and raw_json are added separately.
PAYLOAD_FIELDS = [
    "oper", "veh", "tsi", "route", "desi", "dir", "line", "jrn", "start",
    "oday", "spd", "hdg", "lat", "long", "acc", "dl", "odo", "drst", "stop",
    "loc", "occu",
]


# ----------------------------------------------------------------------------
# 3. Parsing: one MQTT message -> creating one row (a dictionary).
# Returns None if the message is unusable.
# ----------------------------------------------------------------------------
def parse_iso_utc(value):
    # HSL writes times like "2026-10-07T13:51:00.255Z". The final "Z" means UTC;
    # Python wants "+00:00" instead, so we replace it before converting.
    fixed_value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(fixed_value)


def build_row(topic, payload_bytes, received_at):
    # Any of these steps can fail on a broken message: we catch the errors and
    # return None, so one bad message never stops the script.
    try: #m if it fail we skip the row
        raw = payload_bytes.decode("utf-8")        # bytes -> text
        message = json.loads(raw)                  # text -> dictionary
        vp = message["VP"]                         # the data is under the key "VP"
        tst = parse_iso_utc(vp["tst"])             # event time as a datetime
        if vp.get("veh") is None:
            return None
        if vp.get("oper") is None:
            return None
    except (ValueError, KeyError, TypeError, UnicodeDecodeError):
        return None

    row = {} #m building the actual row in a dictionary format
    for name in PAYLOAD_FIELDS:
        row[name] = vp.get(name)                   # .get gives None if the field is missing
    row["tst"] = tst
    row["received_at"] = received_at
    row["topic"] = topic
    row["raw_json"] = raw
    return row


# ----------------------------------------------------------------------------
# 4. [C4] Partitioning by EVENT time: a late message lands in the right hour.
# ----------------------------------------------------------------------------
def partition_dir(base, tst):
    date_text = tst.strftime("%Y-%m-%d")           # e.g. "2026-10-07"
    hour_text = tst.strftime("%H")                 # e.g. "16"
    return base / ("date=" + date_text) / ("hour=" + hour_text) #m creating the folder name/structure


def group_by_partition(rows, base):
    # Returns a dictionary: folder -> list of the rows that belong in it.
    groups = {}
    for row in rows:
        folder = partition_dir(base, row["tst"])
        if folder not in groups:
            groups[folder] = []
        groups[folder].append(row)
    return groups


# ----------------------------------------------------------------------------
# 5. [C1][C2] Writing. One call = one new, closed Parquet file.
# ----------------------------------------------------------------------------
def write_parquet_file(rows, path):
    # List of dictionaries -> table with our declared schema -> Parquet file.
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, path, compression="snappy")


def save_failed(rows, name):
    # Fallback: if Parquet writing fails, keep the original messages as JSON Lines.
    FAILED_DIR.mkdir(parents=True, exist_ok=True)
    failed_path = FAILED_DIR / (name + ".jsonl")
    # "with" closes the file automatically when the block ends.
    with open(failed_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(row["raw_json"] + "\n")


def write_batch(rows, base=OUTPUT_DIR, writer=write_parquet_file):
    """Write rows as one file per (date, hour). Returns the final file paths.

    Atomic write: the file is written as <name>.tmp and renamed at the end.
    Readers that look for *.parquet never see a half-written file.
    (`writer` can be replaced in tests; by default it writes real Parquet.)
    """
    written = []
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    groups = group_by_partition(rows, base) #m the game of dictionary

    for folder in groups:
        group = groups[folder]
        folder.mkdir(parents=True, exist_ok=True)

        # Unique file name: time of writing + 8 random characters.
        random_part = uuid.uuid4().hex[:8]
        name = "batch_" + stamp + "_" + random_part + ".parquet"
        final_path = folder / name
        tmp_path = folder / (name + ".tmp")

        try:
            writer(group, tmp_path)                # 1. write under the temporary name
            os.replace(tmp_path, final_path)       # 2. rename: instant and atomic
            written.append(final_path)
        except Exception as error:                 # never lose a batch silently
            print("!! write failed (" + repr(error) + "); saving batch to", FAILED_DIR)
            if tmp_path.exists():
                tmp_path.unlink()                  # remove the half-written file
            save_failed(group, name)

    return written


# ----------------------------------------------------------------------------
# 6. [C2] The buffer. on_message only adds; the main loop is the only writer.
# ----------------------------------------------------------------------------
class Buffer:
    def __init__(self, max_messages, max_seconds):
        self.max_messages = max_messages
        self.max_seconds = max_seconds
        self.rows = []
        # The MQTT thread adds rows while the main thread empties the buffer.
        # The lock makes sure only one of them touches the list at a time.
        self.lock = threading.Lock()
        # monotonic = a clock that only goes forward (safe for measuring durations).
        self.last_flush = time.monotonic()

    def add(self, row):
        with self.lock:
            self.rows.append(row)

    def is_due(self, now=None):
        # Is it time to write? Yes if there are rows AND (too many OR too old).
        if now is None:
            now = time.monotonic()

        with self.lock:
            if len(self.rows) == 0:
                return False

            too_many = len(self.rows) >= self.max_messages
            seconds_since_flush = now - self.last_flush
            too_old = seconds_since_flush >= self.max_seconds

            if too_many or too_old:
                return True
            else:
                return False

    def drain(self):
        # Take everything out of the buffer and restart the timer.
        with self.lock:
            rows = self.rows
            self.rows = []
            self.last_flush = time.monotonic()
        return rows


# ----------------------------------------------------------------------------
# 7. MQTT callbacks and the flush
# ----------------------------------------------------------------------------
buffer = Buffer(BATCH_MAX_MESSAGES, BATCH_MAX_SECONDS)
stats = {"received": 0, "bad": 0, "written": 0, "files": 0}


def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected to", BROKER, "- reason:", reason_code)
    # Subscribe INSIDE on_connect: after an automatic reconnect we subscribe again.
    for topic in TOPICS:
        client.subscribe(topic)
        print("Subscribed to", topic)


def on_message(client, userdata, msg):
    # Called by paho for every message. It only parses and adds to the buffer:
    # the actual writing happens in the main loop.
    stats["received"] = stats["received"] + 1

    received_at = datetime.now(timezone.utc)
    row = build_row(msg.topic, msg.payload, received_at)

    if row is None:
        stats["bad"] = stats["bad"] + 1
        return

    buffer.add(row)


def flush():
    rows = buffer.drain()
    if len(rows) == 0:
        return

    paths = write_batch(rows)
    stats["written"] = stats["written"] + len(rows)
    stats["files"] = stats["files"] + len(paths)

    time_text = datetime.now().strftime("%H:%M:%S")
    print(
        "[" + time_text + "] wrote", len(rows), "messages in", len(paths), "file(s)",
        "| received =", stats["received"], "bad =", stats["bad"],
    )


# ----------------------------------------------------------------------------
# 8. Main
# ----------------------------------------------------------------------------
def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if USE_TLS:
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT)
    # loop_start runs the MQTT loop in a background thread, so THIS thread is
    # free to check the buffer and write files.
    client.loop_start()
    print("Writing to", OUTPUT_DIR, " (Ctrl+C to stop)\n")

    try:
        while True:
            time.sleep(0.5)
            if buffer.is_due():
                flush()
    except KeyboardInterrupt:
        print("\nStopping...")
    finally: #m when u press ctrl+c, the finally is executed
        client.loop_stop()
        client.disconnect()
        flush()                                    # do not lose what is still in the buffer
        print("Done:", stats["written"], "messages in", stats["files"], "file(s),",
              stats["bad"], "unusable.")


# Run main() only when the file is started directly (python mqtt_to_parquet.py),
# not when another script imports it.
if __name__ == "__main__":
    main()
