# Step 03: Redpanda / Kafka, a log that doesn't lose data

**Goal**: put a durable "record" between the HSL broker and the files, so that if our script stops the messages are not lost.

```
HSL broker ──► producer ──► Redpanda (log) ──► consumer ──► Parquet
```

## Theory

### L1. Why put a log in the middle?

Today's pipeline (step 02) is:

```
HSL broker ──► our script (receives AND writes) ──► files
```

One program does two jobs, and that has three consequences:

- if the script is off, the messages are lost, because the HSL broker keeps no history;
- if it crashes, the buffer in memory is lost;
- if we want to change how we write files, we have to stop receiving too.

The idea: split it in two and put a **durable log** between them.

```
HSL broker ──► [producer] ──► LOG (Redpanda) ──► [consumer] ──► files
                tiny, always on                   can stop, restart, be rewritten
```

- The **producer** only receives from MQTT and appends to the log. It is small, so it rarely needs to change.
- The **consumer** reads from the log and writes Parquet. It can stop, crash or be rewritten, and the messages wait in the log.

**What a log is.** An ordered sequence of messages written to disk, always at the end:

```
position:  0   1   2   3   4   5   6 ...
           m   m   m   m   m   m   m  ──► new messages are added here
```

- Messages are **kept** for a set time (for example 7 days). Reading does **not** delete them.
- Each reader remembers its own position, so it can resume after a stop and can also go back and read from the start again.

An analogy: the MQTT broker is a live radio broadcast (if you are not listening, it is gone). A log is a recording: you can listen later, pick up where you stopped, or replay it from the beginning.

**Redpanda vs Kafka.** Kafka is the original system that defined this idea. Redpanda speaks the same protocol, so the same client libraries and tools work, but it is simpler to run locally (one program, no extra components). What you learn about Kafka applies to Redpanda.

**Honest limits.** A log does not remove every risk:

- if the *producer* is down, we still lose the messages of that period (but it is tiny and simple, so it fails far less often);
- if HSL sends nothing, there is nothing to save;
- a log is not a database: you do not query it, you read it in order.

### L2. The vocabulary

| Word | Meaning | In our project |
|---|---|---|
| **Topic** | a named log, one kind of message | something like `hsl.tram.positions` |
| **Partition** | a topic can be split into independent logs, each one ordered; used to read in parallel when volume is big | very few needed (about 55 messages per second), maybe one |
| **Key** | an optional value attached to a message; the same key always goes to the same partition | the vehicle number |
| **Offset** | the position number of a message inside a partition: 0, 1, 2, 3... | "message 4,512 of partition 0" |
| **Consumer** | a program that reads from the log | the Parquet writer |
| **Consumer group** | a name shared by consumers that work together; the log remembers the group's last processed offset | `parquet-writer` |

Why the key is the vehicle number: **order is guaranteed only inside one partition**. With the vehicle as the key, each tram's events stay in the order they were sent. Order between different trams does not matter to us.

Different groups keep their own positions and read the same messages independently. Later we could add a second group for a live dashboard, and neither would disturb the other.

### L3. The bookmark: committing

Think of the log as a very long book, one line per message, numbered by offset. The consumer reads from top to bottom and keeps a **bookmark**. Putting in the bookmark is called **committing** the offset.

- The bookmark is stored **by the log**, not by the consumer.
- So if the consumer crashes, the bookmark is still there, and on restart the consumer opens the book at the bookmark.

### L4. Delivery guarantees

For every batch, the consumer must do two things: **A.** write the Parquet file, **B.** move the bookmark. It cannot do both at the same instant, so the order matters if a crash falls between them. Take lines 4000-4499 as the batch.

**Order 1: bookmark first, then write**

```
B. bookmark moves to 4500
   --- crash ---
A. (the file was never written)
```

On restart the consumer starts at 4500. Lines 4000-4499 are **never written, and nobody notices**. Data is lost. This is called *at-most-once*.

**Order 2: write first, then bookmark**

```
A. file with lines 4000-4499 is written
   --- crash ---
B. (the bookmark never moved, still at 4000)
```

On restart the consumer reads 4000-4499 again and writes them a **second time**. Nothing is lost, but those lines now exist twice. This is called *at-least-once*.

**We choose Order 2.** Lost data cannot be recovered; duplicate data can be removed later in silver with the key `(oper, veh, tsi)`. So we accept duplicates and never accept loss.

(A third option, *exactly-once*, exists in Kafka for specific setups such as Kafka-to-Kafka with transactions. Between Kafka and an external system like a folder of files it is hard to guarantee. In practice teams use at-least-once plus deduplication.)

Duplicates can now come from three places:

| Source | Cause |
|---|---|
| HSL / MQTT | each event arrives 4 times (observed, cause unknown) |
| Producer | it sends a message, the confirmation is lost, it retries |
| Consumer | crash between writing and committing, then re-reading |

All three are removed by the same rule in silver, so bronze can stay simple and keep everything.

The key point: the log does not make the pipeline reliable by itself. **Reliability comes from the order of your own steps**: write first, commit after, and make duplicates harmless.

### L5. Lag and retention

**Lag** is how far behind the consumer is: the number of messages between its bookmark and the end of the log.

```
last message in the log:   5000
consumer bookmark:         4500
lag = 500 messages  (about 9 seconds of data at 55 messages per second)
```

Do not judge the lag by one reading. **Look at the trend.** Our producer delivers about 3,300 messages per minute.

```
Healthy (steady, wobbles around a small number):   300, 280, 320, 290
Problem (keeps rising):                            300, 3,600, 6,900, 10,200
```

In the second case the lag grows by 3,300 per minute, exactly the arrival rate, so the consumer is processing nothing (crashed or stuck). If it grew by less than 3,300 per minute (for example 1,600), the consumer would be running but too slow. A lag of 5,000 that stays at 5,000 is a consumer that is behind but coping; a lag of 200 that grows every minute is the dangerous one.

The consumer **does not erase** messages: the lag drops because the bookmark moves forward.

**Retention.** The log does not keep messages forever: it removes old ones after a set time or size, for example 7 days. If the consumer is stopped so long that its bookmark points to a message already removed, those messages are lost for good. So retention must be longer than the longest outage we want to survive.

### L6. Broken messages: the dead letter queue (DLQ)

In `mqtt_to_parquet.py`, a message that cannot be processed (broken JSON, no `tst`) is counted as "bad" and **thrown away**. With a log that causes two problems:

1. **We lose the evidence**: we never learn what was wrong, and we cannot reprocess after fixing the cause.
2. **It can block everything**: if a broken message makes the consumer crash, on restart it reads the same message, crashes again, and loops forever, with all the good messages stuck behind it (a "poison message").

The fix is a **dead letter queue**: a separate topic, something like `hsl.tram.positions.dead`. When the consumer finds a message it cannot process, it:

- writes it to the DLQ together with the reason (for example "no `tst`");
- moves its bookmark forward as if the message were handled;
- continues with the next good message.

Benefits: a trace of what went wrong, the main flow is never blocked, and the messages can be replayed once the cause is fixed. An analogy: a post office puts a letter with an unreadable address in a "problem letters" box, instead of throwing it away or letting it block the conveyor belt.

### The whole of step 3 in one picture

```
HSL ──MQTT──► producer ──► TOPIC (log, kept 7 days, key = vehicle)
                              │
                              ▼
                  consumer, group "parquet-writer"
                    1. read a batch (offsets 4000-4499)
                    2. write Parquet (atomic)
                    3. commit offset          ← after writing = at-least-once
                    bad message ──► dead-letter topic
                              │
                  lag = newest offset - committed offset  (watched over time)
```

## Exercise

**Prerequisite:** Docker installed (check with `docker --version`). *Status: not checked yet.*

Planned in three small pieces, each tested before the next (to be refined when we get to the code):

1. `docker-compose.yml` that starts Redpanda and its web console.
2. Producer: MQTT in, Redpanda topic out, with the vehicle number as the key.
3. Consumer: the batching, schema, partitioning and atomic write from step 02, reading from the topic, committing after writing, and sending bad messages to the DLQ.

Things to verify once it runs:

- Stop the consumer for a minute and restart it: is anything missing in bronze? (The lag should grow while it is stopped, then fall.)
- Stop the producer for a minute: what is missing, and why can't the log help?
- In the console, find the topic, its partition and the consumer group's offset.
- Kill the consumer right after it writes a file: do the same messages appear twice?

## Status

Theory done. Code not started.

## My notes

_(to fill in)_