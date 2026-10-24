# Step 03: questions and answers

My questions during step 03 (Redpanda / Kafka) and the answers, in the order they came up.
The diagram of the three threads is in `threads.png` (same folder).

---

## Part A: Theory

### Q1. What does "lag" mean, and how do I read it?

**Lag** = how far the consumer is behind the end of the log: the number of messages between its bookmark (committed offset) and the newest message.

```
newest message in the log:  5000
consumer bookmark:          4500
lag = 500 messages  (about 9 s of data at 55 messages per second)
```

One reading says little. **Look at the trend.** Our producer delivers about 3,300 messages per minute.

```
Healthy (steady, small wobble):   300, 280, 320, 290
Problem (keeps rising):           300, 3,600, 6,900, 10,200
```

- In the second case the lag grows by 3,300 per minute, exactly the arrival rate: the consumer processes nothing (crashed or stuck).
- If it grew by less (say 1,600 per minute), the consumer would be running but too slow.
- A lag of 5,000 that stays at 5,000 is a consumer behind but coping. A lag of 200 that grows every minute is the dangerous one.
- The consumer does **not** erase messages: the lag drops because the bookmark moves forward.

**Retention.** The log removes old messages after a set time (for example 7 days). If the consumer is stopped longer than that, its bookmark points to messages that no longer exist, and they are lost for good. So retention must be longer than the longest outage we want to survive.

### Q2. Why keep broken messages in a dead letter queue (DLQ)?

In step 02 a broken message was counted as "bad" and thrown away. With a log that has two problems:

1. We lose the evidence: we never learn what was wrong and cannot reprocess after fixing the cause.
2. It can block everything: a message that crashes the consumer is read again after the restart, crashes it again, and so on, with all good messages stuck behind it (a "poison message").

The DLQ is a separate topic (for example `hsl.tram.positions.dead`). The consumer writes the bad message there with the reason (for example "no `tst`"), moves its bookmark forward as if the message were handled, and goes on. We keep a trace, the main flow is never blocked, and the messages can be replayed once the cause is fixed. Analogy: the "problem letters" box in a post office.

### Q3. A consumer reads a message: is the message erased?

No. In a log, reading deletes nothing. What moves is the consumer's **bookmark** (the committed offset). Messages disappear only through retention (time or size). This is the main difference from a queue, and the reason two different consumer groups can read the same messages independently.

---

## Part B: Docker and Redpanda

### Q4. What does `docker exec` do?

It runs a command **inside a container that is already running**.

```bash
docker exec redpanda-0 rpk topic create hsl.tram.positions
```

reads as: "in the container named `redpanda-0`, run the program `rpk` with these arguments". `redpanda-0` is the container name we gave in `docker-compose.yml`. The `rpk` program exists inside the container (it is Redpanda's command-line tool), so we use `exec` to reach it without installing anything on the PC.

### Q5. What does `rpk topic create hsl.tram.positions` do?

`rpk` is the Redpanda command-line tool. This command asks the broker to create a topic called `hsl.tram.positions`: a named log, empty at the start. Producers write to it and consumers read from it. The broker does not create topics on its own in our setup, which is why `producer.py` checks at startup that the topic exists and tells us the exact command if not.

Useful options:

```bash
docker exec redpanda-0 rpk topic create hsl.tram.positions -p 1 -c retention.ms=604800000
#   -p 1                      one partition (enough for ~55 messages per second)
#   -c retention.ms=604800000 keep messages 7 days (7 x 24 x 3600 x 1000 ms)
docker exec redpanda-0 rpk topic list
docker exec redpanda-0 rpk topic delete hsl.tram.positions
```

### Q6. What do the produce and consume test commands do?

```bash
echo "hello" | docker exec -i redpanda-0 rpk topic produce hsl.tram.positions
docker exec redpanda-0 rpk topic consume hsl.tram.positions -n 3
```

- `produce` reads lines from the keyboard (here from the pipe) and appends each line as a message at the end of the topic. `-i` keeps the input open so the pipe can reach the container.
- `consume` reads messages from the topic and prints them. `-n 3` stops after three messages. The output shows, for each one, the **offset** (0, 1, 2), the value, the timestamp and the partition.

### Q7. Why does running `consume` again return the same messages?

Because the log is not a queue. Reading does not delete anything, and `rpk topic consume` with no consumer group starts from the beginning each time. The messages stay until retention removes them. Offsets 0, 1, 2 are always the same three messages.

(A real consumer with a **group** remembers its bookmark in the log, so on the next run it continues where it stopped.)

### Q8. What is Redpanda and how does it work? How is it set up?

Redpanda is a **broker** (a server) that stores logs. Programs connect to it over the network, using the Kafka protocol, to append messages (producers) and to read them (consumers).

```
producer ──► [ Redpanda: topic "hsl.tram.positions" (log on disk) ] ──► consumer
```

- It receives messages and writes them at the end of the log on disk.
- It keeps them for the retention period.
- It remembers each consumer group's bookmark.
- It answers "give me messages from offset N".

Set up in `docker-compose.yml`: one Redpanda container in dev mode (a single broker, one CPU core) plus the Console, a web page at `http://localhost:8080` where we see topics, messages and consumer groups. Two listeners:

| Address | Used by |
|---|---|
| `redpanda-0:9092` (internal) | programs inside Docker, such as the console |
| `localhost:19092` (external) | scripts on our PC, such as `producer.py` |

Production would use several brokers with replication. Ours is one broker, enough to learn.

### Q9. Is my picture right? Is a partition a broker?

My picture: the producer sends messages to Redpanda. Yes, correct.

A partition is **not** a broker:
- **Broker** = a server (a Redpanda process).
- **Topic** = a named group of messages inside the broker.
- **Partition** = one ordered log inside a topic. A topic can have one or many.

```
broker
 └─ topic "hsl.tram.positions"
     ├─ partition 0   (ordered log: 0, 1, 2, ...)
     └─ partition 1   (another ordered log)
```

Order is guaranteed **inside** one partition, not between partitions. That is why the message key is the vehicle: the same key always goes to the same partition, so each tram's events keep their order.

### Q10. I closed and reopened Docker and the messages are still there. Why?

Because of the **volume** `redpanda-0` in `docker-compose.yml`:

```yaml
volumes:
  redpanda-0:/var/lib/redpanda/data
```

The container is disposable, but the volume is a storage area managed by Docker, outside the container, where Redpanda writes its log. When the container restarts, it mounts the same volume and finds the log. To really start from zero you must delete the volume (`docker compose down -v`) or the topic (`rpk topic delete`).

---

## Part C: The producer

### Q11. Why do we need Kafka (and confluent-kafka)? Do I install Kafka?

Two different things:

- **Kafka** is the idea and the protocol: a durable log with topics, offsets and consumer groups. Redpanda is a system that speaks the same protocol, so we do **not** install Kafka itself.
- **confluent-kafka** is a Python library. It is the "telephone" our Python code uses to talk to Redpanda over the Kafka protocol. Without it, our script has no way to send messages to the broker. Install with `uv add confluent-kafka`.

Same pattern as MQTT: `paho-mqtt` lets Python talk to the HSL broker, `confluent-kafka` lets Python talk to Redpanda. Bonus: Microsoft Fabric Eventstream also accepts Kafka clients, so this producer code carries over to the Fabric step.

### Q12. What is the envelope, and why the key?

The producer stays "dumb": it does not parse the payload. It wraps the original text in a small JSON:

```json
{"mqtt_topic": "...", "received_at": "2026-10-21T10:15:03+00:00", "payload": "<original text>"}
```

- `mqtt_topic` is kept because it contains operator, vehicle and route.
- `received_at` is when **we** received it (the tram's own time is `tst` inside the payload).
- `payload` is the untouched original, so the consumer decides what to do with it, and bad data never stops the producer.

The Kafka **key** is `operator/vehicle` (for example `0040/00629`), taken from the MQTT topic. Same key = same partition = order per vehicle.

### Q13. What does "asynchronous" mean?

**Synchronous**: you call a function, it does all the work, and returns when the work is finished.
**Asynchronous**: you call a function, it only *starts* or *queues* the work and returns at once. Someone else, in the background, finishes it later.

Analogy: posting a letter. You drop it in the box and walk away (asynchronous). You do not wait next to the box until the letter reaches the other city (synchronous).

In our code, `producer.produce(...)` is asynchronous: it puts the message in a local queue and returns. A background thread sends it to Redpanda. It is **not** Python's `asyncio`; here "async" simply means "the call returns before the work is done, and a thread does the work".

The opposite, synchronous, would be calling `producer.flush()` after every `produce()`: each call would wait for Redpanda, and at 55 messages per second the MQTT thread would be stuck waiting instead of listening.

### Q14. Why `flush()` after Ctrl+C?

Because when messages are in the local queue, they are only in memory. If the script ends right after Ctrl+C, whatever is still in the queue is lost. `producer.flush(10)` waits (up to 10 seconds) until the queue is empty and returns how many messages are still unsent, so we can print a warning if there are any.

### Q15. `poll()` and `delivery_report`: who calls what?

`delivery_report(error, kafka_message)` is a callback we wrote and passed with `on_delivery=`. We never call it. The library calls it **when we call `producer.poll()`**, to tell us whether Redpanda confirmed that message or gave up. So:

- the sender thread sends the batch and gets the confirmations;
- the results wait in the library;
- the main thread's `poll(0.5)` runs `delivery_report` for each result, in the main thread.

`poll(0.5)` waits up to 0.5 s if there is nothing to do, so the `while True` loop does not burn the CPU.

---

## Part D: Where is the asynchronous part? (threads)

Look at `threads.png`.

### Q16. The code looks synchronous. Where is the asynchronous part?

Read top to bottom, the file does look synchronous. The asynchronous part comes from the fact that **three threads** run at the same time:

| Thread | Created by | What it does |
|---|---|---|
| Main thread | the script itself | runs `main()`: check, `loop_start()`, then `while True: poll()` and prints |
| MQTT thread | `client.loop_start()` | receives HSL messages and calls `on_connect` / `on_message` |
| Sender thread | `Producer(...)` | takes messages from the local queue and sends them to Redpanda |

The async boundary is `producer.produce(...)` inside `on_message`: it hands the message to the sender thread through the queue and returns at once.

### Q17. What is paho?

`paho-mqtt` is the Python library that speaks the MQTT protocol (`import paho.mqtt.client as mqtt`). It connects to `mqtt.hsl.fi`, subscribes, receives messages and calls our `on_connect` and `on_message`. It is the MQTT counterpart of `confluent-kafka`, which talks to Redpanda. (Paho is the name of the Eclipse project that made it.)

### Q18. How do `client.connect` and `client.loop_start` start MQTT?

- `client.connect(...)` only opens the network connection to `mqtt.hsl.fi`. It returns when the connection is open. No message is handled yet.
- `client.loop_start()` is what starts MQTT. Inside paho it **creates a new thread** and returns at once. That thread reads from the socket and calls `on_connect` and `on_message`.
- We never call `on_message` ourselves. The paho thread calls it, about 55 times per second.

So the guess "another thread somewhere runs the loop" is right.

### Q19. Is the `while True` loop the async part?

No. It is the main thread's own job and is plain synchronous code. `producer.poll(0.5)` collects the delivery confirmations and runs `delivery_report`; the `if` prints the counters every 10 seconds. The loop also keeps the program alive: if `main()` ended, the whole program would stop, including the other threads.

### Q20. Does `Producer({...})` start one of the async threads? Is the queue just empty at the start?

Yes. `Producer(...)` creates, hidden inside confluent-kafka, the sender thread, together with an empty queue. The queue is only a place in memory (not a thread). `produce()` adds messages to it, and the sender thread empties it. When `Producer(...)` finishes, an idle sender thread is already waiting with an empty queue.

### Q21. Does the MQTT thread send the data to Redpanda?

No. The MQTT thread only listens and, in `on_message`, calls `producer.produce(...)`, which puts the message in the queue. The sender thread (created by `Producer(...)`, not by `loop_start()`) sends it to Redpanda. The names of the objects matter:

- `producer = Producer(...)` is the Redpanda side and starts the **sender** thread.
- `client.loop_start()` is the MQTT side (the paho `client`) and starts the **MQTT** thread.

### Q22. The whole chain, and where the threads exchange information

```
HSL broker ──pushes──► MQTT thread: on_message
                          │  builds envelope + key
                          ▼
                    producer.produce()      ← async starts here, returns at once
                          │
                          ▼
                  local queue (memory)
                          │  the sender takes it later
                          ▼
                    Sender thread ──batch──► Redpanda
                          │◄──────confirms───┘
                          │  result stored in the library
                          ▼
         Main thread: producer.poll() ──► delivery_report
```

The threads meet in only two places:

1. The **queue** (MQTT thread to sender thread).
2. The **stored results** read by `poll()` (sender thread to main thread).

Counters: `received` and `dropped` are changed only by the MQTT thread; `delivered` and `failed` only by the main thread (inside `poll`). No two threads change the same number, so no lock is needed.

---

## Open question (to answer)

Which single line would make `produce()` synchronous, and why do we not want that at 55 messages per second? (Hint in Q13.)
