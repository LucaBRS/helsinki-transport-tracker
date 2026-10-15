# Study path: streaming data engineering

Each folder is a **step**. Go in order, one at a time.
Inside every step you always find the same things:

| What | Purpose |
|---|---|
| `step-NN.md` | Short **theory**, then an **exercise**, then a space for your notes |
| code | the step's script (small, commented) |
| `data/` or `samples/` | data produced by the scripts (ignored by git) |

Every step ends with a working commit.

## The steps

| # | Folder | Question it answers | Status |
|---|---|---|---|
| 01 | [`01-mqtt-connessione`](01-mqtt-connessione/step-01.md) | How does the data arrive? | done |
| 02 | [`02-parquet-microbatch`](02-parquet-microbatch/step-02.md) | How do I store it in a format that is useful for analysis? | done (exercises 4-7 deferred) |
| 03 | [`03-redpanda-kafka`](03-redpanda-kafka/step-03.md) | How do I avoid losing data if something stops? | to do |
| 04 | [`04-dbt-silver-gold`](04-dbt-silver-gold/step-04.md) | How do I remove duplicates and reason about time? | to do |
| 05 | [`05-orchestrazione`](05-orchestrazione/step-05.md) | Who runs and monitors everything? | to do |
| 06 | [`06-fabric`](06-fabric/step-06.md) | What does the platform manage for me? | to do |

## Structure of every step file

1. **Goal**: one sentence.
2. **Theory**: a few concepts, each with an example on real HSL data.
3. **Exercise**: small and verifiable.
4. **My notes**: what I understood, what surprised me, open questions.

## Data used

Real-time tram positions in Helsinki (HSL), via MQTT. For now two lines:
route `1009` (line 9) and route `1006` (line 6).
Documentation: https://digitransit.fi/en/developers/apis/5-realtime-api/vehicle-positions/high-frequency-positioning/
