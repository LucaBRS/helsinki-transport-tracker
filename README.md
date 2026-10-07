# Helsinki Transport Tracker

> Real-time IoT streaming pipeline on Helsinki public transport (MQTT → Redpanda → dbt → Microsoft Fabric)

A specialization project focused on **streaming and IoT**: real-time data from Helsinki's public transport (HSL), processed by an end-to-end pipeline built first **locally** and then migrated to **Microsoft Fabric**, with a strong focus on **orchestration**.

> This file is the starting point for working with Claude Code: it describes goals, architecture, decisions and project phases. Keep it updated as phases are completed.

---

## Goals

1. Learn to handle a real **push-based IoT stream** (MQTT), not API polling.
2. Build a complete local streaming pipeline: ingestion → queue → storage → transformations → dashboard.
3. Learn how to **orchestrate** a hybrid streaming + batch system.
4. Migrate everything to **Microsoft Fabric** (Real-Time Intelligence + Warehouse + dbt), understanding what the platform manages for us.
5. Fast visualization of the results (Streamlit/Grafana for real-time, Power BI on the gold layer).

---

## Data Source: HSL High-Frequency Positioning (HFP)

**HSL** (*Helsingin seudun liikenne*) is the Helsinki Regional Transport Authority. It publishes the real-time position of its vehicles as open data.

- **What**: position and status of Helsinki public transport vehicles (bus, tram, train, metro, ferry, robot buses).
- **Frequency**: most vehicles publish roughly **1 message per second**.
- **Protocol**: **MQTT** (the IoT standard), JSON payload.
- **Broker**: `mqtt.hsl.fi`
  - `mqtts://mqtt.hsl.fi:8883` (MQTT over TLS) ← use this
  - `wss://mqtt.hsl.fi:443` (MQTT over WebSockets with TLS)
- **Topic**: `/hfp/v2/journey/#` (everything). Topics are hierarchical and can be filtered by vehicle type, route, etc.
- **Quick test from the terminal**:
  ```bash
  npm install -g mqtt
  mqtt subscribe -h mqtt.hsl.fi -p 8883 -l mqtts -v -t "/hfp/v2/journey/#"
  ```
- **Documentation**: https://digitransit.fi/en/developers/apis/5-realtime-api/vehicle-positions/high-frequency-positioning/

Why HSL: it is a genuine high-frequency push stream, with messy data, vehicles appearing and disappearing, and GPS coordinates. These are the same problems found in industrial IoT projects.

**Discarded / backup sources**
- *Sensor.Community*: real IoT sensors, but access is by polling an endpoint (last 5 minutes). Not streaming, so discarded.
- *aisstream.io*: ship positions over WebSocket (free API key, beta). Possible second push source in the future.

---

## Local Architecture (Phases 0-5)

```
HSL MQTT broker
      │  (MQTT, ~1 msg/s per vehicle)
      ▼
Python producer  ──────►  Redpanda (Kafka-compatible)
                                 │
                                 ▼
                          Python consumer
                                 │  micro-batches (every N seconds / N messages)
                                 ▼
                     Parquet partitioned by hour  (bronze)
                                 │
                                 ▼
                     dbt-duckdb  (silver → gold, incremental + tests)
                                 │
                                 ▼
          Streamlit / Grafana (real-time)  +  Power BI (gold)

   Orchestrator (Dagster or Airflow): scheduled dbt runs, compaction,
   data quality, freshness checks, alerts, backfills, service health checks
```

### Technical Choices

| Component | Choice | Why |
|---|---|---|
| Queue | **Redpanda** | Kafka-compatible, single container, lightweight. Fabric Eventstream accepts Kafka producers: the same producer code can be reused on Fabric by changing only the connection string. |
| Bronze storage | **Parquet partitioned by hour** | Mimics OneLake. Avoids DuckDB write locks (already hit in Closer Every Year): the consumer writes files, DuckDB only reads them. |
| Transformations | **dbt-duckdb** | Incremental silver/gold models and tests; on Fabric we switch to `dbt-fabric` on the Warehouse. |
| Orchestrator | **Dagster** (proposed) or **Airflow** | See "Open Decisions". |
| Containers | **Docker Compose** | Reproducible local stack. |

---

## Key Concept: Streaming Services vs Orchestration

A streaming system has two kinds of components:

- **Always-on services** (producer, consumer): they are not scheduled. They are started, monitored, and restarted if they fail.
- **Batch jobs** (dbt, compaction, quality checks, backfills): managed by the **orchestrator**.

The orchestrator is the "brain" of the batch side and monitors the health of the streaming side.

---

## Migration to Microsoft Fabric (Phase 6)

### Local → Fabric Mapping

| Task | Local | Fabric |
|---|---|---|
| Receive MQTT messages | always-on Python producer | Eventstream has an MQTT source: if it can connect to the HSL broker the producer disappears, otherwise a small Python bridge forwards messages via Kafka |
| Buffer / queue | Redpanda | Eventstream (managed) |
| Store raw data | consumer → Parquet | automatic (Eventhouse / Lakehouse) |
| Compact small files | scheduled job | automatic in Eventhouse; schedulable table maintenance in Lakehouse |
| Real-time transformations | — | KQL update policies and materialized views |
| Scheduled dbt silver/gold | **orchestrator** | **orchestrator** |
| Dependencies, retries, backfills | **orchestrator** | **orchestrator** |
| "No data for 2 minutes" alert | orchestrator / custom checks | Activator (native) |
| Real-time dashboard | Streamlit / Grafana | Real-Time Dashboard |
| Analytical dashboard | Power BI | Power BI (Direct Lake) |

### Parquet on Fabric
On Fabric you **don't manage Parquet files manually**:
- Eventstream → **Eventhouse**: data lands in KQL tables; the physical format is managed.
- Eventstream → **Lakehouse**: data lands directly as **Delta** tables (Parquet under the hood, files created by Fabric).
- With **OneLake availability**, Eventhouse tables are exposed in OneLake as Delta and can be read by dbt on the Warehouse.

The consumer that writes Parquet micro-batches **disappears**.

### Orchestration on Fabric
The orchestrator is **still needed**, but it does less (running dbt, handling dependencies, retries, backfills). Options:
1. **Fabric Data Pipelines**: native, visual (Azure Data Factory style).
2. **Apache Airflow job** inside Fabric: managed Airflow.
3. **External orchestrator** (Dagster/Airflow) triggering `dbt-fabric`.

### Fabric Architecture: Hot Path and Cold Path
- **Hot path** (seconds): Eventstream → Eventhouse (KQL) → Real-Time Dashboard + Activator.
- **Cold path** (minutes/hours): Eventhouse/Lakehouse in OneLake → dbt on the Warehouse (`dbt-fabric`) → Power BI.

dbt is batch: it doesn't belong in the hot path, but it builds historical and aggregated models in the cold path.

### Notes on dbt + Fabric
- Adapters: `dbt-fabric` (Warehouse, maintained by Microsoft) or `dbt-fabricspark` (Lakehouse).
- Requires the Microsoft ODBC driver; authentication via Microsoft Entra ID (service principal).
- The SQL dialect is **T-SQL**: macros written for DuckDB need adapting (prefer dbt's cross-database macros).
- Fabric is billed by **capacity** (F SKUs), shared across all workloads: keep an eye on the Fabric Capacity Metrics app.
- Language to learn for the Eventhouse: **KQL** (Kusto Query Language).

---

## Project Phases

Each phase ends with a working commit.

### Phase 0: Data Exploration
- [ ] Python script that connects to `mqtt.hsl.fi`, subscribes to a filtered topic (e.g. trams only) and prints messages
- [ ] Save a sample of messages as JSON
- [ ] Document the main payload fields and topic structure in `docs/data-model.md`

### Phase 1: Ingestion
- [ ] `docker-compose.yml` with Redpanda (+ Redpanda Console)
- [ ] Python producer: MQTT → Kafka topic
- [ ] Reconnection handling, logging, configuration via `.env`

### Phase 2: Consumer and Bronze
- [ ] Python consumer: Kafka → Parquet micro-batches, partitioned by date/hour
- [ ] Understand and document offsets, at-least-once delivery and duplicates

### Phase 3: Transformations with dbt
- [ ] `dbt-duckdb` project reading the bronze Parquet files
- [ ] Silver: cleaning, typing, deduplication
- [ ] Gold (examples): average delay per route, average speed per area, active vehicles per minute
- [ ] Incremental models + tests (not_null, unique, accepted_range, freshness)

### Phase 4: Orchestration
- [ ] Orchestrator in Docker Compose
- [ ] Scheduled dbt runs (every 5-15 minutes) or sensor-triggered (new files arrived)
- [ ] Compaction job for small Parquet files
- [ ] Freshness checks and alerts ("no messages for 2 minutes")
- [ ] Health checks for producer/consumer services
- [ ] Backfills

### Phase 5: Visualization
- [ ] Fast real-time dashboard (Streamlit or Grafana)
- [ ] Power BI on the gold models

### Phase 6: Microsoft Fabric
- [ ] Activate a Fabric trial
- [ ] Eventstream (direct MQTT source or Kafka bridge from the producer)
- [ ] Eventhouse + KQL queries + update policies / materialized views
- [ ] Real-Time Dashboard + Activator for alerts
- [ ] OneLake availability → dbt with `dbt-fabric` on the Warehouse (cold path)
- [ ] Orchestration with Fabric Data Pipelines / Airflow job
- [ ] Power BI in Direct Lake mode
- [ ] Comparison in this README: what we built by hand locally vs what Fabric manages for us

---

## Repository Structure (proposed)

```
├── docker-compose.yml
├── .env.example
├── pyproject.toml
├── ingestion/
│   ├── explore/          # Phase 0: MQTT exploration scripts
│   ├── producer/         # Phase 1: MQTT → Redpanda
│   └── consumer/         # Phase 2: Redpanda → Parquet
├── data/
│   └── bronze/           # Partitioned Parquet (gitignored)
├── dbt/                  # Phase 3: dbt project (silver, gold)
├── orchestration/        # Phase 4: Dagster/Airflow
├── dashboard/            # Phase 5: Streamlit/Grafana
├── fabric/               # Phase 6: Eventstream config, KQL, dbt-fabric profile
└── docs/
    ├── data-model.md
    ├── decisions.md
    └── troubleshooting.md
```

---

## Open Decisions

- **Orchestrator: Dagster vs Airflow**
  - *Airflow*: already familiar (used in Closer Every Year); Fabric has a native Airflow job, so the transition is direct.
  - *Dagster*: more modern, asset-based (like Bruin), has *sensors* to react to events (e.g. new files → run dbt) and excellent dbt integration. A new skill to learn.
  - Proposal: **Dagster**, because it teaches more about bridging streaming and batch.
- **Real-time dashboard: Streamlit vs Grafana**: to be decided in Phase 5.
- **Fabric: direct MQTT source in Eventstream or Kafka bridge**: to be verified in Phase 6.

---

## Previous Project

[Closer Every Year](https://github.com/LucaBRS/Closer-Every-Year): batch pipeline on Eurostat data (Bruin, DuckDB, BigQuery, Terraform, Airflow + dbt, Power BI). This project is the next step: from **batch** to **streaming**, from **GCP** to **Microsoft Fabric**.
