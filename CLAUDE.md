# CLAUDE.md: helsinki-transport-tracker

Learning project for a data engineer specializing in streaming / IoT data.
Pipeline: HSL tram MQTT -> Redpanda -> Parquet bronze -> dbt-duckdb silver/gold ->
orchestration (Dagster or Airflow, undecided) -> Microsoft Fabric (Eventstream/Eventhouse/KQL,
OneLake, dbt-fabric, Power BI).

Current status, decisions and next step: @PROGRESS.md

## Rules for working with me (Luca)

### Language
- Talk to me in **Italian**. I understand better, and I often write in Italian.
- Everything written to files is in **English**: code, comments, docstrings, all `.md` documentation.

### How we learn
- **Explain first, then code.** Theory in small pieces; code only after the theory is clear.
- Work in **small tranches**: one concept or one file part at a time, then I test and report.
- Explain every library and every call: what it is, who calls it, why we need it.
  Use analogies when a concept is abstract (bookmark for offsets, post office for the dead letter queue). Add also comment in the code
- If I misunderstand something, correct it plainly and re-explain with a simpler example.
- Do not ask me check questions in documentation files. In chat it is fine to ask one, sparingly.

### Code style (plain Python)
- No f-strings (use `+`, `str()` or `print(a, b)`), no ternary `x if c else y`,
  no list/dict comprehensions, no contracted forms such as `x += 1` (write `x = x + 1`).
- Short functions with one job, clear names, a comment on the "why", not on the obvious.
- Say clearly what was tested and what was not (for example "not run against real Redpanda").

### Documentation files
- One folder per step (`0N-name/`), with `step-NN.md` (theory, exercise, status, notes),
  `step-NN-questions.md` and, when useful, `step-NN-commands.md`.
- `step-NN-questions.md` contains **only the questions I ask you, with your answers**
  (never your questions to me), in the order they came up.
- `learning/learning_readme.md` is the index of the steps. Do **not** edit the README in the project
  root unless I ask.
- Keep my own notes sections as they are; do not overwrite files I edited by hand without asking.
  Read the current file first, my local copy may differ from what you remember.

### Keeping PROGRESS.md up to date (always)
- After every finished piece of work (a script that runs, a decision, a problem solved or found)
  and **at the end of every session**, update `PROGRESS.md`: what is done, decisions and why,
  open problems, the next concrete step.
- Keep it short and current. Replace stale lines, do not just append.
- When I say "update progress" do it right away.

## Environment
- Windows, **Git Bash**, `uv` for Python (`uv add`, `uv run`). Paths with `/`; create files with `touch`
  (not `type nul`).
- Docker Desktop must be running for Redpanda: `docker compose up -d` in `learning/03-redpanda-kafka`.
- Run scripts from the project root, for example `uv run learning/03-redpanda-kafka/producer.py`.
- HSL MQTT: `mqtt.hsl.fi`, port **1883, no TLS** (port 8883 fails here with CERTIFICATE_VERIFY_FAILED).
- Use paho-mqtt 2.x with `CallbackAPIVersion.VERSION2`.

## Project facts worth remembering
- Routes: 1009 (line 9) and 1006 (line 6). About 55 messages per second together.
- MQTT topic levels after `tram`: operator (index 7), vehicle (index 8), route (index 9).
- Each HSL event arrives about 4 times; cause not confirmed yet. Bronze keeps duplicates;
  silver deduplicates on `(oper, veh, tsi)`.
- `tst` = event time (UTC) of the tram; `received_at` = our arrival time.
- Delivery rule: write the file first, commit the offset after (at-least-once); duplicates are accepted.
