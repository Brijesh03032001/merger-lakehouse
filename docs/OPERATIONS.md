# Operating the pipeline

## Jobs

| Job | Trigger | What it does |
|---|---|---|
| `fmcg_prepare_storage` | manual | catalog, schemas, landing volume |
| `fmcg_initial_load` | manual | dimensions, then the historical order load (batch notebooks) |
| `fmcg_daily_incremental` | 02:00 UTC | batch-notebook daily load. Fails on a day with no new files. |
| `fmcg_orders_autoloader` | 02:00 UTC, **paused by default** | exactly-once Auto Loader ingestion + health gate. Replaces the daily batch job. |
| `fmcg_orders_kafka` | every 15 min, **paused by default** | drain the Kafka order topic exactly once per message (see below) |
| `fmcg_backfill_orders` | manual | rebuild an order-date range from Bronze under the current rules |
| `fmcg_entity_resolution` | manual / after dimension changes | match acquired to parent records, publish unified and point-in-time views |
| `fmcg_data_health` | 06:00 UTC | reconcile layers, check SCD2, freshness, valid rate; fails the run on a problem |
| `fmcg_demo_seed_parent` | manual, demo only | loads a synthetic parent company |

`fmcg_initial_load` and `fmcg_daily_incremental` are unchanged from the versions verified on Databricks Free Edition. Everything
newer lives in additional jobs.

## Kafka ingestion

Orders can arrive as messages instead of files. `fmcg_orders_kafka` runs `5_kafka_orders.py`, which turns each message into the same
micro-batch the file path produces and hands it to the same `process_order_batch`: Bronze, the quality contract, Silver, Gold and the
shared monthly fact are unchanged. Kafka's Bronze is its own table, `bronze.orders_kafka`.

**Message contract.** A JSON object with `order_id`, `order_placement_date`, `customer_id`, `product_id`, `order_qty` (numbers or strings).
Key the message by `order_id`, so every message about one order lands in one partition in order. Extra fields are kept, whole, in
`raw_payload` in Bronze and ignored downstream, so a producer adding a field breaks nothing.

**Bad messages are quarantined, never dropped.** A message that yields none of the five fields (garbage, empty, truncated, a JSON
array, or a producer that renamed every field) is flagged `is_malformed` and quarantined as `malformed_message` with its payload. A
message with some fields fails the contract with its specific reasons, like any other row. A renamed field therefore shows up as a surge
in `malformed_message` and trips the `valid_rate` health check.

**Exactly once.** Spark's checkpoint tracks offsets, and Bronze also remembers `(topic, partition, offset, Kafka timestamp)` for every
message it holds, so a retry, a reset checkpoint or an overlapping offset range cannot duplicate or drop anything. The timestamp is in the
key on purpose: a topic that is deleted and recreated restarts at offset 0, and on offsets alone its new messages would be skipped as
"already seen". The later message (by Kafka timestamp, then offset) wins a correction. Progress lives in the checkpoint, not in a consumer
group, so consumer-group lag tools do not show this job's position.

**How it runs.** `trigger=availableNow` (the job's default) reads up to `max_offsets_per_trigger` messages per micro-batch, drains the topic
and stops, so a 15-minute schedule gives near-real-time freshness without a cluster running all day. `trigger=processingTime=30 seconds`
runs continuously and never returns; use it only in a dedicated streaming job, because every micro-batch is a MERGE and a small Delta commit.

**Retention.** `failOnDataLoss` defaults to true: if the topic's retention deleted offsets this job never read, the run fails instead of
silently skipping them. Recover by loading the gap from the source of record, then rerun.

**Security.** For SASL, create a secret scope with `kafka_username` and `kafka_password` and pass its name as `kafka_secret_scope`. On
Databricks the Kafka client is relocated, so the login module needs the `kafkashaded.` prefix (the `jaas_module_prefix` default); on
open-source Spark set it to blank. Credentials never appear in the bundle, and a test enforces that. The workspace must be able to reach the
brokers over the network; Free Edition may not allow that.

**Coexisting with the file jobs.** All three paths write the same Silver and Gold, and Delta can reject overlapping MERGEs on one table with
a concurrent-modification error. Do not schedule the Kafka job across the daily file-load window. The health gate's Bronze recompute reads
all three Bronze tables, so it stays correct whichever mix you run. A Silver row's `read_timestamp` is the Kafka message time, so
`freshness_orders` measures the newest *message*, and fails if either the producer or the pipeline stalls.

**Trying it locally.** `make kafka-up` starts a broker (Docker), `make kafka-produce` replays synthetic orders into it, and `make test-kafka`
runs the real-broker test. **None of that has been run:** no broker was available when this was written, so `format("kafka")` against a real
broker is unverified. Everything after the source is tested without a broker (`tests/spark/test_kafka_ingestion.py`).

## Moving from the batch job to Auto Loader

Both read `orders/landing/`. The batch job moves files to `processed/` after loading; Auto Loader leaves them in place and
remembers them in a checkpoint. They must never both be live.

1. `databricks bundle run fmcg_data_health` and confirm it is green.
2. Pause `fmcg_daily_incremental` (set `incremental_schedule_status=PAUSED`, redeploy).
3. Redeploy with `autoloader_schedule_status=UNPAUSED`, or run `fmcg_orders_autoloader` once by hand first.
4. Auto Loader writes a new Bronze table, `bronze.orders_raw` (all columns as strings). History stays in `bronze.orders`. The health
   gate's Bronze recompute reads both.

Cutting back is the reverse. Nothing is lost, because Bronze is append-only and ingestion skips files it already holds.

## Why a file is never loaded twice or dropped

Three independent layers, so no single one has to be trusted:

1. **Auto Loader checkpoint**: each file is delivered once.
2. **Bronze remembers files**: `ingest_bronze` appends only rows whose `(file_path, file_modification_time)` Bronze does not
   already hold. A retried micro-batch, a replayed file, or a deleted checkpoint therefore cannot duplicate Bronze.
   (Delta's `txnVersion` was rejected here because, after a checkpoint reset, batch ids restart at zero and it would silently
   skip real data.)
3. **Silver and Gold are MERGEs on business keys** that rewrite a row only if a business value changed, so a replay produces no
   updates and the Change Data Feed shows real changes only. A later file that corrects an earlier one wins.

Quarantine and metrics rows for a micro-batch carry an id derived from the files in it, so a retry replaces its own rows.

Limits: if the *same* order arrives in two different files with different quantities, the later `read_timestamp` wins, with no
conflict record. Rows are never hard-deleted from Silver by ingestion; use a backfill.

## Schema changes in the source

- **A new column appears.** Auto Loader stops the stream once (`UnknownFieldException`), widens its stored schema, and the task's
  one retry restarts it. The column lands in Bronze. Silver and Gold are a fixed contract and ignore it.
- **A required column disappears or is renamed.** The raw rows are written to Bronze first, then the task fails with
  `SchemaContractError` so the problem is visible, not guessed at. Fix the mapping and rerun; Bronze skips the files it already has
  and the rest of the pipeline picks them up.
- **Types.** Bronze keeps every column as a string. Typing happens in the contract, so a day whose dates are all ISO cannot be
  inferred as `DATE` and break the append (the batch notebooks do this).

## Backfill

```bash
databricks bundle run --target prod fmcg_backfill_orders \
  --params start_date=2025-08-01,end_date=2025-08-31,max_order_qty=10000
```

It re-derives Silver, Gold and the shared monthly fact for that order-date range from Bronze using the current rules, with
**replace** semantics: a row the rules no longer accept leaves Silver and Gold and appears in `quarantine_orders` with a reason.
Re-running the same arguments leaves every table unchanged. The parent company's own rows in the shared fact are never touched.
Rows whose order date cannot be parsed belong to no range and are left as they are.

Rows quarantined earlier for that date range are replaced by this run's evaluation; the old rows remain readable with time
travel. Run `fmcg_data_health` afterwards (the job does).

## The health gate

`fmcg_data_health` fails, and your `alert_emails` are notified, when any of these breaks. Each compares two things the pipeline
already promises are equal.

| Check | Fails when | First thing to look at |
|---|---|---|
| `run_accounting` | a quality run has `raw ≠ valid + quarantined` | `silver.order_quality_metrics` |
| `valid_rate` | the latest daily run's valid rate is below `min_valid_rate` (0.6) | `quarantine_orders` grouped by reason: a source change usually shows as one reason code surging |
| `silver_gold_row_count`, `silver_gold_quantity` | Gold differs from Silver | a failed or partial run; rerun the load or a backfill |
| `daily_vs_monthly_quantity` | the shared monthly fact differs from the daily fact for the acquired company's customers | a backfill or load stopped before the monthly refresh |
| `bronze_gold_row_count`, `bronze_gold_quantity` | Gold differs from what the contract derives from Bronze | someone edited Gold, a rule changed without a backfill, or files are missing from Bronze |
| `scd2_integrity_*` | a key has two current versions, none, or a gap/overlap between versions | `sb_dim_*_history`, then `DESCRIBE HISTORY` |
| `fact_product_orphans` | Gold has product codes with no dimension row | product dimension load |
| `fact_customer_orphans` | **warning**: Gold has customers with no dimension row | a customer arrived before its master record |
| `xref_code_collisions_*` | a customer or product code exists in both companies | see below |
| `freshness_orders` | the newest ingested order is older than `max_data_age_hours` (30) | the daily job did not run, or the source stopped sending files |

`xref_code_collisions`: the shared fact tables cannot say which company a code came from, so a code used by both would be counted
twice in unified reports. Fix it by namespacing codes upstream.

`freshness_orders` also fires on a legitimately quiet day (a holiday with no files). Raise `max_data_age_hours` or accept the
alert; it is deliberately not guessed away.

Results accumulate in `silver.pipeline_check_results`; `gold.vw_data_health` shows the latest status per check.

### Alerts

Set the recipients once per target in `databricks.yml`:

```yaml
targets:
  prod:
    variables:
      alert_emails: ["data-platform@example.com"]
```

They receive failures from the ingestion, backfill, entity resolution and health jobs, plus a warning when the Auto Loader job runs
longer than an hour. A load that never started is caught by `freshness_orders` in the 06:00 health job, not by the ingestion job.
Free Edition may not deliver email notifications.

## Reading history

```sql
-- what did the customer look like when the order was placed?
SELECT order_id, date, city_as_of, product_as_of FROM fmcg.gold.vw_fact_orders_pit WHERE customer_code = '789403';

-- what did the monthly fact say last quarter?  (history is kept 90 days; use an offset)
SELECT * FROM fmcg.gold.fact_orders TIMESTAMP AS OF '2026-09-30 23:59:59+00:00';

-- row-level lineage of a Silver correction
SELECT * FROM table_changes('fmcg.silver.orders', 5) WHERE _change_type = 'update_postimage';
```

In `vw_fact_orders_pit` an order resolves to the customer or product version valid on its order date; the first version of each
key is back-dated to the beginning of time so orders older than the first load still resolve. Versions otherwise start when the
pipeline first *observed* the change, not when it happened in the business. If the source supplies a business effective date, pass
it as `effective_at` to `apply_scd2`.

## Known limits

- The Auto Loader notebook, the Kafka source against a real broker, the new bundle jobs and the live Claude calls have **not** been run. See the README's
  verification table.
- The batch notebooks re-run the whole quality contract several times; Databricks serverless cannot cache, so the pipeline does
  not rely on caching (it is used when the compute allows it).
- Entity resolution runs on the driver after blocking and refuses more than 200,000 records per side.
- Auto Loader and the Kafka job both run with the `availableNow` trigger: incremental micro-batches on a schedule, not sub-second streaming. Latency is the schedule interval plus a run's duration (a 5,000-row micro-batch took about 5-7 s end to end in the local benchmarks, so a continuous 30-second trigger would spend a large share of its time on fixed overhead).
