# Databricks notebook source
"""
Entity Resolution – Parent / Acquired-Company Merger

Purpose:
- Decide which acquired-company customers and products are the same real-world
  entity as a parent-company record, despite different IDs and spellings
- Auto-match confident pairs, optionally let Claude settle the ambiguous band,
  and queue what is still uncertain for a human
- Publish a cross-reference table so Gold reporting can roll both companies up
  to one unified customer / product

Tech:
- PySpark, Delta Lake (MERGE, replaceWhere)
- Pure-Python matching core (common/entity_resolution.py)
- Optional Claude adjudication (common/llm_adjudicator.py)
"""

# COMMAND ----------

import os

# COMMAND ----------

# MAGIC %run ../1_setup/utilities

# COMMAND ----------

# MAGIC %run ../common/entity_resolution

# COMMAND ----------

# MAGIC %run ../common/llm_adjudicator

# COMMAND ----------

# MAGIC %run ../common/entity_resolution_delta

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("entity", "customers", "Entity (customers | products)")
dbutils.widgets.text("llm_enabled", "false", "Let Claude settle the review band (true | false)")
dbutils.widgets.text("llm_model", DEFAULT_MODEL, "Claude model")
dbutils.widgets.text("llm_max_pairs", "200", "Max uncached pairs sent to Claude per run")
dbutils.widgets.text("secret_scope", "", "Databricks secret scope holding ANTHROPIC_API_KEY")

catalog = dbutils.widgets.get("catalog")
entity = dbutils.widgets.get("entity")
llm_enabled = dbutils.widgets.get("llm_enabled").strip().lower() == "true"
llm_model = dbutils.widgets.get("llm_model")
llm_max_pairs = int(dbutils.widgets.get("llm_max_pairs"))
secret_scope = dbutils.widgets.get("secret_scope")

profile = CUSTOMER_PROFILE if entity == "customers" else PRODUCT_PROFILE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load parent and child records

# COMMAND ----------

ensure_control_tables(spark, catalog, silver_schema)
parents, children = load_records(spark, entity, catalog, silver_schema, gold_schema)
overrides = load_overrides(spark, entity, catalog, silver_schema)
labels = load_labels(spark, entity, catalog, silver_schema)
print(f"{entity}: {len(parents)} parent records, {len(children)} child records, "
      f"{len(overrides)} analyst overrides, {len(labels)} labelled children")

# The matcher runs on the driver after blocking. Fail loudly instead of silently crawling.
MAX_DRIVER_RECORDS = 200_000
if max(len(parents), len(children)) > MAX_DRIVER_RECORDS:
    raise RuntimeError("Too many records for driver-side matching; partition by block key first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Claude for the ambiguous band
# MAGIC Only business/product names and city/category are sent, never customer contacts.
# MAGIC It is off by default; leave it off for data that has not been approved for external processing.

# COMMAND ----------

adjudicator = None
if llm_enabled:
    if anthropic is None:
        raise RuntimeError("llm_enabled=true needs the 'anthropic' package in this job's environment")
    if secret_scope and "ANTHROPIC_API_KEY" not in os.environ:
        os.environ["ANTHROPIC_API_KEY"] = dbutils.secrets.get(secret_scope, "ANTHROPIC_API_KEY")
    adjudicator = ClaudeAdjudicator(
        model=llm_model, cache=load_llm_cache(spark, catalog, silver_schema), max_calls=llm_max_pairs
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve

# COMMAND ----------

resolution = resolve(
    parents, children, profile, overrides=overrides, adjudicator=adjudicator, track_pool=False
)

# COMMAND ----------

metrics = persist_resolution(
    spark,
    catalog=catalog,
    entity=entity,
    parents=parents,
    children=children,
    resolution=resolution,
    llm_new_entries=adjudicator.new_entries if adjudicator else None,
    llm_model=llm_model if adjudicator else None,
    llm_usage=adjudicator.usage if adjudicator else None,
    labels=labels,
    silver=silver_schema,
    gold=gold_schema,
)
print("Entity resolution metrics:", metrics)

# COMMAND ----------

display(spark.table(f"{catalog}.{silver_schema}.entity_match_review_queue").filter(f"entity = '{entity}'").limit(50))
