#!/usr/bin/env bash
# Upload a landing-zone folder to the managed Volume the dev target reads.
#   ./scripts/upload_demo_data.sh [profile] [catalog] [data_root]
# data_root defaults to the project's original local sample; pass demo_data (from
# scripts/generate_demo_data.py) to also upload the synthetic parent company.
set -euo pipefail

PROFILE="${1:-fmcg-free}"
CATALOG="${2:-fmcg_dev}"
DATA_ROOT="${3:-0_data/2_child_company/full_load}"
ROOT="dbfs:/Volumes/${CATALOG}/bronze/landing"

for directory in customers products gross_price orders/landing orders/processed; do
  databricks fs mkdir "$ROOT/$directory" --profile "$PROFILE"
done

for directory in customers products gross_price; do
  databricks fs cp --profile "$PROFILE" --recursive --overwrite "$DATA_ROOT/$directory" "$ROOT/$directory"
done
databricks fs cp --profile "$PROFILE" --recursive --overwrite "$DATA_ROOT/orders/landing" "$ROOT/orders/landing"

# Present only for the synthetic merger scenario; production already holds the parent company.
for directory in parent_customers parent_products parent_fact_orders; do
  if [ -d "$DATA_ROOT/$directory" ]; then
    databricks fs mkdir "$ROOT/$directory" --profile "$PROFILE"
    databricks fs cp --profile "$PROFILE" --recursive --overwrite "$DATA_ROOT/$directory" "$ROOT/$directory"
  fi
done

echo "Uploaded $DATA_ROOT to /Volumes/${CATALOG}/bronze/landing"
