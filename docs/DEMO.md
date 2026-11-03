# Two-minute demo script

For a screen recording or a live walkthrough. It assumes the dev target is deployed and `make deploy` has run
(see [DEPLOYMENT.md](DEPLOYMENT.md)); every query below runs against `fmcg_dev`.

**0:00 The problem.** "Two companies, one customer base. A naive merge counts shared customers twice." Show the Mermaid diagram in the README.

**0:20 One customer, two spellings.**

```sql
SELECT x.source_system, d.customer, x.match_status, x.match_method, round(x.match_score, 3) AS score
FROM fmcg_dev.gold.xref_customers x JOIN fmcg_dev.gold.dim_customers d ON d.customer_code = x.source_code
WHERE x.unified_id IN (SELECT unified_id FROM fmcg_dev.gold.xref_customers GROUP BY unified_id HAVING COUNT(*) = 2)
ORDER BY x.unified_id, x.source_system LIMIT 10;
```

"Parent and acquired records, one unified id, scored." Then the numbers: `SELECT * FROM fmcg_dev.gold.vw_customer_overlap`.

**0:50 What it was unsure about.** `SELECT * FROM fmcg_dev.silver.entity_match_review_queue ORDER BY score DESC LIMIT 5` ("Primefuel Dairy" vs
"Primefuel Nutrition": same brand, different business). Insert one row into `entity_match_overrides`, rerun `fmcg_entity_resolution`, and show the
pair leave the queue.

**1:10 Reports that are correct as of the order date.**

```sql
SELECT order_id, date, city_as_of FROM fmcg_dev.gold.vw_fact_orders_pit WHERE customer_code = '<a customer whose city changed>';
SELECT region, period, sold_quantity, parent_quantity, child_quantity FROM fmcg_dev.gold.vw_orders_by_region_month ORDER BY month, region;
```

**1:30 It tells you when it is wrong.** `SELECT * FROM fmcg_dev.gold.vw_data_health`. Delete a Gold row, run `fmcg_data_health`, show the failed
check and the job-failure notification.

**1:50 Proof.** `make test-spark` output (the real notebooks on local Spark), and the entity-matching table from
[ENTITY_RESOLUTION.md](ENTITY_RESOLUTION.md), with the caveat that it is synthetic data.
