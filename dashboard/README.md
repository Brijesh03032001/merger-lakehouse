# BI Dashboard

This directory contains screenshots and documentation of analytics dashboards built on the Gold layer.

## Sample Dashboards

### Dashboard Overview

**Add `bi_dashboard.png` screenshot here**

Expected visualizations:

1. **Orders Summary**
   - Total orders by month
   - Revenue by customer segment
   - Order volume trends

2. **Product Performance**
   - Top 10 products by revenue
   - Product category mix
   - Pricing analysis

3. **Customer Analytics**
   - Customer acquisition by month
   - Customer lifetime value
   - Repeat purchase rate

4. **Operational KPIs**
   - Data freshness (last load timestamp)
   - Data quality metrics
   - Pipeline execution times

## Databricks Genie

**AI-Powered Self-Service Analytics**

```sql
-- Example Genie queries against Gold layer

Q: "Show me top 5 customers by spend last quarter"
→ SELECT customer_name, SUM(order_amount)
   FROM fmcg.gold.sb_fact_orders
   WHERE month_key >= 202410
   GROUP BY 1 ORDER BY 2 DESC;

Q: "What's our order trend this year?"
→ SELECT date_key, COUNT(*) AS daily_orders
   FROM fmcg.gold.sb_fact_orders
   WHERE YEAR(date_key) = 2024
   GROUP BY 1;
```

## Connecting BI Tools

### Tableau / Power BI / Looker

```
Databricks Gold Tables
         ↓
    SQL Endpoint
         ↓
    BI Tool (JDBC/ODBC)
         ↓
    Shared Dashboards
```

**Connection String:**
```
host: your-workspace.cloud.databricks.com
port: 443
catalog: fmcg
schema: gold
```

## Dashboard Refresh Schedule

- **Dimensions** (daily): `1 AM UTC` (post-incremental load)
- **Facts** (daily): `2 AM UTC` (post-full or incremental load)
- **BI Dashboards** (scheduled): Auto-refresh via BI tool connectors

---

**Add actual dashboard screenshot (`.png` file) to this directory for portfolio impact.**
