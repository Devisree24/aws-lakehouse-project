# AWS Lakehouse — E-Commerce Data Pipeline

A small **data lakehouse** on AWS: batch and incremental ingestion, Glue ETL, curated Parquet, and Athena analytics. Built for learning and portfolio (free-tier friendly).

## Architecture

![Architecture](docs/architecture.png)

- **S3:** Raw (CSV/JSON) → Staging (optional) → Curated (Parquet, partitioned) + Quarantine + Quality reports + Athena results.
- **Glue:** Data Catalog (crawlers), Batch ETL (CSV → Parquet), Incremental ETL (merge raw + incremental → curated).
- **Athena:** Query curated tables (daily revenue, top products, customer LTV, region performance).
- **Lambda + EventBridge:** Synthetic incremental orders → `raw/orders_incremental/` on a schedule.

## Data Flow

1. **Batch:** CSVs in `raw/` → Glue crawler → Glue batch job → `curated/` (Parquet, partitioned by year/month/day).
2. **Incremental:** Lambda writes new orders (JSON) to `raw/orders_incremental/`; Glue incremental job merges with raw and overwrites `curated/orders/`.
3. **Analytics:** Athena queries on `lakehouse_db` (curated tables).

## Data Model

- **Fact:** `orders` — order_id, customer_id, product_id, order_date, revenue; partitioned by year, month, day.
- **Dimensions:** `dim_customers`, `dim_products` (curated Parquet in S3).

See [docs/data_model.md](docs/data_model.md) for details.

## Partition Strategy

- **orders:** Partitioned by `year`, `month`, `day` derived from `order_ts` for efficient date-range queries in Athena.

## How to Run (High-Level)

1. Create S3 bucket and folder structure (raw, curated, quarantine, quality_reports, athena-results).
2. Upload CSVs to raw (customers, products, orders).
3. Create Glue database and crawlers (raw, then curated).
4. Run Glue batch job; then run incremental job when incremental data exists.
5. Set Athena workgroup result location to `s3://.../athena-results/`.
6. Run analytics SQL in Athena (see `sql/`).
7. (Optional) Deploy Lambda + EventBridge schedule for incremental ingestion.

## Tech Stack

- **AWS:** S3, Glue (Catalog, Crawlers, ETL), Athena, Lambda, EventBridge.
- **Format:** CSV (raw), JSON (incremental), Parquet (curated).

## Cost Controls

- $5 monthly budget alert; single region (e.g. us-east-1); small datasets; Parquet + partitions; limited Athena usage; hourly schedule for Lambda. See [docs/cost_controls.md](docs/cost_controls.md).

## Project Structure

```
├── README.md
├── docs/
│   ├── architecture.png
│   ├── data_model.md
│   └── cost_controls.md
├── etl/
│   ├── glue_batch_to_curated.py
│   └── glue_incremental_to_curated.py
├── lambda/
│   └── generate_orders.py
└── sql/
    ├── athena_daily_revenue.sql
    ├── athena_top_products.sql
    ├── athena_customer_ltv.sql
    └── athena_region_performance.sql
```

## Screenshots (Optional)

- S3 bucket structure (raw, curated, quarantine).
- Glue job run (batch or incremental) — Succeeded.
- Athena query result (e.g. daily revenue or top products).

---

*Built as a portfolio project for AWS data engineering.*
