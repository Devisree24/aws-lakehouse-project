# Data Quality Layer

## Overview
We validate incoming order data and separate bad records instead of letting them into curated tables. Invalid rows are written to **quarantine**; run-level metrics are written to **quality_reports**.

## Checks Applied (Orders)

| Check | Rule | Action |
|-------|------|--------|
| **customer_id** | Must not be NULL | Reject row → quarantine |
| **product_id** | Must not be NULL | Reject row → quarantine |
| **order_ts** | Must not be NULL or empty | Reject row → quarantine |
| **qty** | Must be > 0 | Reject row → quarantine |
| **order_id** | Duplicates: keep latest by order_ts | Other duplicates → quarantine |

## Where It Runs
- **Batch ETL:** `glue_batch_to_curated` (Step 5)
- **Incremental ETL:** `glue_incremental_to_curated` (Step 10)

## Outputs

| Output | S3 Path | Format |
|--------|---------|--------|
| **Quarantine (invalid rows)** | `s3://de-lakehouse-devis-us-east-1/quarantine/orders/` | Parquet |
| **Quality reports (run stats)** | `s3://de-lakehouse-devis-us-east-1/quality_reports/` | JSON |

Quality report JSON fields: `total_raw`, `total_valid`, `total_invalid`, `run_ts`.

## Monitoring & Logs
- **Glue jobs:** CloudWatch log group `/aws-glue/jobs` (driver/executor logs, errors).
- **Lambda (order generator):** CloudWatch log group `/aws/lambda/lambda_generate_orders`.

## How to Inspect
1. **S3:** Open `quarantine/orders/` and `quality_reports/` to see files.
2. **Athena (optional):** Crawl `quality_reports/` with a Glue crawler, then query the table to see latest run metrics.
3. **CloudWatch:** Use the log groups above to debug failed runs or inspect logs.
