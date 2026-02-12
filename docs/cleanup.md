# Cleanup Guide (Cost Control)

## Stop Data Generation First
1. EventBridge Scheduler: delete/disable `schedule-lambda-generate-orders`
2. Lambda: delete `lambda_generate_orders`

## Delete Glue Resources
1. Delete ETL jobs:
   - `glue_batch_to_curated`
   - `glue_incremental_to_curated`
2. Delete crawlers:
   - `crawler_raw_lakehouse`
   - `crawler_curated_lakehouse`
3. Delete Glue tables and `lakehouse_db`

## Delete S3
1. Empty bucket `de-lakehouse-devis-us-east-1`
2. Delete the bucket

## Delete IAM Roles (project-specific only)
- `lambda-generate-orders-role`
- `AWSGlueServiceRole-lakehouse`
- scheduler invoke role (if created)

## Verify Billing
- Check Cost Explorer and Budgets for no new charges after cleanup.