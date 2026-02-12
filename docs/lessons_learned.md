# Lessons Learned

- Keeping all services in one region (`us-east-1`) avoids many setup issues.
- Athena result location must be set correctly in the workgroup; wrong bucket causes query failures.
- IAM permissions are critical: Glue needed S3 write access (`PutObject`) for curated/quarantine outputs.
- Partitioned Parquet in curated layer improves query efficiency and reduces Athena scan cost.
- Incremental ingestion with Lambda + EventBridge is simple and effective for simulation.
- Data quality checks (nulls, qty, duplicates) and quarantine outputs make pipelines reliable.
- Option A incremental strategy is easy to implement; Option B (partition overwrite/upsert) is a good future improvement.