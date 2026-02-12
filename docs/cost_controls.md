# Cost Controls (Free-Tier Friendly)

- **Budget:** $5 monthly cost budget with email alert (e.g. at 80%).
- **Region:** Single region (e.g. us-east-1) for all resources.
- **Data size:** Small datasets (e.g. &lt;100 MB total) to limit S3 and scan costs.
- **Formats:** Parquet + partition by year/month/day to reduce Athena scan size.
- **Athena:** Run queries only when needed; avoid repeated large scans.
- **Lambda schedule:** Hourly (not every minute) for incremental order generation.
- **Cleanup:** Remove or lifecycle old test data when done.
