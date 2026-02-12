# Data Model

## Fact Table

**orders** (curated, Parquet, partitioned)
- **Columns:** order_id, customer_id, product_id, order_ts, order_date, qty, unit_price, status, revenue, year, month, day
- **Partition keys:** year, month, day (derived from order_ts)
- **Location:** s3://bucket/curated/orders/

## Dimension Tables

**dim_customers** (curated, Parquet)
- **Columns:** customer_id, name, email, city, state, created_at
- **Location:** s3://bucket/curated/dim_customers/

**dim_products** (curated, Parquet)
- **Columns:** product_id, category, price
- **Location:** s3://bucket/curated/dim_products/

## Raw (Source)

- **orders** (CSV): raw/orders/
- **customers**, **products** (CSV): raw/customers/, raw/products/
- **orders_incremental** (JSON): raw/orders_incremental/
