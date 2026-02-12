"""
Lambda: Generate synthetic new orders and write to S3 (raw/orders_incremental/).
For Step 9 - Incremental pipeline. Schedule with EventBridge (e.g. every 60 min).
"""
import json
import random
from datetime import datetime, timezone
import boto3

s3 = boto3.client("s3")

# Change to your bucket name
BUCKET = "de-lakehouse-devis-us-east-1"
PREFIX = "raw/orders_incremental/"

# Example customer/product IDs (match your data or keep small set)
CUSTOMER_IDS = list(range(1, 101))
PRODUCT_IDS = list(range(101, 351))
STATUSES = ["COMPLETED", "PENDING", "CANCELLED"]


def generate_order(order_id_int: int) -> dict:
    now = datetime.now(timezone.utc)
    order_ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    customer_id = random.choice(CUSTOMER_IDS)
    product_id = random.choice(PRODUCT_IDS)
    qty = random.randint(1, 5)
    unit_price = round(random.uniform(10.0, 200.0), 2)
    status = random.choice(STATUSES)
    return {
        "order_id": f"INC-{order_id_int}",
        "customer_id": customer_id,
        "product_id": product_id,
        "order_ts": order_ts,
        "qty": qty,
        "unit_price": unit_price,
        "status": status,
    }


def lambda_handler(event, context):
    num_orders = 10  # Keep small for free-tier; increase to 50 if desired
    orders = [generate_order(i) for i in range(num_orders)]

    now = datetime.now(timezone.utc)
    filename = now.strftime("orders_%Y%m%d_%H%M%S.json")
    key = f"{PREFIX}{filename}"
    body = "\n".join(json.dumps(o) for o in orders)

    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))

    return {
        "statusCode": 200,
        "body": json.dumps({"written": len(orders), "key": key}),
    }
