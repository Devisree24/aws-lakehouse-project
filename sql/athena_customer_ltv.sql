-- 8.3 Customer LTV (uses raw orders)
-- Database: lakehouse_db

SELECT
  customer_id,
  SUM(qty * unit_price) AS lifetime_value
FROM orders
GROUP BY customer_id
ORDER BY lifetime_value DESC
LIMIT 20;
