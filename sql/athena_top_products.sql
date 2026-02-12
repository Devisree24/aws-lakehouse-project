-- 8.2 Top products by revenue (uses raw orders)
-- Database: lakehouse_db

SELECT
  product_id,
  SUM(qty * unit_price) AS rev
FROM orders
GROUP BY product_id
ORDER BY rev DESC
LIMIT 10;
