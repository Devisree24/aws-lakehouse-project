-- 8.1 Daily revenue (uses raw orders: order_ts, qty, unit_price)
-- Database: lakehouse_db

SELECT
  CAST(date_parse(SUBSTR(order_ts, 1, 10), '%Y-%m-%d') AS DATE) AS order_date,
  SUM(qty * unit_price) AS daily_revenue
FROM orders
WHERE order_ts IS NOT NULL AND order_ts != ''
GROUP BY 1
ORDER BY 1;
