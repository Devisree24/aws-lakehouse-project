-- 8.4 Region performance (orders + dim_customers)
-- Database: lakehouse_db

SELECT
  c.state,
  SUM(o.qty * o.unit_price) AS rev
FROM orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
GROUP BY c.state
ORDER BY rev DESC;
