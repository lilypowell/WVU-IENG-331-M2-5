
-- Business Question:
-- How do product categories contribute to total company revenue, and what is the average order value (AOV) for each category?
-- Approach:
-- This query joins order_items, orders, products, and category translation to calculate total revenue, AOV, and percentage of total revenue by category. Optional start and end date filters are applied to order purchase date.


SELECT
    COALESCE(ct.product_category_name_english, p.product_category_name) AS category_name,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    ROUND(SUM(oi.price), 2) AS total_revenue,
    ROUND(SUM(oi.price) / COUNT(DISTINCT oi.order_id), 2) AS average_order_value,
    ROUND(SUM(oi.price) * 100.0 / SUM(SUM(oi.price)) OVER (), 2) AS percent_of_total_revenue
FROM order_items AS oi
JOIN orders AS o
    ON oi.order_id = o.order_id
JOIN products AS p
    ON oi.product_id = p.product_id
LEFT JOIN category_translation AS ct
    ON p.product_category_name = ct.product_category_name
WHERE ($1 IS NULL OR DATE(o.order_purchase_timestamp) >= $1)
  AND ($2 IS NULL OR DATE(o.order_purchase_timestamp) <= $2)
GROUP BY COALESCE(ct.product_category_name_english, p.product_category_name)
ORDER BY total_revenue DESC;
