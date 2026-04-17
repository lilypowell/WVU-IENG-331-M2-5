-- Business Question #4: How do product categories contribute to total company revenue, and what is the average order value for each category?
-- This query joins order_items with products and category translation to calculate total revenue and number of orders per category, then computes AOV and each category’s share of total revenue.

SELECT
	-- Translate category name to English
	COALESCE(ct.product_category_name_english, p.product_category_name) AS category_name,
	-- Count total orders
	COUNT(DISTINCT oi.order_id) AS total_orders,
	-- Calculate total revenue for each category
	ROUND(SUM(oi.price), 2) AS total_revenue,
	-- Calculate average order value for each category
	ROUND(SUM(oi.price) / COUNT(DISTINCT oi.order_id), 2) AS average_order_value,
	-- Calculate percentage to determine contribution to total revenue
	ROUND(SUM(oi.price) * 100.0 / SUM(SUM(oi.price)) OVER (), 2) AS percent_of_total_revenue
FROM order_items AS oi
JOIN products AS p
	ON oi.product_id = p.product_id
LEFT JOIN category_translation AS ct
	ON p.product_category_name = ct.product_category_name
GROUP BY COALESCE(ct.product_category_name_english, p.product_category_name)
ORDER BY total_revenue DESC;
