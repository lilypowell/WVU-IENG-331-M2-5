--Business Question #3: How quickly do customers place a second order after their first purchase?
--This query will identify first purchase dates and check whether a customer returns within 30, 60, or 90 days.

--Identify customer order
WITH customer_orders AS (
	SELECT 
		c.customer_unique_id,
		o.order_id,
		DATE(o.order_purchase_timestamp) AS order_date
	FROM orders as o
	JOIN customers as c
		ON o.customer_id = c.customer_id
	WHERE o.order_purchase_timestamp IS NOT NULL
),

--Add first purchase date CTE
first_orders AS (
	SELECT 
		customer_unique_id,
		MIN(order_date) AS first_order_date
	FROM customer_orders
	GROUP BY customer_unique_id
),

--Add repeat orders
repeat_orders AS (
	SELECT 
		co.customer_unique_id,
		fo.first_order_date,
		co.order_date,
		DATE_DIFF('day', fo.first_order_date, co.order_date) AS days_since_first_order
	FROM customer_orders as co
	JOIN first_orders as fo
		ON co.customer_unique_id = fo.customer_unique_id
	WHERE co.order_date > fo.first_order_date
),

--Create one row per customer and marks when order repeated
retention_flags AS (
    SELECT
        fo.customer_unique_id,
        fo.first_order_date,
        CASE
            WHEN MIN(ro.days_since_first_order) <= 30 THEN 'Retained 30 Days'
            WHEN MIN(ro.days_since_first_order) <= 60 THEN 'Retained 60 Days'
            WHEN MIN(ro.days_since_first_order) <= 90 THEN 'Retained 90 Days'
            WHEN MIN(ro.days_since_first_order) > 90 THEN 'Retained After 90 Days'
            ELSE 'Not Retained'
        END AS retention_flag
    FROM first_orders AS fo
    LEFT JOIN repeat_orders AS ro
        ON fo.customer_unique_id = ro.customer_unique_id
    GROUP BY fo.customer_unique_id, fo.first_order_date
)

SELECT 
retention_flag,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_customers
FROM retention_flags
GROUP BY retention_flag
ORDER BY
    CASE retention_flag
        WHEN 'Retained 30 Days' THEN 1
        WHEN 'Retained 60 Days' THEN 2
        WHEN 'Retained 90 Days' THEN 3
        WHEN 'Retained After 90 Days' THEN 4
        WHEN 'Not Retained' THEN 5
    END;
