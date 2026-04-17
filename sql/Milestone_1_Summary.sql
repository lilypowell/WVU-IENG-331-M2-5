-- Project 1 Milestone
-- Objective: Use SQL to explore the dataset and answer business questions related to revenue, delivery performance, and customer behavior.

-- Data Quality Audit:
-- We started by checking the dataset to make sure the contents were usable and made sense.
-- This included counted rows in each table, checking for missing values, and looking at the structure of the data.

-- Query 1: Seller Performance
-- Business Question: Which sellers perform best based on revenue, delivery time, and customer reviews?
-- Our query calculated the total revenue for each seller, measured average delivery time, used review scores to represent customer satisfaction, and ranked sellers based on these factors.

-- Query 2: Delivery Performance
-- Business Question: What percentage of orders are early, on time, or late?
-- Our query compared actual delivery dates to estimated delivery dates, classified each order, and calculated the percentage of orders in each classification.

-- Query 3: Customer Retention
-- Business Question: How quickly do customers place a second order after their first purchase?
-- Our query found each customer's first and next order (if applicable), calculated the days in between these dates, and grouped customers into time ranges (30, 60, 90, 90+ days, or not retained).

-- Query 4: Category Revenue and AOV
-- Business Question: How much revenue does each product category generate and what is the average order value (AOV)?
-- Our query calculated the total revenue for each category, counted the total number of orders in the category, calculated the categories' average order values, and found what percentage of total revenue each category contributes.

