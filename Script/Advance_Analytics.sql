-- Change Over Time Analytics
SELECT EXTRACT(YEAR from order_date) as order_year, 
SUM(sales_amount),
count( DISTINCT customer_key) as total_customers,
sum(quantity) as quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_year
ORDER BY order_year

--cumulative Analytics
--Question : Calculate the total sales per month and runnning total of sales over time

SELECT order_year, 
total_sales,
SUM(total_sales) OVER(ORDER BY  order_year) AS running_total,
AVG(avg_price) OVER(ORDER BY  order_year) AS running_avg
FROM(
SELECT DATE_TRUNC('year', order_date) AS order_year,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL 
GROUP BY order_year) t 

/*Analyze the yearly performance of products by comparing their sales to both the average sales,
performance of the product and the previous year's sales */

WITH yearly_product_sales AS (
SELECT EXTRACT(year from order_date) AS order_year,
product_name, 
SUM(sales_amount) AS total_sales
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
GROUP BY order_year, product_name )

SELECT order_year, product_name, total_sales,
ROUND(AVG(total_sales) OVER(PARTITION BY product_name ,2)) AS avg_sales,
total_sales - ROUND(AVG(total_sales) OVER(PARTITION BY product_name ,2)) AS diff_avg,
CASE WHEN total_sales - ROUND(AVG(total_sales) OVER(PARTITION BY product_name ,2)) > 0 THEN 'Above_Avg'
     WHEN total_sales - ROUND(AVG(total_sales) OVER(PARTITION BY product_name ,2)) < 0 THEN 'Below_Avg'
	 ELSE 'AVG'
END avg_change,
LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) py_sales,
total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS py_diff,
CASE WHEN total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'INCREASING'
     WHEN total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'DECREASING'
	 ELSE 'No Change'
END py_change
FROM yearly_product_sales

-- Part-To-Whole Analysis
/* Analyze how an individual part is performing compared to the overall, allowing us to understand which 
category has the greatest impact on the business */

-- which categories contribute the most to overall sales?

with category_sales AS(
SELECT category, sum(sales_amount) AS total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY category)

SELECT category, total_sales,
SUM(total_sales) OVER() AS overall_sales,
CONCAT(ROUND((total_sales/ SUM(total_sales) OVER())*100,2),'%') AS percentage_of_total
FROM category_sales

-- Data Segmentation 
/* Group the data based on a specific range. segment products into cost ranges and count how many products 
fall into each segment */

WITH products AS (
SELECT product_key, product_name, cost,
CASE WHEN cost < 100 THEN 'Below 100'
     WHEN cost BETWEEN 100 and 500 THEN '100-500'
	 WHEN cost BETWEEN 500 and 1000 THEN '500-1000'
	 ELSE 'Above 1000'
END cost_range
FROM gold.dim_products )

SELECT cost_range, count(product_key) as total_products
FROM products 
GROUP BY cost_range
ORDER BY total_products DESC

/* group customers into three segments based on thier spending behaviour:
-VIP: customers with atleast 12 months of history and spending more than 5000.
-Regular: customers with atleast 12 months of history but spending 5000 or less.
-New: customers with a lifespan less than 12 months.
and find the total number of customers by each group. */

WITH customer_spending AS(
SELECT c.customer_key, SUM(f.sales_amount) AS total_sales,
(EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date)))) *12 +
(EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_Date)))) AS lifespan
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
on f.customer_key = c.customer_key
GROUP BY c.customer_key)

SELECT customer_segment, COUNT(customer_key)
FROM (
SELECT customer_key,
CASE WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
     WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
	 ELSE 'New'
END customer_segment
FROM customer_spending) t 
GROUP BY customer_segment
ORDER BY customer_segment


/* Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend 
*/

--BASE QUERY
CREATE VIEW gold.customer_report AS
WITH base_query AS(
SELECT f.order_number,f.product_key,f.order_date, f.sales_amount, f.quantity, c.customer_number,c.customer_key,
CONCAT(first_name,' ', last_name) AS customer_name,
DATE_PART('year', CURRENT_DATE) - DATE_PART('year', birthdate) AS Age
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c 
ON f.customer_key = c.customer_key
WHERE order_date IS NOT NULL)

-- Customer Aggregations
, customer_segmentation AS (
SELECT customer_number,customer_key,customer_name,age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) as last_order_date,
(EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date)))) *12 +
(EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_Date)))) AS lifespan
FROM base_query
GROUP BY customer_number,customer_key,customer_name,age )

SELECT customer_number,customer_key,customer_name,age,
CASE WHEN age < 20 THEN 'Below 20'
     WHEN age between 20 and 29 THEN '20-29'
	 WHEN age between 30 and 39 THEN '30-39'
	 WHEN age between 40 and 49 THEN '40-49'
	 ELSE '50 and Above'
END as Age_group,
CASE WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
     WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
	 ELSE 'New'
END customer_segment,
total_orders, total_sales, total_quantity, total_products,last_order_date
FROM customer_segmentation

/* Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
*/

WITH base_product AS(
SELECT f.order_number, p.product_key, f.order_date, f.sales_amount, p.product_name,f.quantity,
f.customer_key,
p.category,
p.subcategory,
p.cost
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p 
on f.product_key = p.product_key
WHERE order_date IS NOT NULL)

SELECT 
product_key,
product_name,
category,
subcategory,
cost,
(EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date)))) *12 +
(EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_Date)))) AS lifespan,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT customer_key) AS total_customers,
ROUND(CAST(avg(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)) AS NUMERIC), 2) AS average_sales
FROM base_product
group by 
product_key,
product_name,
category,
subcategory,
cost


































