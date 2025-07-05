-- Day 3: SQL Fundamentals
-- Customer analysis queries for Superstore dataset
-- Customer summary with key metrics
SELECT
customer_name,
customer_id,
segment,
region,
COUNT(DISTINCT order_id) as total_orders,
SUM(sales) as total_sales,
SUM(profit) as total_profit,
AVG(sales) as avg_order_value,
MAX(order_date) as last_order_date
FROM superstore
GROUP BY customer_name, customer_id, segment, region
HAVING SUM(sales) > 1000
ORDER BY total_sales DESC
LIMIT 50;
-- Regional performance analysis
SELECT
region,
COUNT(DISTINCT customer_id) as unique_customers,
COUNT(DISTINCT order_id) as total_orders,
SUM(sales) as total_revenue,
AVG(sales) as avg_order_value,
SUM(profit) / SUM(sales) * 100 as profit_margin_pct
FROM superstore
GROUP BY region
ORDER BY total_revenue DESC;
EOF                                                
