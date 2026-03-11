/* ============================================================
   File: revenue_metrics.sql
   Project: E-commerce Lifecycle Analytics

   Purpose
   -------
   This script calculates key revenue performance metrics
   for the ecommerce lifecycle analytics project.

   These queries replicate the revenue insights produced in
   the Python business insights notebook.

   Key Metrics
   -----------
   • Total Revenue
   • Total Purchases
   • Average Order Value
   • Revenue by Category
   • Revenue by Price Band
   • Top Revenue Categories
   • Revenue Distribution

   Source Table
   ------------
   ecommerce_cleaned

   Required Columns
   ----------------
   user_id
   event_type
   price
   main_category
   price_band
   ============================================================ */



/* ============================================================
   1. CORE REVENUE METRICS
   ------------------------------------------------------------
   Calculate the total revenue generated and total purchases.
   These represent the primary KPIs for ecommerce performance.
   ============================================================ */

SELECT

    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases,

    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue,

    ROUND(
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END)
        /
        NULLIF(COUNT(CASE WHEN event_type = 'purchase' THEN 1 END),0),
        2
    ) AS average_order_value

FROM ecommerce_cleaned;



/* ============================================================
   2. REVENUE BY PRODUCT CATEGORY
   ------------------------------------------------------------
   Identify which product categories contribute the most
   revenue to the platform.
   ============================================================ */

SELECT

    main_category,

    COUNT(CASE WHEN event_type='purchase' THEN 1 END) AS purchases,

    SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END) AS revenue,

    ROUND(
        SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END)
        /
        NULLIF(COUNT(CASE WHEN event_type='purchase' THEN 1 END),0),
        2
    ) AS avg_order_value

FROM ecommerce_cleaned

GROUP BY main_category

ORDER BY revenue DESC;



/* ============================================================
   3. REVENUE BY PRICE BAND
   ------------------------------------------------------------
   Analyze purchasing behavior across different price tiers.
   This helps identify which price segments drive the most
   revenue and conversion value.
   ============================================================ */

SELECT

    price_band,

    COUNT(CASE WHEN event_type='purchase' THEN 1 END) AS purchases,

    SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END) AS revenue,

    ROUND(
        SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END)
        /
        NULLIF(COUNT(CASE WHEN event_type='purchase' THEN 1 END),0),
        2
    ) AS avg_order_value

FROM ecommerce_cleaned

GROUP BY price_band

ORDER BY revenue DESC;



/* ============================================================
   4. TOP REVENUE GENERATING CATEGORIES
   ------------------------------------------------------------
   Highlight the categories producing the highest revenue.
   These represent the strongest performing product groups.
   ============================================================ */

SELECT

    main_category,

    SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END) AS total_revenue,

    COUNT(CASE WHEN event_type='purchase' THEN 1 END) AS purchases

FROM ecommerce_cleaned

GROUP BY main_category

ORDER BY total_revenue DESC

LIMIT 10;



/* ============================================================
   5. USER REVENUE CONTRIBUTION
   ------------------------------------------------------------
   Evaluate how revenue is distributed across users.
   Useful for identifying high-value customers.
   ============================================================ */

WITH user_revenue AS (

    SELECT

        user_id,

        COUNT(CASE WHEN event_type='purchase' THEN 1 END) AS purchases,

        SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END) AS revenue

    FROM ecommerce_cleaned

    GROUP BY user_id

)

SELECT

    COUNT(*) AS total_customers,

    AVG(revenue) AS avg_customer_revenue,

    MAX(revenue) AS highest_customer_revenue

FROM user_revenue;



/* ============================================================
   6. DAILY REVENUE TREND
   ------------------------------------------------------------
   Analyze how revenue changes over time.
   This helps detect seasonal patterns or growth trends.
   ============================================================ */

SELECT

    DATE(event_time) AS order_date,

    COUNT(CASE WHEN event_type='purchase' THEN 1 END) AS purchases,

    SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END) AS revenue

FROM ecommerce_cleaned

GROUP BY DATE(event_time)

ORDER BY order_date;



/* ============================================================
   7. REVENUE SHARE BY CATEGORY
   ------------------------------------------------------------
   Measure what percentage of total revenue comes from each
   product category.
   ============================================================ */

WITH category_revenue AS (

    SELECT

        main_category,

        SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END) AS revenue

    FROM ecommerce_cleaned

    GROUP BY main_category

),

total_rev AS (

    SELECT SUM(revenue) AS total_revenue FROM category_revenue

)

SELECT

    cr.main_category,

    cr.revenue,

    ROUND(cr.revenue * 100.0 / tr.total_revenue,2) AS revenue_share_pct

FROM category_revenue cr

CROSS JOIN total_rev tr

ORDER BY revenue DESC;