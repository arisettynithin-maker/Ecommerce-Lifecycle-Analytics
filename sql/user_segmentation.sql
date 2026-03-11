/* ============================================================
   File: user_segmentation.sql
   Project: E-commerce Lifecycle Analytics

   Purpose
   -------
   This script builds behavioral customer segments based on
   user purchase activity, engagement level, and spending.

   The goal is to categorize users into actionable groups for
   marketing, retention, and personalization strategies.

   Segments Created
   ----------------
   1. High Value Customers
   2. Frequent Buyers
   3. Occasional Buyers
   4. Browsers / Low Engagement Users

   Source Table
   ------------
   ecommerce_cleaned

   Required Columns
   ----------------
   user_id
   user_session
   event_type
   price
   ============================================================ */


/* ============================================================
   1. USER BEHAVIOR AGGREGATION
   ------------------------------------------------------------
   Aggregate key behavioral metrics for each user.
   These metrics form the foundation for segmentation.
   ============================================================ */

WITH user_behavior AS (

    SELECT
        user_id,

        COUNT(DISTINCT user_session) AS total_sessions,

        COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS total_views,

        COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS total_carts,

        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases,

        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue

    FROM ecommerce_cleaned

    GROUP BY user_id
)

SELECT *
FROM user_behavior
ORDER BY total_revenue DESC;



/* ============================================================
   2. DERIVED USER METRICS
   ------------------------------------------------------------
   Calculate additional behavioral metrics such as
   Average Order Value (AOV).
   ============================================================ */

WITH user_behavior AS (

    SELECT
        user_id,
        COUNT(DISTINCT user_session) AS total_sessions,
        COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS total_views,
        COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS total_carts,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases,
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue
    FROM ecommerce_cleaned
    GROUP BY user_id
)

SELECT
    user_id,
    total_sessions,
    total_views,
    total_carts,
    total_purchases,
    total_revenue,

    CASE
        WHEN total_purchases > 0
        THEN ROUND(total_revenue * 1.0 / total_purchases, 2)
        ELSE 0
    END AS average_order_value

FROM user_behavior;



/* ============================================================
   3. CUSTOMER SEGMENTATION LOGIC
   ------------------------------------------------------------
   Assign each user to a segment based on purchasing behavior.
   This segmentation helps businesses tailor marketing
   strategies and improve customer retention.
   ============================================================ */

WITH user_behavior AS (

    SELECT
        user_id,
        COUNT(DISTINCT user_session) AS total_sessions,
        COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS total_views,
        COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS total_carts,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases,
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue
    FROM ecommerce_cleaned
    GROUP BY user_id
),

user_metrics AS (

    SELECT
        user_id,
        total_sessions,
        total_views,
        total_carts,
        total_purchases,
        total_revenue,

        CASE
            WHEN total_purchases > 0
            THEN total_revenue * 1.0 / total_purchases
            ELSE 0
        END AS avg_order_value

    FROM user_behavior
)

SELECT
    user_id,
    total_sessions,
    total_views,
    total_carts,
    total_purchases,
    total_revenue,
    ROUND(avg_order_value,2) AS avg_order_value,

    CASE
        WHEN total_purchases >= 5 AND total_revenue >= 500
            THEN 'High Value Customer'

        WHEN total_purchases >= 3
            THEN 'Frequent Buyer'

        WHEN total_purchases BETWEEN 1 AND 2
            THEN 'Occasional Buyer'

        WHEN total_views > 0 AND total_purchases = 0
            THEN 'Browser'

        ELSE 'Low Engagement'
    END AS customer_segment

FROM user_metrics

ORDER BY total_revenue DESC;



/* ============================================================
   4. SEGMENT DISTRIBUTION
   ------------------------------------------------------------
   Count how many users belong to each segment.
   This provides a high-level view of the customer base.
   ============================================================ */

WITH segmented_users AS (

    SELECT
        user_id,

        CASE
            WHEN COUNT(CASE WHEN event_type='purchase' THEN 1 END) >= 5
                THEN 'High Value Customer'

            WHEN COUNT(CASE WHEN event_type='purchase' THEN 1 END) >= 3
                THEN 'Frequent Buyer'

            WHEN COUNT(CASE WHEN event_type='purchase' THEN 1 END) BETWEEN 1 AND 2
                THEN 'Occasional Buyer'

            WHEN COUNT(CASE WHEN event_type='view' THEN 1 END) > 0
                 AND COUNT(CASE WHEN event_type='purchase' THEN 1 END) = 0
                THEN 'Browser'

            ELSE 'Low Engagement'
        END AS customer_segment

    FROM ecommerce_cleaned

    GROUP BY user_id
)

SELECT
    customer_segment,
    COUNT(*) AS user_count
FROM segmented_users
GROUP BY customer_segment
ORDER BY user_count DESC;



/* ============================================================
   5. REVENUE CONTRIBUTION BY SEGMENT
   ------------------------------------------------------------
   Identify which customer segments contribute the most
   revenue to the business.
   ============================================================ */

WITH user_revenue AS (

    SELECT
        user_id,
        SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END) AS revenue,
        COUNT(CASE WHEN event_type='purchase' THEN 1 END) AS purchases
    FROM ecommerce_cleaned
    GROUP BY user_id
),

segmented_users AS (

    SELECT
        user_id,
        revenue,

        CASE
            WHEN purchases >= 5 AND revenue >= 500
                THEN 'High Value Customer'
            WHEN purchases >= 3
                THEN 'Frequent Buyer'
            WHEN purchases BETWEEN 1 AND 2
                THEN 'Occasional Buyer'
            ELSE 'Low Value / Browser'
        END AS customer_segment

    FROM user_revenue
)

SELECT
    customer_segment,
    COUNT(*) AS users,
    SUM(revenue) AS total_revenue,
    ROUND(AVG(revenue),2) AS avg_user_revenue
FROM segmented_users
GROUP BY customer_segment
ORDER BY total_revenue DESC;