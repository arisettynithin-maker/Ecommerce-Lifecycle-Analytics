/* ============================================================
   File: funnel_metrics.sql
   Project: E-commerce Lifecycle Analytics
   Purpose:
   This script reproduces the key funnel analysis from the
   Python notebook using SQL.

   Covered sections:
   1. Event distribution
   2. User-level funnel
   3. Session-level funnel
   4. Overall funnel conversion metrics
   5. Category-level funnel
   6. Price-band funnel
   7. Cart abandonment analysis

   Assumptions:
   - Source table name: ecommerce_cleaned
   - Required columns:
       user_id
       user_session
       event_type
       main_category
       price_band

   Notes:
   - This script uses standard SQL-style CASE WHEN logic.
   - If you are using SQLite / PostgreSQL / MySQL, this style
     should work with little or no modification.
   ============================================================ */


/* ============================================================
   1. EVENT DISTRIBUTION
   ------------------------------------------------------------
   Purpose:
   Understand the mix of funnel events in the dataset.
   This shows how much activity is browsing vs carting vs buying.
   ============================================================ */

SELECT
    event_type,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS event_pct
FROM ecommerce_cleaned
GROUP BY event_type
ORDER BY event_count DESC;


/* ============================================================
   2. USER-LEVEL FUNNEL FLAGS
   ------------------------------------------------------------
   Purpose:
   For each user, check whether they performed each funnel stage
   at least once during the analysis period.
   Equivalent to the user_funnel table in the notebook.
   ============================================================ */

WITH user_funnel AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY user_id
)
SELECT *
FROM user_funnel
ORDER BY user_id;


/* ============================================================
   3. USER-LEVEL FUNNEL SUMMARY
   ------------------------------------------------------------
   Purpose:
   Aggregate the number of users at each funnel stage.
   Equivalent to:
   - user_views
   - user_carts
   - user_purchases
   - user_funnel_summary
   ============================================================ */

WITH user_funnel AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY user_id
),
user_stage_counts AS (
    SELECT 'view' AS stage, SUM(viewed) AS users FROM user_funnel
    UNION ALL
    SELECT 'cart' AS stage, SUM(carted) AS users FROM user_funnel
    UNION ALL
    SELECT 'purchase' AS stage, SUM(purchased) AS users FROM user_funnel
),
user_base AS (
    SELECT SUM(viewed) AS total_view_users
    FROM user_funnel
)
SELECT
    usc.stage,
    usc.users,
    ROUND(usc.users * 100.0 / ub.total_view_users, 2) AS pct_of_view_users
FROM user_stage_counts usc
CROSS JOIN user_base ub;


/* ============================================================
   4. USER-LEVEL CONVERSION METRICS
   ------------------------------------------------------------
   Purpose:
   Measure user progression through the funnel.
   These match:
   - user_view_to_cart
   - user_cart_to_purchase
   - user_view_to_purchase
   ============================================================ */

WITH user_funnel AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY user_id
),
user_counts AS (
    SELECT
        SUM(viewed) AS user_views,
        SUM(carted) AS user_carts,
        SUM(purchased) AS user_purchases
    FROM user_funnel
)
SELECT
    user_views,
    user_carts,
    user_purchases,
    ROUND(user_carts * 1.0 / NULLIF(user_views, 0), 4) AS user_view_to_cart,
    ROUND(user_purchases * 1.0 / NULLIF(user_carts, 0), 4) AS user_cart_to_purchase,
    ROUND(user_purchases * 1.0 / NULLIF(user_views, 0), 4) AS user_view_to_purchase
FROM user_counts;


/* ============================================================
   5. SESSION-LEVEL FUNNEL FLAGS
   ------------------------------------------------------------
   Purpose:
   For each session, check whether at least one view, cart, and
   purchase happened.
   Equivalent to the session_funnel table in the notebook.
   ============================================================ */

WITH session_funnel AS (
    SELECT
        user_session,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY user_session
)
SELECT *
FROM session_funnel
ORDER BY user_session;


/* ============================================================
   6. SESSION-LEVEL FUNNEL SUMMARY
   ------------------------------------------------------------
   Purpose:
   Aggregate the number of sessions reaching each stage.
   Equivalent to session_funnel_summary in the notebook.
   ============================================================ */

WITH session_funnel AS (
    SELECT
        user_session,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY user_session
),
session_stage_counts AS (
    SELECT 'view' AS stage, SUM(viewed) AS sessions FROM session_funnel
    UNION ALL
    SELECT 'cart' AS stage, SUM(carted) AS sessions FROM session_funnel
    UNION ALL
    SELECT 'purchase' AS stage, SUM(purchased) AS sessions FROM session_funnel
),
session_base AS (
    SELECT SUM(viewed) AS total_view_sessions
    FROM session_funnel
)
SELECT
    ssc.stage,
    ssc.sessions,
    ROUND(ssc.sessions * 100.0 / sb.total_view_sessions, 2) AS pct_of_view_sessions
FROM session_stage_counts ssc
CROSS JOIN session_base sb;


/* ============================================================
   7. SESSION-LEVEL CONVERSION METRICS
   ------------------------------------------------------------
   Purpose:
   Measure conversion at the session level.
   Equivalent to:
   - session_view_to_cart
   - session_cart_to_purchase
   - session_view_to_purchase
   ============================================================ */

WITH session_funnel AS (
    SELECT
        user_session,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY user_session
),
session_counts AS (
    SELECT
        SUM(viewed) AS session_views,
        SUM(carted) AS session_carts,
        SUM(purchased) AS session_purchases
    FROM session_funnel
)
SELECT
    session_views,
    session_carts,
    session_purchases,
    ROUND(session_carts * 1.0 / NULLIF(session_views, 0), 4) AS session_view_to_cart,
    ROUND(session_purchases * 1.0 / NULLIF(session_carts, 0), 4) AS session_cart_to_purchase,
    ROUND(session_purchases * 1.0 / NULLIF(session_views, 0), 4) AS session_view_to_purchase
FROM session_counts;


/* ============================================================
   8. DROP-OFF / CONVERSION SUMMARY TABLE
   ------------------------------------------------------------
   Purpose:
   Present the six core funnel metrics in one compact result set.
   Equivalent to dropoff_summary in the notebook.
   ============================================================ */

WITH user_funnel AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY user_id
),
session_funnel AS (
    SELECT
        user_session,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY user_session
),
user_counts AS (
    SELECT
        SUM(viewed) AS user_views,
        SUM(carted) AS user_carts,
        SUM(purchased) AS user_purchases
    FROM user_funnel
),
session_counts AS (
    SELECT
        SUM(viewed) AS session_views,
        SUM(carted) AS session_carts,
        SUM(purchased) AS session_purchases
    FROM session_funnel
)
SELECT
    'user_view_to_cart' AS metric,
    ROUND(user_carts * 1.0 / NULLIF(user_views, 0), 4) AS value
FROM user_counts

UNION ALL

SELECT
    'user_cart_to_purchase' AS metric,
    ROUND(user_purchases * 1.0 / NULLIF(user_carts, 0), 4) AS value
FROM user_counts

UNION ALL

SELECT
    'user_view_to_purchase' AS metric,
    ROUND(user_purchases * 1.0 / NULLIF(user_views, 0), 4) AS value
FROM user_counts

UNION ALL

SELECT
    'session_view_to_cart' AS metric,
    ROUND(session_carts * 1.0 / NULLIF(session_views, 0), 4) AS value
FROM session_counts

UNION ALL

SELECT
    'session_cart_to_purchase' AS metric,
    ROUND(session_purchases * 1.0 / NULLIF(session_carts, 0), 4) AS value
FROM session_counts

UNION ALL

SELECT
    'session_view_to_purchase' AS metric,
    ROUND(session_purchases * 1.0 / NULLIF(session_views, 0), 4) AS value
FROM session_counts;


/* ============================================================
   9. CATEGORY-LEVEL USER FUNNEL
   ------------------------------------------------------------
   Purpose:
   Evaluate conversion by main product category.
   This helps identify strong and weak categories.
   Equivalent to category_funnel_summary in the notebook.
   ============================================================ */

WITH category_user_funnel AS (
    SELECT
        main_category,
        user_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY main_category, user_id
)
SELECT
    main_category,
    SUM(viewed) AS users_viewed,
    SUM(carted) AS users_carted,
    SUM(purchased) AS users_purchased,
    ROUND(SUM(carted) * 1.0 / NULLIF(SUM(viewed), 0), 4) AS view_to_cart_rate,
    ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(carted), 0), 4) AS cart_to_purchase_rate,
    ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(viewed), 0), 4) AS view_to_purchase_rate
FROM category_user_funnel
GROUP BY main_category
ORDER BY view_to_purchase_rate DESC;


/* ============================================================
   10. TOP CONVERTING CATEGORIES
   ------------------------------------------------------------
   Purpose:
   Highlight the best-performing categories among those with
   meaningful traffic volume.
   Notebook filter used: users_viewed >= 1000
   ============================================================ */

WITH category_user_funnel AS (
    SELECT
        main_category,
        user_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY main_category, user_id
),
category_funnel_summary AS (
    SELECT
        main_category,
        SUM(viewed) AS users_viewed,
        SUM(carted) AS users_carted,
        SUM(purchased) AS users_purchased,
        ROUND(SUM(carted) * 1.0 / NULLIF(SUM(viewed), 0), 4) AS view_to_cart_rate,
        ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(carted), 0), 4) AS cart_to_purchase_rate,
        ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(viewed), 0), 4) AS view_to_purchase_rate
    FROM category_user_funnel
    GROUP BY main_category
)
SELECT *
FROM category_funnel_summary
WHERE users_viewed >= 1000
ORDER BY view_to_purchase_rate DESC
LIMIT 15;


/* ============================================================
   11. LOWEST CONVERTING CATEGORIES
   ------------------------------------------------------------
   Purpose:
   Surface weak categories that may need pricing, merchandising,
   trust, or UX improvement.
   ============================================================ */

WITH category_user_funnel AS (
    SELECT
        main_category,
        user_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY main_category, user_id
),
category_funnel_summary AS (
    SELECT
        main_category,
        SUM(viewed) AS users_viewed,
        SUM(carted) AS users_carted,
        SUM(purchased) AS users_purchased,
        ROUND(SUM(carted) * 1.0 / NULLIF(SUM(viewed), 0), 4) AS view_to_cart_rate,
        ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(carted), 0), 4) AS cart_to_purchase_rate,
        ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(viewed), 0), 4) AS view_to_purchase_rate
    FROM category_user_funnel
    GROUP BY main_category
)
SELECT *
FROM category_funnel_summary
WHERE users_viewed >= 1000
ORDER BY view_to_purchase_rate ASC
LIMIT 15;


/* ============================================================
   12. PRICE-BAND USER FUNNEL
   ------------------------------------------------------------
   Purpose:
   Compare conversion across product price segments.
   Equivalent to price_band_funnel_summary in the notebook.
   ============================================================ */

WITH price_band_user_funnel AS (
    SELECT
        price_band,
        user_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY price_band, user_id
)
SELECT
    price_band,
    SUM(viewed) AS users_viewed,
    SUM(carted) AS users_carted,
    SUM(purchased) AS users_purchased,
    ROUND(SUM(carted) * 1.0 / NULLIF(SUM(viewed), 0), 4) AS view_to_cart_rate,
    ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(carted), 0), 4) AS cart_to_purchase_rate,
    ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(viewed), 0), 4) AS view_to_purchase_rate
FROM price_band_user_funnel
GROUP BY price_band
ORDER BY view_to_purchase_rate DESC;


/* ============================================================
   13. USER-LEVEL CART ABANDONMENT
   ------------------------------------------------------------
   Purpose:
   Estimate how many users added to cart but never purchased.
   This is used as a proxy for cart abandonment.
   ============================================================ */

WITH user_funnel AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY user_id
)
SELECT
    COUNT(CASE WHEN carted = 1 AND purchased = 0 THEN 1 END) AS cart_abandoners,
    COUNT(CASE WHEN carted = 1 THEN 1 END) AS all_cart_users,
    ROUND(
        COUNT(CASE WHEN carted = 1 AND purchased = 0 THEN 1 END) * 1.0
        / NULLIF(COUNT(CASE WHEN carted = 1 THEN 1 END), 0),
        4
    ) AS cart_abandonment_rate
FROM user_funnel;


/* ============================================================
   14. SESSION-LEVEL CART ABANDONMENT
   ------------------------------------------------------------
   Purpose:
   Estimate session abandonment after add-to-cart activity.
   Equivalent to session cart abandonment calculation
   in the notebook.
   ============================================================ */

WITH session_funnel AS (
    SELECT
        user_session,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM ecommerce_cleaned
    GROUP BY user_session
)
SELECT
    COUNT(CASE WHEN carted = 1 AND purchased = 0 THEN 1 END) AS cart_abandoning_sessions,
    COUNT(CASE WHEN carted = 1 THEN 1 END) AS all_cart_sessions,
    ROUND(
        COUNT(CASE WHEN carted = 1 AND purchased = 0 THEN 1 END) * 1.0
        / NULLIF(COUNT(CASE WHEN carted = 1 THEN 1 END), 0),
        4
    ) AS session_cart_abandonment_rate
FROM session_funnel;