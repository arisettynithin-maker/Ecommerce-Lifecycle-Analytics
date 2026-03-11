/* ============================================================
   File: cohort_retention.sql
   Project: E-commerce Lifecycle Analytics

   Purpose
   -------
   This script calculates customer retention cohorts using
   event data from the ecommerce dataset.

   The analysis measures how many users return in the months
   following their first recorded activity.

   Steps Covered
   -------------
   1. Identify each user's first activity month (cohort month)
   2. Calculate months since cohort start
   3. Count active users per cohort per month
   4. Compute retention percentages
   5. Produce a cohort retention matrix

   Source Table
   ------------
   ecommerce_cleaned

   Required Columns
   ----------------
   user_id
   event_time
   ============================================================ */


/* ============================================================
   1. USER FIRST ACTIVITY (COHORT MONTH)
   ------------------------------------------------------------
   Determine the first time each user appeared in the dataset.
   This becomes the user's cohort month.
   ============================================================ */

WITH user_first_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(event_time)) AS cohort_month
    FROM ecommerce_cleaned
    GROUP BY user_id
)

SELECT *
FROM user_first_activity
ORDER BY cohort_month;



/* ============================================================
   2. USER MONTHLY ACTIVITY
   ------------------------------------------------------------
   Convert event timestamps into monthly activity periods.
   Each row represents a user active during a given month.
   ============================================================ */

WITH user_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', event_time) AS activity_month
    FROM ecommerce_cleaned
)

SELECT *
FROM user_activity
ORDER BY activity_month;



/* ============================================================
   3. COHORT DATASET
   ------------------------------------------------------------
   Join user cohort month with activity months to calculate
   the number of months since the user's first activity.
   ============================================================ */

WITH user_first_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(event_time)) AS cohort_month
    FROM ecommerce_cleaned
    GROUP BY user_id
),

user_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', event_time) AS activity_month
    FROM ecommerce_cleaned
),

cohort_dataset AS (
    SELECT
        ufa.user_id,
        ufa.cohort_month,
        ua.activity_month,
        DATE_PART('month', AGE(ua.activity_month, ufa.cohort_month)) AS months_since_cohort
    FROM user_first_activity ufa
    JOIN user_activity ua
        ON ufa.user_id = ua.user_id
)

SELECT *
FROM cohort_dataset
ORDER BY cohort_month, months_since_cohort;



/* ============================================================
   4. COHORT RETENTION COUNTS
   ------------------------------------------------------------
   Count how many users from each cohort returned in each
   subsequent month.
   ============================================================ */

WITH user_first_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(event_time)) AS cohort_month
    FROM ecommerce_cleaned
    GROUP BY user_id
),

user_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', event_time) AS activity_month
    FROM ecommerce_cleaned
),

cohort_dataset AS (
    SELECT
        ufa.user_id,
        ufa.cohort_month,
        ua.activity_month,
        DATE_PART('month', AGE(ua.activity_month, ufa.cohort_month)) AS months_since_cohort
    FROM user_first_activity ufa
    JOIN user_activity ua
        ON ufa.user_id = ua.user_id
)

SELECT
    cohort_month,
    months_since_cohort,
    COUNT(DISTINCT user_id) AS active_users
FROM cohort_dataset
GROUP BY cohort_month, months_since_cohort
ORDER BY cohort_month, months_since_cohort;



/* ============================================================
   5. COHORT SIZE
   ------------------------------------------------------------
   Determine the number of users in each cohort (month 0).
   ============================================================ */

WITH user_first_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(event_time)) AS cohort_month
    FROM ecommerce_cleaned
    GROUP BY user_id
)

SELECT
    cohort_month,
    COUNT(DISTINCT user_id) AS cohort_size
FROM user_first_activity
GROUP BY cohort_month
ORDER BY cohort_month;



/* ============================================================
   6. RETENTION RATE CALCULATION
   ------------------------------------------------------------
   Calculate retention percentages relative to the cohort size.
   This allows us to see how engagement decays over time.
   ============================================================ */

WITH user_first_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(event_time)) AS cohort_month
    FROM ecommerce_cleaned
    GROUP BY user_id
),

user_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', event_time) AS activity_month
    FROM ecommerce_cleaned
),

cohort_dataset AS (
    SELECT
        ufa.user_id,
        ufa.cohort_month,
        ua.activity_month,
        DATE_PART('month', AGE(ua.activity_month, ufa.cohort_month)) AS months_since_cohort
    FROM user_first_activity ufa
    JOIN user_activity ua
        ON ufa.user_id = ua.user_id
),

cohort_counts AS (
    SELECT
        cohort_month,
        months_since_cohort,
        COUNT(DISTINCT user_id) AS active_users
    FROM cohort_dataset
    GROUP BY cohort_month, months_since_cohort
),

cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT user_id) AS cohort_users
    FROM user_first_activity
    GROUP BY cohort_month
)

SELECT
    cc.cohort_month,
    cc.months_since_cohort,
    cc.active_users,
    cs.cohort_users,
    ROUND(cc.active_users * 100.0 / cs.cohort_users, 2) AS retention_rate_pct
FROM cohort_counts cc
JOIN cohort_size cs
    ON cc.cohort_month = cs.cohort_month
ORDER BY cc.cohort_month, cc.months_since_cohort;