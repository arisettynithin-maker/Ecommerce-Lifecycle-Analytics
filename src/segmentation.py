"""
segmentation.py

Purpose
-------
This module performs customer segmentation for the ecommerce
lifecycle analytics project.

It aggregates user-level behavioral metrics and assigns each
user to a customer segment based on engagement and purchasing
behavior.

Segments Created
----------------
High Value Customer
Frequent Buyer
Occasional Buyer
Browser
Low Engagement
"""

import pandas as pd
import numpy as np


# -----------------------------------------------------------
# STEP 1 — Build User Behavioral Metrics
# -----------------------------------------------------------
# This step aggregates event-level data into user-level metrics.
# Each row in the output represents a unique user with summary
# behavioral statistics such as:
#
# - number of sessions
# - product views
# - cart additions
# - purchases
# - total revenue generated
#
# These metrics form the foundation for customer segmentation.


def build_user_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate key behavioral metrics for each user.

    Metrics include:
    - total sessions
    - total views
    - total carts
    - total purchases
    - total revenue
    - average order value
    """

    user_metrics = (
        df.groupby("user_id")
        .agg(
            total_sessions=("user_session", "nunique"),
            total_views=("is_view", "sum"),
            total_carts=("is_cart", "sum"),
            total_purchases=("is_purchase", "sum"),
            total_revenue=(
                "price",
                lambda x: x[df.loc[x.index, "event_type"] == "purchase"].sum()
            ),
            first_activity=("event_time", "min"),
            last_activity=("event_time", "max")
        )
        .reset_index()
    )

    # -------------------------------------------------------
    # Calculate Average Order Value (AOV)
    # -------------------------------------------------------
    # AOV is calculated only for users who made purchases.
    # This metric helps identify high-value customers.

    user_metrics["avg_order_value"] = np.where(
        user_metrics["total_purchases"] > 0,
        user_metrics["total_revenue"] / user_metrics["total_purchases"],
        0
    ).round(2)

    return user_metrics


# -----------------------------------------------------------
# STEP 2 — Assign Customer Segments
# -----------------------------------------------------------
# Users are classified into behavioral segments based on
# their purchasing frequency and revenue contribution.
#
# Segmentation logic:
#
# High Value Customer → frequent purchases + high revenue
# Frequent Buyer      → multiple purchases
# Occasional Buyer    → 1–2 purchases
# Browser             → views but no purchases
# Low Engagement      → minimal interaction


def assign_customer_segments(user_metrics: pd.DataFrame) -> pd.DataFrame:
    """
    Assign behavioral segments based on purchase activity
    and revenue contribution.
    """

    df = user_metrics.copy()

    df["customer_segment"] = np.select(
        [
            (df["total_purchases"] >= 5) & (df["total_revenue"] >= 500),
            (df["total_purchases"] >= 3),
            (df["total_purchases"].between(1, 2)),
            (df["total_views"] > 0) & (df["total_purchases"] == 0)
        ],
        [
            "High Value Customer",
            "Frequent Buyer",
            "Occasional Buyer",
            "Browser"
        ],
        default="Low Engagement"
    )

    return df


# -----------------------------------------------------------
# STEP 3 — Segment Distribution Analysis
# -----------------------------------------------------------
# This summarizes how many users fall into each segment.
#
# It helps answer questions such as:
# - What percentage of users are browsers?
# - How many high-value customers exist?


def segment_distribution(segmented_df: pd.DataFrame) -> pd.DataFrame:
    """
    Count number of users in each customer segment.
    """

    distribution = (
        segmented_df.groupby("customer_segment")
        .size()
        .reset_index(name="user_count")
        .sort_values("user_count", ascending=False)
    )

    return distribution


# -----------------------------------------------------------
# STEP 4 — Revenue Contribution by Segment
# -----------------------------------------------------------
# This analysis evaluates the financial importance of
# each customer segment.
#
# Metrics calculated:
# - total users
# - total revenue
# - average revenue per user
# - average order value
#
# This insight is often used for marketing prioritization.


def revenue_by_segment(segmented_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze revenue contribution by customer segment.
    """

    revenue_summary = (
        segmented_df.groupby("customer_segment")
        .agg(
            users=("user_id", "count"),
            total_revenue=("total_revenue", "sum"),
            avg_revenue=("total_revenue", "mean"),
            avg_order_value=("avg_order_value", "mean")
        )
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    revenue_summary["avg_revenue"] = revenue_summary["avg_revenue"].round(2)
    revenue_summary["avg_order_value"] = revenue_summary["avg_order_value"].round(2)

    return revenue_summary


# -----------------------------------------------------------
# STEP 5 — Generate All Segmentation Outputs
# -----------------------------------------------------------
# This function orchestrates the full segmentation workflow
# and returns all relevant tables needed for reporting,
# dashboarding, or further analysis.


def create_segmentation_outputs(df: pd.DataFrame) -> dict:
    """
    Generate all segmentation outputs.

    Returns
    -------
    dict
        Dictionary containing segmentation tables.
    """

    user_metrics = build_user_metrics(df)

    segmented_users = assign_customer_segments(user_metrics)

    outputs = {
        "user_metrics": user_metrics,
        "customer_segments": segmented_users,
        "segment_distribution": segment_distribution(segmented_users),
        "revenue_by_segment": revenue_by_segment(segmented_users)
    }

    return outputs


# -----------------------------------------------------------
# STEP 6 — Save Segmentation Outputs
# -----------------------------------------------------------
# Export segmentation tables as CSV files so they can be
# easily used in Power BI dashboards, SQL validation, or
# reporting pipelines.


def save_segmentation_outputs(outputs: dict, output_dir: str) -> None:
    """
    Save segmentation outputs to CSV files.
    """

    for name, table in outputs.items():
        table.to_csv(f"{output_dir}/{name}.csv", index=False)