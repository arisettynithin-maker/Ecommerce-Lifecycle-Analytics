"""
feature_engineering.py

Purpose
-------
This module creates derived features used across the ecommerce
lifecycle analytics project.

It includes:
- category extraction
- price band creation
- session-level summaries
- user-level summaries

These engineered features support:
- funnel analysis
- retention analysis
- customer segmentation
- revenue insights
"""

import pandas as pd
import numpy as np


def create_category_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract main and sub category from category_code.
    """

    df = df.copy()

    if "category_code" in df.columns:
        df["main_category"] = df["category_code"].str.split(".").str[0]
        df["sub_category"] = df["category_code"].str.split(".").str[1]

        df["main_category"] = df["main_category"].fillna("unknown")
        df["sub_category"] = df["sub_category"].fillna("unknown")

    return df


def create_price_band(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create price bands for product price segmentation.
    """

    df = df.copy()

    bins = [-np.inf, 50, 100, 200, 500, np.inf]
    labels = ["Low", "Lower-Mid", "Mid", "Upper-Mid", "High"]

    df["price_band"] = pd.cut(
        df["price"],
        bins=bins,
        labels=labels
    )

    df["price_band"] = df["price_band"].astype("string")

    return df


def create_event_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create binary flags for funnel event types.
    """

    df = df.copy()

    df["is_view"] = (df["event_type"] == "view").astype(int)
    df["is_cart"] = (df["event_type"] == "cart").astype(int)
    df["is_purchase"] = (df["event_type"] == "purchase").astype(int)

    return df


def build_session_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create session-level summary metrics.
    """

    session_summary = (
        df.groupby("user_session")
        .agg(
            user_id=("user_id", "first"),
            session_start=("event_time", "min"),
            session_end=("event_time", "max"),
            total_events=("event_type", "count"),
            total_views=("is_view", "sum"),
            total_carts=("is_cart", "sum"),
            total_purchases=("is_purchase", "sum"),
            total_revenue=(
                "price",
                lambda x: x[df.loc[x.index, "event_type"] == "purchase"].sum()
            ),
        )
        .reset_index()
    )

    session_summary["session_duration_minutes"] = (
        (session_summary["session_end"] - session_summary["session_start"])
        .dt.total_seconds()
        / 60
    ).round(2)

    return session_summary


def build_user_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create user-level summary metrics.
    """

    user_summary = (
        df.groupby("user_id")
        .agg(
            total_sessions=("user_session", "nunique"),
            total_events=("event_type", "count"),
            total_views=("is_view", "sum"),
            total_carts=("is_cart", "sum"),
            total_purchases=("is_purchase", "sum"),
            total_revenue=(
                "price",
                lambda x: x[df.loc[x.index, "event_type"] == "purchase"].sum()
            ),
            first_event_time=("event_time", "min"),
            last_event_time=("event_time", "max"),
        )
        .reset_index()
    )

    user_summary["avg_revenue_per_purchase"] = np.where(
        user_summary["total_purchases"] > 0,
        user_summary["total_revenue"] / user_summary["total_purchases"],
        0
    ).round(2)

    return user_summary


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full feature engineering pipeline.
    """

    df = df.copy()

    df = create_category_features(df)
    df = create_price_band(df)
    df = create_event_flags(df)

    return df


def create_analytics_tables(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Create engineered event-level data plus session and user summaries.

    Returns
    -------
    tuple
        (engineered_df, session_summary, user_summary)
    """

    engineered_df = engineer_features(df)
    session_summary = build_session_summary(engineered_df)
    user_summary = build_user_summary(engineered_df)

    return engineered_df, session_summary, user_summary


def save_feature_outputs(
    engineered_df: pd.DataFrame,
    session_summary: pd.DataFrame,
    user_summary: pd.DataFrame,
    engineered_path: str,
    session_path: str,
    user_path: str
) -> None:
    """
    Save engineered outputs to CSV files.
    """

    engineered_df.to_csv(engineered_path, index=False)
    session_summary.to_csv(session_path, index=False)
    user_summary.to_csv(user_path, index=False)