"""
funnel.py

Purpose
-------
This module calculates funnel metrics for the ecommerce
lifecycle analytics project.

It includes:
- event distribution
- user-level funnel
- session-level funnel
- conversion rates
- category-level funnel
- price-band funnel
- cart abandonment metrics

These outputs support both notebook analysis and dashboarding.
"""

import pandas as pd
import numpy as np


def event_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate count and percentage share of each event type.
    """

    event_summary = (
        df["event_type"]
        .value_counts(dropna=False)
        .reset_index()
    )
    event_summary.columns = ["event_type", "event_count"]

    event_summary["event_pct"] = (
        event_summary["event_count"] / event_summary["event_count"].sum() * 100
    ).round(2)

    return event_summary


def build_user_funnel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create user-level funnel flags.
    """

    user_funnel = (
        df.groupby("user_id")
        .agg(
            viewed=("is_view", "max"),
            carted=("is_cart", "max"),
            purchased=("is_purchase", "max")
        )
        .reset_index()
    )

    return user_funnel


def summarize_user_funnel(user_funnel: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize user-level funnel stage counts and percentages.
    """

    summary = pd.DataFrame({
        "stage": ["view", "cart", "purchase"],
        "users": [
            user_funnel["viewed"].sum(),
            user_funnel["carted"].sum(),
            user_funnel["purchased"].sum()
        ]
    })

    view_users = summary.loc[summary["stage"] == "view", "users"].iloc[0]

    summary["pct_of_view_users"] = np.where(
        view_users > 0,
        (summary["users"] / view_users * 100).round(2),
        0
    )

    return summary


def calculate_user_conversion_metrics(user_funnel: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate user-level funnel conversion rates.
    """

    user_views = user_funnel["viewed"].sum()
    user_carts = user_funnel["carted"].sum()
    user_purchases = user_funnel["purchased"].sum()

    metrics = pd.DataFrame({
        "metric": [
            "user_views",
            "user_carts",
            "user_purchases",
            "user_view_to_cart",
            "user_cart_to_purchase",
            "user_view_to_purchase"
        ],
        "value": [
            user_views,
            user_carts,
            user_purchases,
            round(user_carts / user_views, 4) if user_views > 0 else 0,
            round(user_purchases / user_carts, 4) if user_carts > 0 else 0,
            round(user_purchases / user_views, 4) if user_views > 0 else 0
        ]
    })

    return metrics


def build_session_funnel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create session-level funnel flags.
    """

    session_funnel = (
        df.groupby("user_session")
        .agg(
            user_id=("user_id", "first"),
            viewed=("is_view", "max"),
            carted=("is_cart", "max"),
            purchased=("is_purchase", "max")
        )
        .reset_index()
    )

    return session_funnel


def summarize_session_funnel(session_funnel: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize session-level funnel stage counts and percentages.
    """

    summary = pd.DataFrame({
        "stage": ["view", "cart", "purchase"],
        "sessions": [
            session_funnel["viewed"].sum(),
            session_funnel["carted"].sum(),
            session_funnel["purchased"].sum()
        ]
    })

    view_sessions = summary.loc[summary["stage"] == "view", "sessions"].iloc[0]

    summary["pct_of_view_sessions"] = np.where(
        view_sessions > 0,
        (summary["sessions"] / view_sessions * 100).round(2),
        0
    )

    return summary


def calculate_session_conversion_metrics(session_funnel: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate session-level funnel conversion rates.
    """

    session_views = session_funnel["viewed"].sum()
    session_carts = session_funnel["carted"].sum()
    session_purchases = session_funnel["purchased"].sum()

    metrics = pd.DataFrame({
        "metric": [
            "session_views",
            "session_carts",
            "session_purchases",
            "session_view_to_cart",
            "session_cart_to_purchase",
            "session_view_to_purchase"
        ],
        "value": [
            session_views,
            session_carts,
            session_purchases,
            round(session_carts / session_views, 4) if session_views > 0 else 0,
            round(session_purchases / session_carts, 4) if session_carts > 0 else 0,
            round(session_purchases / session_views, 4) if session_views > 0 else 0
        ]
    })

    return metrics


def category_funnel_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build user-level funnel summary by main category.
    """

    category_funnel = (
        df.groupby(["main_category", "user_id"])
        .agg(
            viewed=("is_view", "max"),
            carted=("is_cart", "max"),
            purchased=("is_purchase", "max")
        )
        .reset_index()
    )

    summary = (
        category_funnel.groupby("main_category")
        .agg(
            users_viewed=("viewed", "sum"),
            users_carted=("carted", "sum"),
            users_purchased=("purchased", "sum")
        )
        .reset_index()
    )

    summary["view_to_cart_rate"] = np.where(
        summary["users_viewed"] > 0,
        (summary["users_carted"] / summary["users_viewed"]).round(4),
        0
    )

    summary["cart_to_purchase_rate"] = np.where(
        summary["users_carted"] > 0,
        (summary["users_purchased"] / summary["users_carted"]).round(4),
        0
    )

    summary["view_to_purchase_rate"] = np.where(
        summary["users_viewed"] > 0,
        (summary["users_purchased"] / summary["users_viewed"]).round(4),
        0
    )

    return summary.sort_values("view_to_purchase_rate", ascending=False)


def price_band_funnel_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build user-level funnel summary by price band.
    """

    price_funnel = (
        df.groupby(["price_band", "user_id"])
        .agg(
            viewed=("is_view", "max"),
            carted=("is_cart", "max"),
            purchased=("is_purchase", "max")
        )
        .reset_index()
    )

    summary = (
        price_funnel.groupby("price_band")
        .agg(
            users_viewed=("viewed", "sum"),
            users_carted=("carted", "sum"),
            users_purchased=("purchased", "sum")
        )
        .reset_index()
    )

    summary["view_to_cart_rate"] = np.where(
        summary["users_viewed"] > 0,
        (summary["users_carted"] / summary["users_viewed"]).round(4),
        0
    )

    summary["cart_to_purchase_rate"] = np.where(
        summary["users_carted"] > 0,
        (summary["users_purchased"] / summary["users_carted"]).round(4),
        0
    )

    summary["view_to_purchase_rate"] = np.where(
        summary["users_viewed"] > 0,
        (summary["users_purchased"] / summary["users_viewed"]).round(4),
        0
    )

    return summary.sort_values("view_to_purchase_rate", ascending=False)


def cart_abandonment_metrics(
    user_funnel: pd.DataFrame,
    session_funnel: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate cart abandonment at user and session level.
    """

    cart_abandoning_users = (
        ((user_funnel["carted"] == 1) & (user_funnel["purchased"] == 0)).sum()
    )
    all_cart_users = (user_funnel["carted"] == 1).sum()

    cart_abandoning_sessions = (
        ((session_funnel["carted"] == 1) & (session_funnel["purchased"] == 0)).sum()
    )
    all_cart_sessions = (session_funnel["carted"] == 1).sum()

    abandonment = pd.DataFrame({
        "metric": [
            "cart_abandoning_users",
            "all_cart_users",
            "user_cart_abandonment_rate",
            "cart_abandoning_sessions",
            "all_cart_sessions",
            "session_cart_abandonment_rate"
        ],
        "value": [
            cart_abandoning_users,
            all_cart_users,
            round(cart_abandoning_users / all_cart_users, 4) if all_cart_users > 0 else 0,
            cart_abandoning_sessions,
            all_cart_sessions,
            round(cart_abandoning_sessions / all_cart_sessions, 4) if all_cart_sessions > 0 else 0
        ]
    })

    return abandonment


def create_funnel_outputs(df: pd.DataFrame) -> dict:
    """
    Generate all funnel-related outputs in one function.

    Returns
    -------
    dict
        Dictionary of funnel tables and metrics.
    """

    user_funnel = build_user_funnel(df)
    session_funnel = build_session_funnel(df)

    outputs = {
        "event_distribution": event_distribution(df),
        "user_funnel": user_funnel,
        "user_funnel_summary": summarize_user_funnel(user_funnel),
        "user_conversion_metrics": calculate_user_conversion_metrics(user_funnel),
        "session_funnel": session_funnel,
        "session_funnel_summary": summarize_session_funnel(session_funnel),
        "session_conversion_metrics": calculate_session_conversion_metrics(session_funnel),
        "category_funnel_summary": category_funnel_summary(df),
        "price_band_funnel_summary": price_band_funnel_summary(df),
        "cart_abandonment_metrics": cart_abandonment_metrics(user_funnel, session_funnel)
    }

    return outputs


def save_funnel_outputs(outputs: dict, output_dir: str) -> None:
    """
    Save funnel output tables to CSV files.
    """

    for name, table in outputs.items():
        table.to_csv(f"{output_dir}/{name}.csv", index=False)