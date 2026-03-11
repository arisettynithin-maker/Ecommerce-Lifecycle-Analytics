"""
retention.py

Purpose
-------
This module calculates cohort retention metrics for the ecommerce
lifecycle analytics project.

It includes:
- cohort month assignment
- activity month creation
- months since cohort calculation
- cohort size
- cohort retention counts
- cohort retention percentages
- retention matrix output

These outputs support retention analysis and dashboarding.
"""

import pandas as pd


# -----------------------------------------------------------
# STEP 1 — Create Activity Month
# -----------------------------------------------------------
# Convert event_time into a monthly activity timestamp.
# This standardizes events into month buckets so we can
# perform cohort-based analysis.


def add_activity_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create activity_month from event_time.
    """

    df = df.copy()
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df["activity_month"] = df["event_time"].dt.to_period("M").dt.to_timestamp()

    return df


# -----------------------------------------------------------
# STEP 2 — Assign Cohort Month
# -----------------------------------------------------------
# Each user is assigned to the month of their first activity.
# This becomes the user's "cohort_month" and represents when
# the user first entered the platform.


def add_cohort_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign each user to their first activity month.
    """

    df = df.copy()

    first_activity = (
        df.groupby("user_id")["activity_month"]
        .min()
        .rename("cohort_month")
    )

    df = df.merge(first_activity, on="user_id", how="left")

    return df


# -----------------------------------------------------------
# STEP 3 — Calculate Months Since Cohort
# -----------------------------------------------------------
# This calculates how many months have passed since the user
# first joined the platform. It allows us to measure retention
# across time (Month 0, Month 1, Month 2, etc).


def calculate_months_since_cohort(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the number of months between activity_month and cohort_month.
    """

    df = df.copy()

    df["months_since_cohort"] = (
        (df["activity_month"].dt.year - df["cohort_month"].dt.year) * 12
        + (df["activity_month"].dt.month - df["cohort_month"].dt.month)
    )

    return df


# -----------------------------------------------------------
# STEP 4 — Prepare Cohort Dataset
# -----------------------------------------------------------
# This pipeline prepares the dataset required for retention
# analysis by executing the full transformation sequence.


def prepare_cohort_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full preprocessing pipeline for cohort retention analysis.
    """

    df = add_activity_month(df)
    df = add_cohort_month(df)
    df = calculate_months_since_cohort(df)

    return df


# -----------------------------------------------------------
# STEP 5 — Calculate Cohort Sizes
# -----------------------------------------------------------
# Cohort size represents the number of unique users who first
# appeared in each cohort_month. This forms the denominator
# for retention rate calculations.


def cohort_size(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate number of unique users in each cohort.
    """

    cohort_sizes = (
        df.groupby("cohort_month")["user_id"]
        .nunique()
        .reset_index()
    )
    cohort_sizes.columns = ["cohort_month", "cohort_size"]

    return cohort_sizes.sort_values("cohort_month")


# -----------------------------------------------------------
# STEP 6 — Calculate Retention Counts
# -----------------------------------------------------------
# This calculates how many users from each cohort remain active
# in subsequent months.


def cohort_retention_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count active users for each cohort and months_since_cohort.
    """

    retention_counts = (
        df.groupby(["cohort_month", "months_since_cohort"])["user_id"]
        .nunique()
        .reset_index()
    )
    retention_counts.columns = ["cohort_month", "months_since_cohort", "active_users"]

    return retention_counts.sort_values(["cohort_month", "months_since_cohort"])


# -----------------------------------------------------------
# STEP 7 — Calculate Retention Rates
# -----------------------------------------------------------
# Retention rate is calculated by dividing active users by
# the original cohort size.


def cohort_retention_rates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate retention percentages for each cohort/month.
    """

    counts = cohort_retention_counts(df)
    sizes = cohort_size(df)

    retention = counts.merge(sizes, on="cohort_month", how="left")

    retention["retention_rate_pct"] = (
        retention["active_users"] / retention["cohort_size"] * 100
    ).round(2)

    return retention.sort_values(["cohort_month", "months_since_cohort"])


# -----------------------------------------------------------
# STEP 8 — Build Retention Matrix (Percentages)
# -----------------------------------------------------------
# This reshapes the retention table into a cohort matrix.
# Rows represent cohorts and columns represent months since
# cohort creation.


def build_retention_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a cohort retention matrix with percentages.
    Rows = cohort_month
    Columns = months_since_cohort
    """

    retention = cohort_retention_rates(df)

    retention_matrix = retention.pivot(
        index="cohort_month",
        columns="months_since_cohort",
        values="retention_rate_pct"
    )

    return retention_matrix.round(2)


# -----------------------------------------------------------
# STEP 9 — Build Retention Matrix (User Counts)
# -----------------------------------------------------------
# Similar to the percentage matrix but shows the actual number
# of active users in each cohort/month.


def build_retention_count_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a cohort retention matrix with active user counts.
    Rows = cohort_month
    Columns = months_since_cohort
    """

    counts = cohort_retention_counts(df)

    count_matrix = counts.pivot(
        index="cohort_month",
        columns="months_since_cohort",
        values="active_users"
    )

    return count_matrix


# -----------------------------------------------------------
# STEP 10 — Generate All Retention Outputs
# -----------------------------------------------------------
# This function orchestrates the full retention analysis
# pipeline and returns all outputs required for reporting
# and dashboarding.


def create_retention_outputs(df: pd.DataFrame) -> dict:
    """
    Generate all retention-related outputs in one function.

    Returns
    -------
    dict
        Dictionary of retention tables.
    """

    cohort_df = prepare_cohort_data(df)

    outputs = {
        "cohort_dataset": cohort_df,
        "cohort_size": cohort_size(cohort_df),
        "cohort_retention_counts": cohort_retention_counts(cohort_df),
        "cohort_retention_rates": cohort_retention_rates(cohort_df),
        "retention_matrix": build_retention_matrix(cohort_df),
        "retention_count_matrix": build_retention_count_matrix(cohort_df),
    }

    return outputs


# -----------------------------------------------------------
# STEP 11 — Save Outputs
# -----------------------------------------------------------
# Export retention tables as CSV files so they can be used
# in Power BI dashboards, SQL validation, or reporting layers.


def save_retention_outputs(outputs: dict, output_dir: str) -> None:
    """
    Save retention outputs to CSV files.
    """

    for name, table in outputs.items():
        table.to_csv(f"{output_dir}/{name}.csv", index=True)