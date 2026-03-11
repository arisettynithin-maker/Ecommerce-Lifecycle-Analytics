"""
utils.py

Purpose
-------
This module contains reusable helper functions for the ecommerce
lifecycle analytics project.

These utilities support:
- basic dataset validation
- datetime conversion
- missing value checks
- duplicate checks
- event flag creation
- safe percentage calculations
- CSV export

The goal is to keep repeated logic out of the main analysis modules
and make the project more modular and maintainable.
"""

import os
import pandas as pd
import numpy as np


# -----------------------------------------------------------
# STEP 1 — Validate Required Columns
# -----------------------------------------------------------
# This function checks whether the dataset contains all
# required columns before downstream analysis begins.
# It helps prevent runtime errors and makes debugging easier.


def validate_columns(df: pd.DataFrame, required_columns: list) -> None:
    """
    Validate that all required columns exist in the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    required_columns : list
        List of required column names.

    Raises
    ------
    ValueError
        If one or more required columns are missing.
    """

    missing_cols = [col for col in required_columns if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")


# -----------------------------------------------------------
# STEP 2 — Convert Event Time to Datetime
# -----------------------------------------------------------
# Standardizes the event_time column into pandas datetime
# format so that time-based analysis can be performed
# consistently across modules.


def convert_event_time(df: pd.DataFrame, column: str = "event_time") -> pd.DataFrame:
    """
    Convert a datetime column to pandas datetime format.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str, default='event_time'
        Name of the datetime column.

    Returns
    -------
    pd.DataFrame
        DataFrame with converted datetime column.
    """

    df = df.copy()
    df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


# -----------------------------------------------------------
# STEP 3 — Missing Value Summary
# -----------------------------------------------------------
# Provides a quick audit of null values in each column.
# This is useful during the data cleaning stage and when
# assessing data quality before analysis.


def missing_value_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a summary of missing values by column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        Table showing missing count and missing percentage.
    """

    summary = pd.DataFrame({
        "column_name": df.columns,
        "missing_count": df.isna().sum().values,
        "missing_pct": (df.isna().mean().values * 100).round(2)
    })

    return summary.sort_values("missing_count", ascending=False).reset_index(drop=True)


# -----------------------------------------------------------
# STEP 4 — Duplicate Summary
# -----------------------------------------------------------
# Helps identify duplicate rows in the dataset.
# This is especially useful when validating raw ecommerce
# event-level data before aggregation.


def duplicate_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a summary of duplicate rows.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        Single-row summary table of duplicate statistics.
    """

    total_rows = len(df)
    duplicate_rows = df.duplicated().sum()
    duplicate_pct = round((duplicate_rows / total_rows) * 100, 2) if total_rows > 0 else 0

    summary = pd.DataFrame({
        "total_rows": [total_rows],
        "duplicate_rows": [duplicate_rows],
        "duplicate_pct": [duplicate_pct]
    })

    return summary


# -----------------------------------------------------------
# STEP 5 — Add Event Flags
# -----------------------------------------------------------
# Creates binary indicator columns for key event types.
# These flags make downstream aggregation much easier in
# funnel analysis and customer segmentation.


def add_event_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create binary flags for view, cart, and purchase events.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing event_type.

    Returns
    -------
    pd.DataFrame
        DataFrame with is_view, is_cart, and is_purchase columns.
    """

    df = df.copy()

    validate_columns(df, ["event_type"])

    df["is_view"] = np.where(df["event_type"] == "view", 1, 0)
    df["is_cart"] = np.where(df["event_type"] == "cart", 1, 0)
    df["is_purchase"] = np.where(df["event_type"] == "purchase", 1, 0)

    return df


# -----------------------------------------------------------
# STEP 6 — Safe Percentage Calculation
# -----------------------------------------------------------
# This utility avoids division-by-zero errors when calculating
# ratios, conversion rates, and retention metrics.


def safe_divide(numerator, denominator, multiplier: float = 1.0, round_digits: int = 2):
    """
    Safely divide two values and optionally scale the result.

    Parameters
    ----------
    numerator : numeric
        Numerator value.
    denominator : numeric
        Denominator value.
    multiplier : float, default=1.0
        Multiply final result by this value (e.g. 100 for percentages).
    round_digits : int, default=2
        Number of decimal places.

    Returns
    -------
    float
        Safe division result.
    """

    if denominator == 0 or pd.isna(denominator):
        return 0.0

    return round((numerator / denominator) * multiplier, round_digits)


# -----------------------------------------------------------
# STEP 7 — Create Category Split Columns
# -----------------------------------------------------------
# Splits category_code into higher-level product hierarchy.
# This is helpful for category-level analysis in dashboards
# and business recommendations.


def split_category_code(df: pd.DataFrame, column: str = "category_code") -> pd.DataFrame:
    """
    Split category_code into main_category and sub_category.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str, default='category_code'
        Column to split.

    Returns
    -------
    pd.DataFrame
        DataFrame with main_category and sub_category columns.
    """

    df = df.copy()

    validate_columns(df, [column])

    df[column] = df[column].astype("string")
    df["main_category"] = df[column].str.split(".").str[0].fillna("unknown")
    df["sub_category"] = df[column].str.split(".").str[1].fillna("unknown")

    return df


# -----------------------------------------------------------
# STEP 8 — Standard Data Cleaning Helper
# -----------------------------------------------------------
# A lightweight helper for repeated cleaning operations such as
# trimming text fields and normalizing values to lowercase.


def clean_text_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Strip whitespace and convert selected text columns to lowercase.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    columns : list
        List of text columns to clean.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame.
    """

    df = df.copy()

    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip().str.lower()

    return df


# -----------------------------------------------------------
# STEP 9 — Save a Single DataFrame to CSV
# -----------------------------------------------------------
# Standard export helper for individual outputs.


def save_csv(df: pd.DataFrame, output_path: str, index: bool = False) -> None:
    """
    Save a DataFrame to CSV.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to save.
    output_path : str
        Full output file path.
    index : bool, default=False
        Whether to save the index.
    """

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=index)


# -----------------------------------------------------------
# STEP 10 — Save Multiple Output Tables
# -----------------------------------------------------------
# Saves a dictionary of DataFrames into a target folder.
# Useful for modules that generate several analytics outputs
# at once.


def save_output_dict(outputs: dict, output_dir: str, index: bool = False) -> None:
    """
    Save multiple DataFrames stored in a dictionary to CSV files.

    Parameters
    ----------
    outputs : dict
        Dictionary where keys are output names and values are DataFrames.
    output_dir : str
        Folder path to save outputs.
    index : bool, default=False
        Whether to include index in CSV export.
    """

    os.makedirs(output_dir, exist_ok=True)

    for name, table in outputs.items():
        table.to_csv(f"{output_dir}/{name}.csv", index=index)