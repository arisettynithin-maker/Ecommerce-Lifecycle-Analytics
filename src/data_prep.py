"""
data_prep.py

Purpose
-------
This module handles loading, cleaning, and preparing the raw
ecommerce dataset for downstream analytics.

It performs the following steps:
- Load raw CSV data
- Standardize data types
- Clean categorical columns
- Convert price to numeric
- Extract category hierarchy
"""

import pandas as pd


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load raw ecommerce dataset.

    Parameters
    ----------
    file_path : str
        Path to the raw CSV file

    Returns
    -------
    DataFrame
        Loaded dataset
    """

    df = pd.read_csv(file_path)

    return df


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize categorical columns by trimming whitespace
    and converting text to lowercase.
    """

    text_cols = ["event_type", "category_code", "brand"]

    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()

    return df


def convert_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert important columns to correct datatypes.
    """

    id_cols = ["product_id", "category_id", "user_id", "user_session"]

    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df["event_time"] = pd.to_datetime(df["event_time"])

    return df


def extract_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split category_code into main and sub categories.
    """

    df["main_category"] = df["category_code"].str.split(".").str[0]
    df["sub_category"] = df["category_code"].str.split(".").str[1]

    df["main_category"] = df["main_category"].fillna("unknown")
    df["sub_category"] = df["sub_category"].fillna("unknown")

    return df


def clean_dataset(file_path: str) -> pd.DataFrame:
    """
    Complete data preparation pipeline.
    """

    df = load_data(file_path)

    df = standardize_columns(df)

    df = convert_dtypes(df)

    df = extract_categories(df)

    return df


def save_clean_data(df: pd.DataFrame, output_path: str):
    """
    Save processed dataset to disk.
    """

    df.to_csv(output_path, index=False)