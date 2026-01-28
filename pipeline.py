#!/usr/bin/env python
"""
pipeline.py

End-to-end data pipeline for the Online Retail dataset:
- Download/load data
- Clean and validate
- Create transformed tables
- Load into DuckDB warehouse (retail.duckdb)
"""

import os
import duckdb
import pandas as pd


DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "retail.duckdb")


def load_online_retail() -> pd.DataFrame:
    """
    Load the Online Retail dataset directly from the UCI URL using pandas.
    This is the standard Excel file linked from the UCI "Online Retail" dataset.
    """
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"
    print(f"Downloading dataset from {url} ...")
    df = pd.read_excel(url)

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    expected = {
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    }
    missing = expected - set(df.columns)
    if missing:
        print(f"WARNING: Dataset is missing expected columns: {missing}")

    return df


def clean_online_retail(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning & typing:
    - Strip column names
    - Parse dates
    - Cast numeric types
    - Drop rows with missing CustomerID
    - Filter out cancellations, zero/negative quantities and unit prices
    - Compute line_revenue and invoice_month
    """
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    # Parse InvoiceDate
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

    # Strip text fields
    df["InvoiceNo"] = df["InvoiceNo"].astype(str).str.strip()
    df["StockCode"] = df["StockCode"].astype(str).str.strip()
    df["Description"] = df["Description"].astype(str).str.strip()
    df["Country"] = df["Country"].astype(str).str.strip()

    # Numeric types
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")

    # CustomerID
    df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce")

    # Drop obviously invalid rows
    df = df.dropna(
        subset=["InvoiceNo", "InvoiceDate", "StockCode", "Quantity", "UnitPrice"]
    )

    # For CLV/retention we need a robust identifier; drop missing customers
    df = df.dropna(subset=["CustomerID"])
    df["CustomerID"] = df["CustomerID"].astype("int64")

    # Filter out cancellations and negative/zero values
    df["InvoiceNo_upper"] = df["InvoiceNo"].str.upper()
    mask_valid = (
        ~df["InvoiceNo_upper"].str.startswith("C")
        & (df["Quantity"] > 0)
        & (df["UnitPrice"] > 0)
    )
    df = df.loc[mask_valid].copy()
    df.drop(columns=["InvoiceNo_upper"], inplace=True)

    # Compute revenue & month
    df["line_revenue"] = df["Quantity"] * df["UnitPrice"]
    df["invoice_month"] = df["InvoiceDate"].dt.to_period("M").dt.to_timestamp("M")

    return df


def create_duckdb_schema(clean_df: pd.DataFrame, raw_df: pd.DataFrame) -> None:
    """
    Persist raw & cleaned data into DuckDB as analytical tables.
    Tables:
      - raw_online_retail
      - fct_transactions
    """
    con = duckdb.connect(DUCKDB_PATH)

    # Register in-memory DataFrames
    con.register("raw_df", raw_df)
    con.register("clean_df", clean_df)

    # Raw table (light typing, for lineage/debugging)
    con.execute("""
        CREATE OR REPLACE TABLE raw_online_retail AS
        SELECT *
        FROM raw_df
    """)

    # Fact table with consistent types and derived columns
    con.execute("""
        CREATE OR REPLACE TABLE fct_transactions AS
        SELECT
            CAST(InvoiceNo AS VARCHAR)          AS invoice_no,
            CAST(StockCode AS VARCHAR)          AS stock_code,
            CAST(Description AS VARCHAR)        AS description,
            CAST(Quantity AS INTEGER)           AS quantity,
            CAST(InvoiceDate AS TIMESTAMP)      AS invoice_timestamp,
            CAST(invoice_month AS DATE)         AS invoice_month,
            CAST(UnitPrice AS DOUBLE)           AS unit_price,
            CAST(line_revenue AS DOUBLE)        AS line_revenue,
            CAST(CustomerID AS INTEGER)         AS customer_id,
            CAST(Country AS VARCHAR)            AS country
        FROM clean_df
    """)

    con.close()


def main():
    print("Loading Online Retail dataset from UCI...")
    raw_df = load_online_retail()
    print(f"Loaded raw dataset with {len(raw_df):,} rows.")

    print("Cleaning and transforming dataset...")
    clean_df = clean_online_retail(raw_df)
    print(f"Cleaned dataset has {len(clean_df):,} rows after filters.")

    print(f"Creating DuckDB warehouse at {DUCKDB_PATH} ...")
    create_duckdb_schema(clean_df, raw_df)
    print("Done. Tables created:")
    print("  - raw_online_retail")
    print("  - fct_transactions")


if __name__ == "__main__":
    main()
