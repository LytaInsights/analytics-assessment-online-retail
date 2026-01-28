#!/usr/bin/env python
"""
dashboard_app.py

Streamlit dashboard on top of DuckDB (retail.duckdb).
Run with:
    streamlit run dashboard_app.py
"""

import duckdb
import pandas as pd
import streamlit as st
from datetime import date


DUCKDB_PATH = "retail.duckdb"


@st.cache_resource
def get_connection():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def get_date_bounds(con):
    row = con.execute(
        "SELECT MIN(invoice_month), MAX(invoice_month) FROM fct_transactions"
    ).fetchone()
    return row[0], row[1]


def load_retention(con) -> pd.DataFrame:
    return con.execute(
        "SELECT * FROM customer_retention_monthly ORDER BY invoice_month"
    ).df()


def load_aov(con, where_clause: str = "", params=None) -> float:
    params = params or []
    query = f"""
        WITH order_revenue AS (
            SELECT
                invoice_no,
                SUM(line_revenue) AS order_revenue
            FROM fct_transactions
            {where_clause}
            GROUP BY invoice_no
        )
        SELECT
            CASE WHEN COUNT(*) = 0 THEN NULL
                 ELSE SUM(order_revenue) / COUNT(*)
            END AS aov
        FROM order_revenue
    """
    return con.execute(query, params).fetchone()[0]


def load_clv_summary(con, where_clause: str = "", params=None) -> pd.DataFrame:
    params = params or []
    query = f"""
        WITH ord AS (
            SELECT
                invoice_no,
                invoice_month,
                customer_id,
                SUM(line_revenue) AS order_revenue
            FROM fct_transactions
            {where_clause}
            GROUP BY invoice_no, invoice_month, customer_id
        )
        SELECT
            customer_id,
            MIN(invoice_month)                    AS first_order_month,
            MAX(invoice_month)                    AS last_order_month,
            COUNT(DISTINCT invoice_no)            AS order_count,
            SUM(order_revenue)                    AS total_revenue,
            AVG(order_revenue)                    AS avg_order_value
        FROM ord
        GROUP BY customer_id
    """
    return con.execute(query, params).df()


def load_top_products(con, where_clause: str = "", params=None, limit: int = 10) -> pd.DataFrame:
    params = params or []
    query = f"""
        SELECT
            stock_code,
            description,
            SUM(line_revenue) AS total_revenue
        FROM fct_transactions
        {where_clause}
        GROUP BY stock_code, description
        ORDER BY total_revenue DESC
        LIMIT {limit}
    """
    return con.execute(query, params).df()


def load_revenue_by_country(con, where_clause: str = "", params=None) -> pd.DataFrame:
    params = params or []
    query = f"""
        SELECT
            country,
            SUM(line_revenue) AS revenue
        FROM fct_transactions
        {where_clause}
        GROUP BY country
        ORDER BY revenue DESC
    """
    return con.execute(query, params).df()


def main():
    st.title("Online Retail Analytics Dashboard")

    con = get_connection()

    # Sidebar filters
    st.sidebar.header("Filters")

    min_month, max_month = get_date_bounds(con)
    if min_month is None or max_month is None:
        st.error("No data found in fct_transactions. Run pipeline.py first.")
        return

    min_date = date(min_month.year, min_month.month, 1)
    max_date = date(max_month.year, max_month.month, 1)

    date_range = st.sidebar.slider(
        "Invoice month range",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        format="YYYY-MM",
    )

    countries_df = con.execute(
        "SELECT DISTINCT country FROM fct_transactions ORDER BY country"
    ).df()
    country_options = countries_df["country"].tolist()

    selected_countries = st.sidebar.multiselect(
        "Countries",
        options=country_options,
        default=[],
        help="Leave empty to include all countries.",
    )

    # Build WHERE clause + params
    where_clauses = []
    params = []

    start_month = pd.Timestamp(date_range[0])
    end_month = pd.Timestamp(date_range[1]).to_period("M").to_timestamp("M")

    where_clauses.append("invoice_month BETWEEN ? AND ?")
    params.extend([start_month, end_month])

    if selected_countries:
        placeholders = ", ".join(["?"] * len(selected_countries))
        where_clauses.append(f"country IN ({placeholders})")
        params.extend(selected_countries)

    where_clause_sql = ""
    if where_clauses:
        where_clause_sql = "WHERE " + " AND ".join(where_clauses)

    # MRR within filters
    mrr_df = con.execute(
        f"""
        SELECT invoice_month, SUM(line_revenue) AS mrr
        FROM fct_transactions
        {where_clause_sql}
        GROUP BY invoice_month
        ORDER BY invoice_month
        """,
        params,
    ).df()

    # AOV within filters
    aov_value = load_aov(con, where_clause_sql, params)

    # CLV summary within filters (use median as a scalar KPI)
    clv_df = load_clv_summary(con, where_clause_sql, params)
    clv_median = clv_df["total_revenue"].median() if not clv_df.empty else None

    # Top products
    top_products_df = load_top_products(con, where_clause_sql, params)

    # Revenue by country
    rev_country_df = load_revenue_by_country(con, where_clause_sql, params)

    # Retention (global, for stability)
    retention_df = load_retention(con)

    # Current month MRR (last month in filtered range)
    current_mrr = mrr_df["mrr"].iloc[-1] if not mrr_df.empty else None

    # Latest retention
    current_retention = (
        retention_df["retention_rate"].iloc[-1] if not retention_df.empty else None
    )

    # KPI layout
    st.subheader("Key Metrics")

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric(
        "MRR (last month in range)",
        f"£{current_mrr:,.0f}" if current_mrr is not None else "N/A",
    )
    col2.metric(
        "Customer Retention (last available month)",
        f"{current_retention*100:.1f}%" if current_retention is not None else "N/A",
    )
    col3.metric(
        "Average Order Value (AOV)",
        f"£{aov_value:,.2f}" if aov_value is not None else "N/A",
    )
    col4.metric(
        "Median CLV (filtered customers)",
        f"£{clv_median:,.0f}" if clv_median is not None else "N/A",
    )
    top_product_rev = (
        top_products_df["total_revenue"].iloc[0] if not top_products_df.empty else None
    )
    col5.metric(
        "Top Product Revenue (filtered)",
        f"£{top_product_rev:,.0f}" if top_product_rev is not None else "N/A",
    )

    # Time series
    st.subheader("Time Series")

    ts_col1, ts_col2 = st.columns(2)

    with ts_col1:
        st.markdown("**Monthly Revenue (MRR)**")
        if not mrr_df.empty:
            mrr_chart_df = mrr_df.set_index("invoice_month")
            st.line_chart(mrr_chart_df["mrr"])
        else:
            st.info("No data for selected filters.")

    with ts_col2:
        st.markdown("**Customer Retention (global)**")
        if not retention_df.empty:
            ret_chart_df = retention_df.set_index("invoice_month")[["retention_rate"]]
            st.line_chart(ret_chart_df)
        else:
            st.info("Retention data not available.")

    # Top 10 products
    st.subheader("Top 10 Products by Revenue (Filtered)")

    if not top_products_df.empty:
        st.dataframe(top_products_df)
        chart_df = top_products_df.copy()
        chart_df["label"] = (
            chart_df["stock_code"] + " - " + chart_df["description"].str.slice(0, 20)
        )
        chart_df = chart_df.set_index("label")
        st.bar_chart(chart_df["total_revenue"])
    else:
        st.info("No product data for selected filters.")

    # Revenue by country
    st.subheader("Revenue by Country (Filtered)")
    if not rev_country_df.empty:
        st.dataframe(rev_country_df)
    else:
        st.info("No revenue data for selected filters.")

    st.caption(
        "Filters apply to MRR, AOV, CLV, top products, and revenue by country. "
        "Customer retention is shown at the global dataset level for stability."
    )


if __name__ == "__main__":
    main()
