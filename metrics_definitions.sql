-- metrics_definitions.sql
-- Semantic layer views on top of fct_transactions in DuckDB

------------------------------------------------------------
-- Helper: order_revenue view (per-invoice revenue)
------------------------------------------------------------

CREATE OR REPLACE VIEW order_revenue AS
SELECT
    invoice_no,
    invoice_month,
    customer_id,
    SUM(line_revenue) AS order_revenue
FROM fct_transactions
GROUP BY invoice_no, invoice_month, customer_id;


------------------------------------------------------------
-- 1. Monthly Recurring Revenue (MRR)
--    Business definition: total revenue per month.
------------------------------------------------------------

CREATE OR REPLACE VIEW mrr_monthly AS
SELECT
    invoice_month,
    SUM(line_revenue) AS mrr
FROM fct_transactions
GROUP BY invoice_month
ORDER BY invoice_month;


------------------------------------------------------------
-- 2. Customer Retention Rate
--    Business definition:
--    For a given month M, retention rate =
--      (# of customers who purchased in BOTH month M-1 and M)
--        / (# of customers who purchased in month M-1)
------------------------------------------------------------

-- Monthly active customers
CREATE OR REPLACE VIEW customer_monthly_active AS
SELECT
    invoice_month,
    COUNT(DISTINCT customer_id) AS active_customers
FROM fct_transactions
GROUP BY invoice_month;

-- Retention by month
CREATE OR REPLACE VIEW customer_retention_monthly AS
WITH month_pairs AS (
    SELECT DISTINCT
        ma.invoice_month AS month,
        -- previous calendar month
        ma.invoice_month - INTERVAL 1 MONTH AS prev_month
    FROM customer_monthly_active ma
),
retained AS (
    SELECT
        mp.month,
        COUNT(DISTINCT curr.customer_id) AS customers_retained
    FROM month_pairs mp
    JOIN fct_transactions curr
      ON curr.invoice_month = mp.month
    JOIN fct_transactions prev
      ON prev.invoice_month = mp.prev_month
     AND prev.customer_id = curr.customer_id
    GROUP BY mp.month
)
SELECT
    mp.month AS invoice_month,
    prev.active_customers  AS customers_previous,
    r.customers_retained,
    CASE
        WHEN prev.active_customers = 0 THEN NULL
        ELSE r.customers_retained::DOUBLE / prev.active_customers
    END AS retention_rate
FROM month_pairs mp
JOIN customer_monthly_active prev
  ON prev.invoice_month = mp.prev_month
JOIN retained r
  ON r.month = mp.month
ORDER BY invoice_month;


------------------------------------------------------------
-- 3. Average Order Value (AOV)
--    Business definition:
--      AOV = total revenue / number of orders
--    (order = distinct invoice_no)
------------------------------------------------------------

-- Overall AOV
CREATE OR REPLACE VIEW aov_overall AS
SELECT
    SUM(order_revenue) / COUNT(*) AS aov
FROM order_revenue;

-- Monthly AOV
CREATE OR REPLACE VIEW aov_monthly AS
SELECT
    invoice_month,
    SUM(order_revenue) / COUNT(*) AS aov
FROM order_revenue
GROUP BY invoice_month
ORDER BY invoice_month;


------------------------------------------------------------
-- 4. Customer Lifetime Value (CLV)
--    Business definition:
--      Total revenue per customer over observed lifetime.
------------------------------------------------------------

CREATE OR REPLACE VIEW customer_lifetime_value AS
WITH ord AS (
    SELECT
        invoice_no,
        invoice_month,
        customer_id,
        SUM(line_revenue) AS order_revenue
    FROM fct_transactions
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
GROUP BY customer_id;


------------------------------------------------------------
-- 5. Top Products by Revenue
--    Business definition:
--      Products ranked by total revenue contribution.
------------------------------------------------------------

CREATE OR REPLACE VIEW top_products_by_revenue AS
SELECT
    stock_code,
    description,
    SUM(line_revenue) AS total_revenue,
    RANK() OVER (ORDER BY SUM(line_revenue) DESC) AS revenue_rank
FROM fct_transactions
GROUP BY stock_code, description
ORDER BY total_revenue DESC;


------------------------------------------------------------
-- 6. Revenue by Country (for dashboard)
------------------------------------------------------------

CREATE OR REPLACE VIEW revenue_by_country_monthly AS
SELECT
    invoice_month,
    country,
    SUM(line_revenue) AS revenue
FROM fct_transactions
GROUP BY invoice_month, country
ORDER BY invoice_month, revenue DESC;
