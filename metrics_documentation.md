# Metrics Documentation

This file documents the 5 required KPIs used in the Online Retail Analytics Assessment.

---

## 1. Monthly Recurring Revenue (MRR)

**Business definition:**  
Total recognized revenue per month. In this dataset (non‑subscription), MRR = monthly revenue.

**SQL logic:**  
See `mrr_monthly` view in `metrics_definitions.sql`.

**Refresh frequency:** Daily or after each pipeline run.  
**Limitations:** Not true subscription MRR; limited to dataset window.

---

## 2. Customer Retention Rate

**Business definition:**  
Percentage of customers who purchased in both the current and previous month.

**SQL logic:**  
Uses `customer_monthly_active` and `customer_retention_monthly` views.

**Refresh frequency:** Daily.  
**Limitations:** First month has no retention baseline; short dataset window.

---

## 3. Average Order Value (AOV)

**Business definition:**  
Total revenue divided by number of orders (unique invoices).

**SQL logic:**  
Views: `order_revenue`, `aov_overall`, `aov_monthly`.

**Refresh frequency:** Daily.  
**Limitations:** Large/wholesale orders may skew averages.

---

## 4. Customer Lifetime Value (CLV)

**Business definition:**  
Historical observed spend per customer across all orders.

**SQL logic:**  
View: `customer_lifetime_value`.

**Refresh frequency:** Daily.  
**Limitations:** Not predictive; dataset covers only one year.

---

## 5. Top Products by Revenue

**Business definition:**  
Products ranked by total revenue.

**SQL logic:**  
View: `top_products_by_revenue`.

**Refresh frequency:** Daily.  
**Limitations:** Product descriptions may be inconsistent; no category grouping.
