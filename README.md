# Analytics Engineer Assessment – Online Retail

This repository contains a complete analytics workflow for the UCI Online Retail dataset.

---

## Project Structure

```
analytics-assessment/
├── pipeline.py
├── metrics_definitions.sql
├── metrics_documentation.md
├── dashboard_app.py
├── analytics_pipeline.ipynb
├── dashboard_link.txt
└── README.md
```

---

## Setup Instructions

### 1. Create & Activate Virtual Environment

```
python -m venv venv
venv\Scripts\activate   # Windows
```

### 2. Install Dependencies

```
pip install -r requirements.txt
```

### 3. Run Data Pipeline

```
python pipeline.py
```

Creates:
- `retail.duckdb`
- Tables: `raw_online_retail`, `fct_transactions`

### 4. Apply Metrics SQL

```
python
>>> import duckdb
>>> con = duckdb.connect("retail.duckdb")
>>> con.execute(open("metrics_definitions.sql").read())
>>> con.close()
>>> exit()
```

### 5. Run Dashboard

```
streamlit run dashboard_app.py
```

Access at:
- http://localhost:8501

---

## Time Breakdown (approx.)

- Pipeline: 1–1.5 hrs  
- Metrics layer: 1 hr  
- Dashboard: 1–1.5 hrs  
- Documentation: 0.5 hr  

---

## Improvements with More Time

- Add dbt + data tests  
- Add automated CI pipeline  
- Cohort analysis dashboard  
- Deploy dashboard publicly
