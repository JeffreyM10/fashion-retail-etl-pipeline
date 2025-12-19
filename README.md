# 🛍 Fashion Retail Data Ingestion & ML Pipeline

A robust, end-to-end **ETL + Machine Learning pipeline** built with Python and PostgreSQL to process, validate, clean, and analyze retail sales data.

---

## 📌 Overview

This project implements a complete **data ingestion subsystem** for fashion retail data, designed to handle messy real-world inputs and transform them into reliable, analytics-ready datasets.

The pipeline:
- Ingests raw CSV retail sales data  
- Validates schema and enforces business rules  
- Cleans and standardizes records  
- Logs rejected rows with full context  
- Loads curated data into PostgreSQL using idempotent UPSERT logic  
- Uses the cleaned dataset to train and evaluate machine learning models  

The result is a repeatable, auditable pipeline that mirrors how data flows in modern data engineering and analytics platforms.

---

## 💡 Why This Project Matters

Retail data often comes from multiple systems—POS terminals, online stores, promotions, and returns—leading to inconsistent formats, missing values, and unreliable inputs.

Without a structured ingestion process:
- Bad data silently pollutes analytics  
- Failures are hard to trace  
- Pipelines are difficult to rerun or extend  
- Machine learning models train on unreliable data  

This project demonstrates how to solve those problems using **configuration-driven ETL**, strong **data quality checks**, and a clean separation between **raw, cleaned, and analytical layers**.

---

## 🏗 Pipeline Architecture

**Extract → Validate → Clean → Load → Analyze**

- **Bronze layer**: Raw CSV ingestion  
- **Silver layer**: Cleaned, deduplicated, validated data in PostgreSQL  
- **Rejects table**: Invalid rows logged with reason and raw payload  
- **ML layer**: Models trained on Silver data to uncover customer behavior patterns  

---

## 📂 Project Structure

```
ingestion/
├── src/
│   ├── main.py
│   ├── reader.py
│   ├── validate.py
│   ├── clean.py
│   ├── load.py
│   ├── ml_analysis.py
│   ├── logging_config.py
│   └── tests/
│       ├── test_validate.py
│       └── test_clean.py
├── config/
│   └── sources.yaml
├── logs/
│   └── ingestion.log
├── .env
└── README.md
```

---

## 🧾 Dataset Description

The Fashion Retail dataset contains mock retail transactions with fields such as:

- Customer Reference ID  
- Item Purchased  
- Purchase Amount (USD)  
- Date Purchase  
- Review Rating (0–5)  
- Payment Method  

Rows failing validation are logged to a rejects table, while valid rows are cleaned and loaded for analytics and ML.

---

## ✅ Pipeline Features

### Configuration-Driven Ingestion
- YAML-defined schema, types, source paths, and targets  
- Easy onboarding of new data sources  

### Validation & Data Quality
- Required column checks  
- Safe type casting  
- Business rules enforcement  

### Cleaning & Deduplication
- Standardized formatting  
- Normalized strings  
- Deduplication using business keys  

### PostgreSQL Loading
- UPSERT logic for idempotent runs  
- Separate tables for valid data and rejects  

### Logging
- Run summaries with row counts and runtime  
- Error tracking for rejected records  

---

## 🤖 Machine Learning

The cleaned Silver-layer data powers ML models to predict low customer reviews.

- Logistic Regression for interpretability  
- Random Forest for nonlinear pattern detection  
- Outlier detection on purchase amounts  

This highlights how reliable ETL directly enables meaningful analytics.

---

## 🧪 Testing

- pytest-based unit tests  
- Validation and cleaning logic tested in isolation  
- No database mocking required  

---

## 🔐 Secure Configuration

- Credentials stored in `.env`  
- `.env` excluded from version control  

---

## 🚀 Running the Pipeline

```bash
python -m ingestion.src.main
```

---

## 🌱 Future Enhancements

- Real retail data ingestion (APIs, POS systems)  
- Orchestration with Airflow or Prefect  
- ML model savepoints and versioning  
- Data quality frameworks (dbt / Great Expectations)  
- Expanded analytics and Gold-layer tables  

---

## 🧠 Summary

This project demonstrates:
- Real-world ETL pipeline design  
- Strong data validation and auditing  
- Clean separation of data layers  
- Secure configuration management  
- Machine learning built on trusted data  

It reflects modern data engineering practices used in production systems.
