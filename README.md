# Fashion Retail ETL Pipeline
A clean, professional, and fully configurable data-ingestion pipeline built with Python and PostgreSQL.

## Overview
This project implements a complete **ETL (Extract, Transform, Load)** pipeline for processing Fashion Retail Sales data.  
The system ingests CSV files, validates schema and data types, applies business rules, cleans and standardizes values, and loads curated records into a PostgreSQL staging table.

Invalid or malformed rows are captured with full context for auditability, ensuring transparency and data integrity throughout the ingestion lifecycle.

---

## 💡 Why This Project Matters
Modern data engineering pipelines must be:

- **Reliable** — detect and isolate bad data  
- **Maintainable** — driven by configuration, not hard-coded logic  
- **Auditable** — track failures and transformations  
- **Scalable** — support new sources with minimal changes  

This project demonstrates those principles in a real ETL workflow suitable for analytics, reporting, or downstream modeling.

---

## 📂 Project Structure
ingestion/
src/
main.py
reader.py
validate.py
clean.py
load.py
config.py
config/
sources.yml
data/
Fashion_Retail_Sales_Small_test.csv
logs/
run.log

yaml
Copy code

---

## 🧾 Dataset Description
The Fashion Retail dataset contains 99 mock purchase transactions with the following fields:

| Column                     | Description                                      |
|---------------------------|--------------------------------------------------|
| Customer Reference ID     | Unique identifier for each customer              |
| Item Purchased            | Retail product bought                             |
| Purchase Amount (USD)     | Transaction total                                 |
| Date Purchase             | Date the purchase occurred                        |
| Review Rating             | Optional rating between 0–5                       |
| Payment Method            | Cash or Credit Card                                |

After validation:
- **81 rows** pass all checks  
- **18 rows** are rejected and stored for auditing  

---

## ✔️ Pipeline Features

### **1. Configuration-Driven Ingestion**
A YAML file defines:
- Schema  
- Data types  
- File paths  
- Target tables  
- Primary keys  

Enables rapid onboarding of new data sources.

---

### **2. Schema & Type Validation**
The pipeline validates:
- Required columns  
- Normalized column names  
- Conformance to data types (`int`, `float`, `string`, `datetime`)  

Rows failing schema or type validation flow into a rejection path.

---

### **3. Business Rule Validation**
Rules include:
- Purchase amount must be ≥ 0  
- Review ratings must be between 0 and 5  
- Payment method must be `Cash` or `Credit Card`  

---

### **4. Data Cleaning**
The dataset is cleaned by:
- Trimming whitespace  
- Standardizing payment methods  
- Formatting item names consistently  
- Normalizing string types  

---

### **5. Loading to PostgreSQL**
Validated, cleaned rows are loaded into:

stg_fashion_sales

sql
Copy code

Rejected rows (with metadata and JSON payloads) are loaded into:

stg_rejects

yaml
Copy code

Both support traceability and downstream processing.

---

## Logging
Each run generates an entry in:

logs/run.log

makefile
Copy code

Example:

2025-01-10 23:02:11 | INFO | source=fashion_sales_csv | read=99 | loaded=81 | rejected=18

yaml
Copy code

This provides lightweight observability into pipeline operations.

---

## Technology Stack
- **Python 3**
  - Pandas  
  - SQLAlchemy  
  - psycopg2-binary  
  - PyYAML  
- **PostgreSQL**
- **DBeaver** (database visualization)

---

## Running the Pipeline
From the project root:

```bash
python -m ingestion.src.main


Future Enhancements
🌱 UPSERT / deduplication logic

Airflow or Prefect orchestration

Cloud storage ingestion (S3/GCS)

Automated quality reports

Transformation layer for analytics


Summary
This project demonstrates:

ETL architecture

Schema & type validation

Business rule enforcement

Data cleaning

Rejection handling with JSON payloads

PostgreSQL loading

Operational logging