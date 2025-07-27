
# 🛒 Ecommerce Data Pipeline and Analysis

This project implements an end-to-end data pipeline to process, transform, and analyze e-commerce transaction data. It automates the ingestion of synthetic data into PostgreSQL, transfers it to BigQuery through GCS, performs multi-layered transformations using dbt, and visualizes insights via Looker Studio. The pipeline runs using Apache Airflow and Docker, with support for email notifications on failure.

---

## 📁 Configuration & Secrets

- GCP Service Account JSON: `config/key.json`
- PostgreSQL credentials, GCP project ID, and SMTP settings should be stored as environment variables or managed via Airflow Variables.
- Raw ingested data is stored by date in:  
  `data/source/csv/YYYY-MM-DD/`

---

## 🎯 Project Objectives

- ⏱️ Automate ETL process using **Airflow**
- 🐍 Ingest synthetic e-commerce data into **PostgreSQL**
- ☁️ Extract & load PostgreSQL data into **GCS**, then into **BigQuery**
- 🔄 Transform datasets using **dbt** across staging, core, and datamart layers
- 📊 Create business dashboards in **Looker Studio**
- 📧 Implement real-time email alerts using **SMTP**

---

## ⚙️ Tech Stack

| Category            | Tools                                     |
|---------------------|-------------------------------------------|
| Orchestration        | Apache Airflow (Docker)                  |
| Database             | PostgreSQL                               |
| Data Lake            | Google Cloud Storage (GCS)               |
| Data Warehouse       | Google BigQuery                          |
| Transformation       | dbt (Data Build Tool)                    |
| Programming Language | Python                                   |
| Visualization        | Looker Studio                            |
| Notification         | SMTP via Airflow                         |
| Containerization     | Docker & Docker Compose                  |

---

## 🏗️ Project Structure

```bash
├── airflow/                      
│   ├── dags/                     # All Airflow DAGs
│   │   ├── ingest_to_postgres.py
│   │   ├── postgres_to_gcs.py
│   │   ├── gcs_to_bq_staging.py
│   │   ├── bq_transform_core.py
│   │   └── bq_transform_datamart.py
│   ├── plugins/
│   ├── Dockerfile
│   └── requirements.txt
│
├── dbt/                          
│   ├── models/
│   │   ├── staging/
│   │   ├── dim/
│   │   ├── fact/
│   │   └── datamart/
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── ingestion/
│   └── main_ingestion.py         # Python script to insert dummy data into PostgreSQL
│
├── config/
│   └── key.json                  # GCP service account key
│
├── data/
│   └── source/csv/YYYY-MM-DD/
│
├── docker-compose.yml
└── README.md
```

---

## 🧪 How to Run the Project

### 📦 1. Start Docker + Airflow

```bash
docker-compose build
docker-compose up -d
```

Visit the Airflow web UI: [http://localhost:8080](http://localhost:8080)  
(default login: `airflow` / `airflow`)

---

### 🐍 2. Ingest Synthetic Data (to PostgreSQL)

```bash
cd ingestion/
python main_ingestion.py
```

- This script creates dummy transaction data using `Faker` and inserts it into PostgreSQL.
- You can modify the number of inserted rows via `main(n=1000)`.

---

### ⏳ 3. Trigger Airflow DAGs (in order)

In Airflow UI, manually trigger the following DAGs sequentially:

1. `ingest_to_postgres` (optional if you run the script manually)
2. `postgres_to_gcs`
3. `gcs_to_bq_staging`
4. `bq_transform_core`
5. `bq_transform_datamart`

---

### 🧠 4. Run dbt Manually (optional via CLI)

```bash
cd dbt/
dbt run
```

You can also run `dbt test` to validate model logic.

---

### 📊 5. View Dashboard

Open your [Looker Studio](https://lookerstudio.google.com/) dashboard (linked manually).

Metrics visualized:

- Total Sales & Revenue Trends
- Top Selling Products
- Customer Transaction Behavior
- Payment Preferences
- Channel & Store Performance

---

### 📧 6. Email Notification

If any Airflow task fails, you will receive an email with:

- DAG and task names
- Error message and exception
- Execution timestamp
- Log URL (Airflow UI)

Configured via Airflow callbacks with SMTP (`send_email_smtp`).

---

## 🧱 Data Warehouse Layers

### 🟨 Staging Layer (`stg_*`)
Raw structured data from GCS → BigQuery.

### 🟦 Core Layer (Fact & Dimension)
Standardized data for relationships and metrics:

- `fact_transaction`, `fact_review`
- `dim_customer`, `dim_product`, `dim_payment`, `dim_session`

### 🟥 Datamart Layer (`marts_*`)
Aggregated tables for analytics use cases:

- `marts_customer_product_summary`
- `marts_payment_transaction_summary`
- `marts_review_product_summary`
- `marts_session_transaction_summary`

---

## 📊 EDA Summary (Before Transformation)

| Metric                  | Value          |
|--------------------------|----------------|
| Transactions             | ~16,500 rows   |
| Unique Products          | ~60,000 items  |
| Customers                | ~100,000       |
| Review Records           | ~16,500 rows   |
| Payment Types            | 12             |
| Web Sessions             | ~50,000 rows   |

---

## 🧩 Challenges Faced

- DAG scheduling and dependencies for multi-stage data movement
- Incremental and partitioned loading in BigQuery
- Exception handling for email alerts
- dbt backfill management

---

## 🚀 Future Improvements

- ✅ Add data validation with **dbt test** or **Great Expectations**
- 🔁 Use **parameterized DAGs** and Jinja templating
- 🚀 Implement **CI/CD** for dbt and Airflow (GitHub Actions)
- 📈 Integrate basic **predictive ML models**
- ☁️ Migrate Airflow to **Cloud Composer**
- 🔎 Improve monitoring with **Stackdriver** or **Prometheus**

---

## ✅ Conclusion

This project delivers a structured and automated data pipeline for e-commerce analytics. From ingestion to visualization, the solution empowers decision-makers with near real-time business insights. The architecture is modular, scalable, and ready for production deployment or advanced analytics use cases.
