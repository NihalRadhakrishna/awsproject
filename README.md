# 🚕 Uber Ride Streaming & Batch Data Platform (AWS – Kappa Architecture)

## 📌 Project Overview

This project implements a real-time and batch data processing platform for ride booking events using a **Kappa Architecture** pattern on AWS.

The system ingests ride data via HTTP, processes it in real time for alerts, and replays the same Kafka stream for batch analytics using a Bronze → Silver → Gold data lake architecture.

---

# 🏗 Architecture Overview

## 🔹 Ingestion Layer

1. Ride data is sent via HTTP to **Amazon API Gateway**
2. **AWS Lambda** consumes data from API Gateway
3. Lambda publishes events to **Amazon MSK (Kafka)** in real time

---

# ⚡ Stream Processing Layer (Real-Time)

- Data is consumed from **Amazon MSK**
- Processed in real-time using **Apache Spark on Amazon EMR (Streaming Cluster)**
- Business metrics are evaluated (e.g., surge triggers, anomalies)
- Alerts are sent via **Amazon SNS (Email Notifications)**

This layer handles:
- Low-latency processing
- Event-driven alerts
- Near real-time analytics

---

# 📦 Batch Processing Layer

The batch layer follows a **Kappa Architecture replay model**.

Instead of storing raw files separately, it:
- Replays historical events directly from Kafka
- Uses **Amazon MWAA (Managed Airflow)** for orchestration
- Uses **AWS CloudFormation** for Infrastructure as Code (IaC)
- Creates a separate EMR cluster for batch workloads

---

# 🪵 Bronze → Silver → Gold Pipeline (Data Lake Design)

Batch processing is executed through Airflow DAG workflows:

## 🥉 Bronze Layer
- Raw Kafka replay data
- Stored in **Amazon S3 (bronze/)**
- Minimal transformation

## 🥈 Silver Layer
- Cleaned & structured data
- Deduplication and validation
- Stored in **Amazon S3 (silver/)**

## 🥇 Gold Layer
- Aggregated and business-ready data
- Stored in **Amazon S3 (gold/)**
- Final output modeled as a **Star Schema**

---

# ⭐ Data Warehouse Model (Gold Layer)

The Gold layer is structured using a **Star Schema**:

- `fact_rides`
- `dim_customer`
- `dim_driver`
- `dim_date`
- `dim_location`
- `dim_payment`

This enables efficient analytical queries using:

- **AWS Glue (Data Catalog)**
- **Amazon Athena**

---

# 🔐 Security & Governance

| Service | Purpose |
|----------|----------|
| AWS IAM | Roles & permissions |
| AWS KMS | Encryption (Kafka, S3, Secrets) |
| AWS Secrets Manager | Kafka authentication credentials |
| Amazon CloudWatch | Logs & monitoring |

---

# 🧰 Tech Stack

- Amazon API Gateway  
- AWS Lambda  
- Amazon MSK (Kafka)  
- Amazon EMR (Spark – Streaming & Batch)  
- Amazon MWAA (Airflow)  
- AWS CloudFormation (IaC)  
- Amazon S3 (Data Lake)  
- AWS Glue  
- Amazon Athena  
- Amazon SNS  
- AWS IAM  
- AWS KMS  
- AWS Secrets Manager  
- Amazon CloudWatch  

---

# 🧠 Architecture Pattern

This system follows a **Kappa Architecture** approach:

- Single streaming backbone (Kafka)
- Real-time processing layer
- Replay-based batch analytics
- Unified event log for both workloads

### Benefits:
- Replay capability
- No duplicate ingestion pipelines
- Scalable architecture
- Fault-tolerant design
- Unified processing model

---

---

# 👨‍💻 Author

**Nihal Radhakrishna**  
AWS Data Engineering Project  
