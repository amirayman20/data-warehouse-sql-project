# SQL Data Warehouse Project (Medallion Architecture)

This repository contains my first end‑to‑end SQL Data Warehouse project.  
I am building this project step by step to strengthen my skills in data engineering, SQL development, and analytical data modeling.

The project follows the Medallion Architecture (Bronze → Silver → Gold) to ensure clean, structured, and business‑ready data.

---

## 🏗️ Data Architecture (Medallion)

### 🟫 Bronze Layer — Raw Data
Stores the raw data exactly as received from the source systems (CSV files).  
No cleaning or transformations are applied at this stage.

### 🥈 Silver Layer — Cleaned Data
This layer includes all data quality and standardization steps:
- Data type corrections  
- Duplicate removal  
- Missing value handling  
- Outlier treatment  
- Basic transformations  

### 🥇 Gold Layer — Business Layer
Contains the final analytical model prepared for reporting:
- Fact tables  
- Dimension tables  
- KPIs and metrics  
- Aggregated data for analysis  

---

## 📊 Project Goals
- Build a structured SQL-based data warehouse  
- Apply Medallion Architecture in a real project  
- Practice ETL logic using SQL  
- Design a clean star schema for analytics  
- Develop SQL queries for insights and reporting  

---

## 🛠️ Tools & Technologies
- SQL Server  
- SQL Server Management Studio (SSMS)  
- Git & GitHub  
- DrawIO (for diagrams)  

---

## 📂 Repository Structure
data-warehouse-project/
│
├── datasets/          # Raw CSV files
├── docs/              # Architecture diagrams and documentation
├── scripts/           # SQL scripts for Bronze, Silver, and Gold layers
├── tests/             # Data quality checks
└── README.md          # Project overview

---

## 📌 Project Status
This is my working version of the project.  
I will continue updating the repository as I progress through each layer and add more documentation, diagrams, and SQL scripts.
