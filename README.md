# End-to-End Brazilian E-Commerce Data Engineering & Analytics Platform

## 1. Project Overview

This project demonstrates an end-to-end data engineering pipeline built using Azure Data Factory, Azure Data Lake Storage, Azure Databricks, Delta Lake and Power BI.

The project uses the Brazilian E-Commerce Public Dataset by Olist to implement data ingestion, data quality validation, transformation, incremental processing, historical data tracking and analytical reporting.

The pipeline follows a **Bronze–Silver–Gold architecture**, transforming raw source data into structured datasets and business-oriented analytical outputs.

## 2. Business Context & Project Objective

E-commerce platforms generate data across multiple business entities, including orders, customers, products, payments, sellers and reviews. Consolidating these datasets into reliable, consistent and analytics-ready information requires structured data processing.

The objective of this project is to build a data engineering workflow that:

- Ingests source datasets through Azure Data Factory.
- Supports incremental data ingestion.
- Stores raw data in the Bronze layer.
- Applies data quality checks and transformations in the Silver layer.
- Implements Delta Lake MERGE operations.
- Maintains historical changes using SCD Type 2 and SCD Type 3 techniques in the Gold layer.
- Provides analytical views and Power BI visualisations.

## 3. Architecture

The project follows a Medallion Architecture:

**Source Data → Azure Data Factory → Azure Data Lake Storage → Azure Databricks → Bronze → Silver → Gold → Power BI**

| Layer / Component       | Purpose                                                                     |
| ----------------------- | --------------------------------------------------------------------------- |
| Source                  | Brazilian E-Commerce CSV datasets                                           |
| Azure Data Factory      | Pipeline orchestration and incremental ingestion                            |
| Azure Data Lake Storage | Data storage                                                                |
| Bronze                  | Raw ingested data with ingestion metadata                                   |
| Silver                  | Data validation, cleansing, transformation and referential integrity checks |
| Gold                    | Business-oriented datasets and historical data tracking                     |
| Power BI                | Analytical reporting and visualisation                                      |

## 4. Technology Stack

| Technology                   | Usage                                     |
| ---------------------------- | ----------------------------------------- |
| Azure Data Factory           | Data ingestion and orchestration          |
| Azure Data Lake Storage Gen2 | Cloud data storage                        |
| Azure Databricks             | Data processing and transformation        |
| PySpark                      | Distributed data processing               |
| Delta Lake                   | MERGE operations and table management     |
| Python                       | Pipeline and transformation logic         |
| SQL                          | Data analysis and analytical views        |
| Power BI                     | Data visualisation and reporting          |
| Git & GitHub                 | Version control and project documentation |

## 5. Key Implementations

### Data Ingestion

- Azure Data Factory pipeline for source data ingestion.
- Parameterised and dynamic dataset configurations.
- Manifest-driven incremental file ingestion using processed-file tracking.
- Processed-file tracking to support incremental execution.

### Bronze Layer

- Ingestion of source datasets into the Bronze layer.
- Addition of ingestion metadata.
- Retention of raw ingested records for downstream processing.

### Silver Layer

- Data cleansing and transformation using PySpark.
- Null and business-rule validations.
- Referential integrity checks across related datasets.
- Invalid-record identification and quarantine handling.
- Delta Lake MERGE operations for incremental updates.

### Gold Layer

- Business-oriented analytical datasets.
- Slowly Changing Dimension (SCD) Type 2 implementation for historical tracking.
- Slowly Changing Dimension (SCD) Type 3 implementation for selected attribute changes.
- Analytical views for reporting.

### Reporting

- Power BI dashboard for e-commerce sales analysis.
- Visualisation of sales and related business metrics using Gold-layer outputs.

## 6. Repository Structure

```
.
├── ADF/
│   ├── datasets/
│   ├── linkedServices/
│   └── pipelines/
│
├── Data/
│   ├── 2018-10-17/
│   └── 2018-10-18/
│
├── Databricks/
│   └── olist-dataengineering-project/
│       ├── config/
│       ├── Notebooks/
│       │   ├── Bronze/
│       │   ├── Silver/
│       │   └── Gold/
│       ├── src/
│       ├── transformations/
│       └── Views/
│
├── Evidence/
│
├── PowerBI/
│
└── README.md
```

## 7. Execution Flow

The project workflow is organised into the following stages:

1. Configure the Azure storage and Data Factory resources.
2. Execute the ADF ingestion pipeline.
3. Verify the ingested data in the Bronze layer.
4. Execute the Silver-layer notebooks for validation and transformation.
5. Verify Delta MERGE and data-quality results.
6. Execute the Gold-layer processing and historical dimension logic.
7. Create or refresh analytical views.
8. Connect Power BI to the analytical outputs and refresh the report.

Detailed setup instructions, configuration requirements and execution guidance will be documented separately.

## 8. Evidence & Validation

The `Evidence/` directory contains execution evidence and validation results collected during project implementation.

The evidence includes:

- Initial and subsequent pipeline execution results.
- Incremental ingestion behaviour.
- Bronze and Silver processing results.
- Data-quality and quarantine results.
- Delta MERGE validation.
- SCD Type 2 and SCD Type 3 validation scenarios.

Refer to the evidence documents for execution-level details and validation outputs.

## 9. Project Scope & Reproducibility

This repository contains exported ADF configurations, Databricks source files, sample input datasets, Power BI report files and implementation evidence.

Reproducing the complete workflow requires appropriately configured Azure resources, storage access, Databricks environment and Power BI connectivity.

Cloud resource identifiers, credentials and connection secrets must be configured in the user's own environment. Sensitive credentials are not included in this repository.

## 10. Dataset Source

The project uses the Brazilian E-Commerce Public Dataset by Olist, available on Kaggle:

[https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

The dataset contains information about customers, orders, order items, payments, reviews, products, sellers and geolocation.

---

**Project Focus:** Azure Data Engineering | Incremental Data Pipelines | PySpark | Delta Lake | Data Quality | SCD Type 2 & Type 3 | Power BI
