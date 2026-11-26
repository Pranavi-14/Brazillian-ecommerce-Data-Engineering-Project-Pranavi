# Setup and Execution Guide

This document describes the prerequisites, environment configuration and high-level execution workflow for the Brazilian E-Commerce Data Engineering project.

## 1. Prerequisites

The following services and tools are required to reproduce the project:

| Tool / Service               | Purpose                             |
| ---------------------------- | ----------------------------------- |
| Azure subscription           | Access to Azure cloud services      |
| Azure Data Factory           | Pipeline creation and orchestration |
| Azure Data Lake Storage Gen2 | Storage for ingested datasets       |
| Azure Databricks             | Data processing and transformation  |
| Apache Spark / PySpark       | Distributed data processing         |
| Power BI Desktop             | Analytical reporting                |
| Git                          | Version control                     |

You will also need access to the Brazilian E-Commerce Public Dataset by Olist.

Dataset: [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## 2. Repository Contents

The repository is organised into the following main directories:

- `ADF/` – Azure Data Factory datasets, linked services and pipeline JSON exports.
- `Data/` – Sample source data organised by business date.
- `Databricks/` – Databricks notebooks, configuration, source modules, transformations and analytical views.
- `Evidence/` – Pipeline execution and validation evidence.
- `PowerBI/` – Power BI report file.

## 3. Azure Environment Configuration

Before executing the pipeline, configure the required Azure resources in your own subscription.

### Azure Data Lake Storage Gen2

1. Create an Azure Storage Account.
2. Enable hierarchical namespace to use Data Lake Storage Gen2 capabilities.
3. Create the required storage containers and directories.
4. Upload or configure access to the source datasets as required by the pipeline.

Use your own storage account and credentials. Do not reuse or publish credentials from another environment.

### Azure Data Factory

1. Create an Azure Data Factory instance.
2. Import or recreate the exported linked services, datasets and pipeline configurations from the `ADF/` directory.
3. Update storage endpoints and authentication settings to match your environment.
4. Verify that the datasets and linked services reference the correct resources.
5. Configure the pipeline parameters and required source and destination paths.

The repository contains the exported configurations. Resource-specific values must be reviewed and updated before execution.

### Azure Databricks

1. Create or use an Azure Databricks workspace.
2. Configure access to the storage location used by the project.
3. Import the Python source files and notebooks from the `Databricks/` directory.
4. Review the configuration in `config/Constants.py`.
5. Verify that storage paths, table locations and other environment-specific settings match your setup.
6. Attach the notebooks to a suitable compute resource before execution.

## 4. Data Configuration

The `Data/` directory contains datasets organised into two business-date folders:

- `2018-10-17/`
- `2018-10-18/`

These folders support the project's incremental ingestion scenarios.

Review the source paths and date-related configuration before running the pipeline. Ensure that the input files are available in the location expected by the ADF pipeline.

## 5. Execution Workflow

The project execution follows this high-level sequence:

### Step 1 – Data Ingestion

- Configure and validate the ADF linked services.
- Verify the dataset configurations.
- Execute the incremental ingestion pipeline.
- Inspect the pipeline execution result and the ingested Bronze data.

### Step 2 – Bronze Layer

- Verify the ingested source datasets.
- Check ingestion metadata and record counts.
- Confirm that the expected input files have been processed.

### Step 3 – Silver Layer

- Execute the relevant Silver notebooks.
- Apply data cleansing and transformation logic.
- Perform data-quality and referential integrity validations.
- Review rejected records and quarantine outputs.
- Validate the resulting Silver datasets.

### Step 4 – Gold Layer

- Execute the Gold-layer processing.
- Validate the analytical datasets.
- Review the SCD Type 2 and SCD Type 3 historical tracking scenarios where applicable.

### Step 5 – Power BI

- Open the report from `PowerBI/`.
- Configure the data connection for your environment.
- Refresh the data.
- Verify the analytical visuals against the Gold-layer outputs.

## 6. Validation

After execution, validate the following:

- ADF pipeline execution status.
- Incremental ingestion behaviour across the available business dates.
- Bronze ingestion record counts and metadata.
- Silver transformation and quarantine results.
- Delta MERGE behaviour.
- SCD Type 2 and SCD Type 3 results.
- Power BI data refresh and report outputs.

Refer to the `Evidence/` directory for the implementation and validation evidence collected for this project.

## 7. Configuration & Security Notes

- Use your own Azure subscription and resources.
- Update environment-specific storage paths and connection settings.
- Never commit account keys, access tokens, passwords or other secrets to GitHub.
- Use secure credential management for cloud connections.
- Review the exported JSON configurations before importing them into another environment.

## 8. Reproducibility Considerations

The repository provides the project source files, exported configurations, sample input data and execution evidence.

The exact execution environment may differ depending on Azure resource configuration, Databricks runtime, storage permissions and Power BI connectivity. Review the configuration files before attempting to reproduce the workflow.