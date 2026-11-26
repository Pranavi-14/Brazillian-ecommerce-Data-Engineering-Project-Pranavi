# Databricks Implementation – Bronze, Silver & Gold

## 1. Overview

Azure Databricks is used for data processing, transformation, data-quality validation and analytical modelling in this project.

The implementation follows a Medallion Architecture consisting of Bronze, Silver and Gold layers.

The project uses PySpark for distributed data processing and Delta Lake for table storage and incremental data modifications.

## 2. Databricks Repository Structure

The Databricks implementation is organised as follows:

```
Databricks/
└── olist-dataengineering-project/
    ├── config/
    │   └── Constants.py
    │
    ├── Notebooks/
    │   ├── Bronze/
    │   │   └── Bronze_BrazillianDataSet.py
    │   │
    │   ├── Silver/
    │   │   ├── final.py
    │   │   ├── geolocation.py
    │   │   ├── orders.py
    │   │   ├── orders_customers.py
    │   │   ├── orders_payments.py
    │   │   ├── order_items.py
    │   │   ├── order_products.py
    │   │   ├── order_reviews.py
    │   │   ├── order_sellers.py
    │   │   └── referential_integrity.py
    │   │
    │   └── Gold/
    │       └── sales_analysis.py
    │
    ├── src/
    │   ├── loaders.py
    │   └── Writers.py
    │
    ├── transformations/
    │   ├── rules/
    │   └── silver/
    │
    └── Views/
```

## 3. Bronze Layer – Raw Data Ingestion

The Bronze layer stores the ingested source datasets for downstream processing.

The source entities include:

- Customers
- Geolocation
- Orders
- Order items
- Order payments
- Order reviews
- Products
- Sellers
- Product category translations

The project includes a Bronze ingestion notebook:

`Notebooks/Bronze/Bronze_BrazillianDataSet.py`

The ingestion workflow is designed to retain source data and ingestion metadata for traceability.

Metadata fields used in the ingestion workflow include:

- `business_date`
- `load_date`
- `ingested_at`
- `source_file`

The Azure Data Factory ingestion implementation is documented separately in `pipeline-design.md`.

## 4. Silver Layer – Data Transformation & Quality

The Silver layer contains transformed datasets prepared for downstream analytical processing.

PySpark notebooks and transformation modules are organised by business entity.

### Transformation areas

- Orders and customers
- Order items
- Payments
- Products
- Reviews
- Sellers
- Geolocation
- Referential integrity

### Data-quality processing

The implementation includes validation and data-quality checks across the source entities.

These include:

- Null and missing-value checks.
- Business-rule validations.
- Referential integrity checks.
- Identification of invalid records.
- Quarantine handling for rejected records.
- Delta Lake MERGE operations for incremental processing.

The project evidence documents validation results, record counts and quarantine outcomes across the Bronze and Silver layers.

## 5. Gold Layer – Dimensional Modelling

The Gold layer converts the transformed data into business-oriented analytical datasets.

The dimensional model contains:

### Dimension tables

| Dimension       | Description         | Historical tracking |
| --------------- | ------------------- | ------------------- |
| `dim_dates`     | Calendar attributes | Not applicable      |
| `dim_customers` | Customer attributes | SCD Type 2          |
| `dim_products`  | Product attributes  | SCD Type 2          |
| `dim_sellers`   | Seller attributes   | SCD Type 3          |

### Fact table

| Table        | Description                   | Grain                  |
| ------------ | ----------------------------- | ---------------------- |
| `fact_sales` | Sales and order-item measures | One row per order item |

The fact table includes:

- `order_id`
- `order_item_id`
- `date_key`
- `customer_key`
- `product_key`
- `seller_key`
- `price`
- `freight_value`
- `item_total_value`

The item total is calculated as:

`item_total_value = price + freight_value`

## 6. Slowly Changing Dimension Type 2

SCD Type 2 is implemented for customer and product dimensions.

The objective is to preserve historical versions of selected dimension attributes when changes are detected.

### Tracked customer attributes

- `customer_city`
- `customer_state`
- `customer_zip_code_prefix`

### Tracked product attributes

- `product_category_name`
- `product_category_name_english`

### Implementation approach

For the initial load:

- Each source entity is inserted into the corresponding dimension.
- A surrogate key is assigned.
- `effective_from_date` is initialised to the dataset start date.
- `effective_to_date` is set to `9999-12-31` for the current version.

For subsequent loads:

1. Incoming Silver records are compared against the current Gold dimension versions.
2. Records are classified as new, changed or unchanged.
3. For a changed record, the existing dimension version is closed by updating its `effective_to_date`.
4. A new dimension version is inserted with a new surrogate key.
5. New entities are inserted as their first version.
6. Unchanged entities are excluded from the staging changes.

The implementation uses a composite MERGE key consisting of the business key and `effective_from_date`.

This allows multiple historical versions of the same customer or product to be maintained.

## 7. Slowly Changing Dimension Type 3

SCD Type 3 is implemented for the seller dimension.

Unlike SCD Type 2, this approach maintains one row per seller and stores a limited previous-value history.

### Tracked seller attributes

- `seller_city`
- `seller_state`
- `seller_zip_code_prefix`

Corresponding previous-value columns are maintained:

- `previous_seller_city`
- `previous_seller_state`
- `previous_seller_zip_code_prefix`

### Implementation approach

For the initial load:

- Each seller is assigned a surrogate key.
- Current seller attributes are populated.
- Previous-value columns are initialised as NULL.

For subsequent loads:

1. Incoming seller records are compared against the existing Gold dimension.
2. New, changed and unchanged sellers are identified.
3. For changed sellers, current attributes are updated.
4. Previous-value columns retain the earlier values for attributes that changed.
5. Unchanged attributes retain their existing previous-value history.
6. New sellers are inserted with a new surrogate key.

The MERGE uses `seller_id` as the business key.

SCD Type 3 maintains only one level of previous-value history; it does not preserve a complete sequence of historical versions.

## 8. Fact Table – fact_sales

The fact table is built from Silver order-item data and joined with the Gold dimensions.

### Fact table grain

One row per order item, identified by:

`(order_id, order_item_id)`

### Source datasets

- `silver.order_items`
- `silver.orders`

### Dimension lookups

The fact-building logic joins:

- Order purchase date to `dim_dates`.
- Product to the historically valid version in `dim_products`.
- Customer to the historically valid version in `dim_customers`.
- Seller to `dim_sellers`.

For customer and product dimensions, the purchase date is checked against the dimension's effective date range.

This ensures the fact record references the dimension version valid on the transaction date.

The seller dimension uses its stable seller surrogate key.

### Fact measures

- Product price
- Freight value
- Item total value

### Incremental write

The fact table is written through the project's `write_delta` utility using a MERGE key consisting of:

- `order_id`
- `order_item_id`

This supports matching existing order-item records and inserting unmatched records through the configured Delta write logic.

## 9. Delta Lake & Reusable Components

The project includes reusable source modules:

- `src/loaders.py`
- `src/Writers.py`

The Gold notebook imports `write_delta` from `src.Writers`.

This separates the notebook's transformation and staging logic from the reusable table-writing functionality.

The exact write and MERGE behaviour is implemented in the shared writer module.

## 10. Analytical Views

The repository contains eight analytical view definitions:

| View                      | Analytical purpose                |
| ------------------------- | --------------------------------- |
| `vw_monthly_sales`        | Monthly sales and revenue trends  |
| `vw_sales_overview`       | Overall sales performance         |
| `vw_product_performance`  | Product category performance      |
| `vw_seller_performance`   | Seller sales performance          |
| `vw_customer_performance` | Customer purchasing performance   |
| `vw_order_metrics`        | Order-level metrics               |
| `vw_freight_analysis`     | Freight contribution and analysis |
| `vw_customer_retention`   | One-time versus repeat customers  |

These views provide analytical outputs for Power BI reporting.

## 11. Implementation Evidence

The `Evidence/` directory contains execution evidence for the pipeline and data processing stages.

The evidence documents:

- Bronze ingestion results.
- Silver transformation and data-quality outcomes.
- Quarantine record counts.
- Delta MERGE scenarios.
- Customer and product SCD Type 2 scenarios.
- Seller SCD Type 3 scenarios.

Refer to the evidence documents for the corresponding execution results.

## 12. Configuration Considerations

The current `config/Constants.py` file contains relative dataset path constants for the source entities.

The Databricks environment also requires appropriate configuration for:

- Storage access.
- Workspace paths.
- Table locations.
- Python module imports.
- Databricks compute resources.

Workspace-specific paths in notebook code may need to be updated when importing the project into another Databricks workspace.

No credentials or access secrets should be committed to the repository.

## 13. Scope & Limitations

This document describes the implementation represented by the repository source files and collected execution evidence.

Reproducing the complete workflow requires configuring the Azure and Databricks resources in the target environment.

Performance benchmarking, production deployment, automated scheduling and comprehensive operational monitoring should be treated as separate capabilities unless supported by additional implementation and execution evidence.