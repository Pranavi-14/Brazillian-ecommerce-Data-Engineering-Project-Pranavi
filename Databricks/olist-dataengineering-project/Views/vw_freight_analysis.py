# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC databricks_olist_pranavi.gold.vw_freight_analysis
# MAGIC COMMENT 'Freight revenue analysis by product category'
# MAGIC AS
# MAGIC SELECT
# MAGIC     p.product_category_name,
# MAGIC     COUNT(*) AS total_items,
# MAGIC     ROUND(SUM(f.freight_value), 2) AS total_freight,
# MAGIC     ROUND(SUM(f.price), 2) AS product_revenue,
# MAGIC     ROUND(SUM(f.item_total_value), 2) AS total_revenue,
# MAGIC     ROUND(AVG(f.freight_value), 2) AS average_freight_per_item,
# MAGIC     ROUND(
# MAGIC         SUM(f.freight_value) / NULLIF(SUM(f.item_total_value), 0) * 100,
# MAGIC         2
# MAGIC     ) AS freight_percentage
# MAGIC FROM databricks_olist_pranavi.gold.fact_sales f
# MAGIC JOIN databricks_olist_pranavi.gold.dim_products p
# MAGIC     ON f.product_key = p.product_key
# MAGIC GROUP BY
# MAGIC     p.product_category_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_olist_pranavi.gold.vw_freight_analysis
# MAGIC ORDER BY total_freight DESC;
