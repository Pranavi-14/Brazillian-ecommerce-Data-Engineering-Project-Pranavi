# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW
# MAGIC databricks_olist_pranavi.gold.vw_sales_overview
# MAGIC
# MAGIC COMMENT 'Overall e-commerce sales KPIs and order metrics'
# MAGIC
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC
# MAGIC     COUNT(*) AS total_order_items,
# MAGIC
# MAGIC     ROUND(SUM(price), 2) AS product_revenue,
# MAGIC
# MAGIC     ROUND(SUM(freight_value), 2) AS freight_revenue,
# MAGIC
# MAGIC     ROUND(SUM(item_total_value), 2) AS total_revenue,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(item_total_value) /
# MAGIC         NULLIF(COUNT(DISTINCT order_id), 0),
# MAGIC         2
# MAGIC     ) AS average_order_value
# MAGIC
# MAGIC FROM databricks_olist_pranavi.gold.fact_sales;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_olist_pranavi.gold.vw_sales_overview;
