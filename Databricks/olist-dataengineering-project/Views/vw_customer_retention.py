# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC databricks_olist_pranavi.gold.vw_customer_retention
# MAGIC COMMENT 'Customer classification based on repeat purchasing behaviour'
# MAGIC AS
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN total_orders = 1 THEN 'One-time Customer'
# MAGIC         ELSE 'Repeat Customer'
# MAGIC     END AS customer_type,
# MAGIC     COUNT(*) AS total_customers
# MAGIC FROM databricks_olist_pranavi.gold.vw_customer_performance
# MAGIC GROUP BY
# MAGIC     CASE
# MAGIC         WHEN total_orders = 1 THEN 'One-time Customer'
# MAGIC         ELSE 'Repeat Customer'
# MAGIC     END;
