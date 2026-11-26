# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC databricks_olist_pranavi.gold.vw_customer_performance
# MAGIC COMMENT 'Customer purchasing behaviour based on orders and revenue'
# MAGIC AS
# MAGIC SELECT
# MAGIC     c.customer_unique_id,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC     COUNT(*) AS total_items_purchased,
# MAGIC     ROUND(SUM(f.price), 2) AS product_revenue,
# MAGIC     ROUND(SUM(f.freight_value), 2) AS freight_revenue,
# MAGIC     ROUND(SUM(f.item_total_value), 2) AS total_revenue,
# MAGIC     ROUND(
# MAGIC         SUM(f.item_total_value) / COUNT(DISTINCT f.order_id),
# MAGIC         2
# MAGIC     ) AS average_order_value
# MAGIC FROM databricks_olist_pranavi.gold.fact_sales f
# MAGIC JOIN databricks_olist_pranavi.gold.dim_customers c
# MAGIC     ON f.customer_key = c.customer_key
# MAGIC GROUP BY
# MAGIC     c.customer_unique_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_olist_pranavi.gold.vw_customer_performance
# MAGIC ORDER BY total_revenue DESC;
