# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC databricks_olist_pranavi.gold.vw_order_metrics
# MAGIC COMMENT 'Order-level sales metrics including item count and order value'
# MAGIC AS
# MAGIC SELECT
# MAGIC     f.order_id,
# MAGIC     COUNT(*) AS total_items,
# MAGIC     COUNT(DISTINCT f.product_key) AS unique_products,
# MAGIC     COUNT(DISTINCT f.seller_key) AS unique_sellers,
# MAGIC     ROUND(SUM(f.price), 2) AS product_revenue,
# MAGIC     ROUND(SUM(f.freight_value), 2) AS freight_revenue,
# MAGIC     ROUND(SUM(f.item_total_value), 2) AS total_order_value,
# MAGIC     ROUND(AVG(f.price), 2) AS average_item_price
# MAGIC FROM databricks_olist_pranavi.gold.fact_sales f
# MAGIC GROUP BY
# MAGIC     f.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_olist_pranavi.gold.vw_order_metrics
# MAGIC ORDER BY total_order_value DESC
# MAGIC LIMIT 20;
