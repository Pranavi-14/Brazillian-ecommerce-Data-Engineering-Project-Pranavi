# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC databricks_olist_pranavi.gold.vw_seller_performance
# MAGIC COMMENT 'Seller performance based on sales, orders and revenue'
# MAGIC AS
# MAGIC SELECT
# MAGIC     s.seller_id,
# MAGIC     COUNT(*) AS units_sold,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC     ROUND(SUM(f.price), 2) AS product_revenue,
# MAGIC     ROUND(SUM(f.freight_value), 2) AS freight_revenue,
# MAGIC     ROUND(SUM(f.item_total_value), 2) AS total_revenue,
# MAGIC     ROUND(AVG(f.price), 2) AS average_product_price
# MAGIC FROM databricks_olist_pranavi.gold.fact_sales f
# MAGIC JOIN databricks_olist_pranavi.gold.dim_sellers s
# MAGIC     ON f.seller_key = s.seller_key
# MAGIC GROUP BY
# MAGIC     s.seller_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_olist_pranavi.gold.vw_seller_performance
# MAGIC ORDER BY total_revenue DESC;
