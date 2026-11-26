# Databricks notebook source
# DBTITLE 1,Create vw_product_performance view
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC databricks_olist_pranavi.gold.vw_product_performance
# MAGIC COMMENT 'Product category performance based on sales, quantity and orders'
# MAGIC AS
# MAGIC SELECT
# MAGIC     p.product_category_name,
# MAGIC     COALESCE(p.product_category_name_english, p.product_category_name) AS product_category_name_english,
# MAGIC     COUNT(*) AS units_sold,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC     ROUND(SUM(f.price), 2) AS product_revenue,
# MAGIC     ROUND(SUM(f.freight_value), 2) AS freight_revenue,
# MAGIC     ROUND(SUM(f.item_total_value), 2) AS total_revenue,
# MAGIC     ROUND(AVG(f.price), 2) AS average_product_price
# MAGIC FROM databricks_olist_pranavi.gold.fact_sales f
# MAGIC JOIN databricks_olist_pranavi.gold.dim_products p
# MAGIC     ON f.product_key = p.product_key
# MAGIC GROUP BY
# MAGIC     p.product_category_name,
# MAGIC     COALESCE(p.product_category_name_english, p.product_category_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_olist_pranavi.gold.vw_product_performance;
