# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Monthly Sales View
# MAGIC
# MAGIC Creates `gold.vw_monthly_sales` from `gold.fact_sales` and `gold.dim_dates`, showing revenue and number of orders per month (by purchase date).

# COMMAND ----------

# DBTITLE 1,Create monthly sales view
# MAGIC %sql
# MAGIC -- Revenue = item_total_value (price + freight); product_revenue = price only.
# MAGIC -- fact_sales is at order-item grain, so orders are counted DISTINCT.
# MAGIC CREATE OR REPLACE VIEW databricks_olist_pranavi.gold.vw_monthly_sales
# MAGIC COMMENT 'Monthly revenue and number of orders, based on order purchase date'
# MAGIC AS
# MAGIC SELECT
# MAGIC   d.year,
# MAGIC   d.month,
# MAGIC   make_date(d.year, d.month, 1)            AS month_start,
# MAGIC   COUNT(DISTINCT f.order_id)               AS no_of_orders,
# MAGIC   ROUND(SUM(f.item_total_value), 2)        AS total_revenue,
# MAGIC   ROUND(SUM(f.price), 2)                   AS product_revenue,
# MAGIC   ROUND(SUM(f.freight_value), 2)           AS freight_revenue
# MAGIC FROM databricks_olist_pranavi.gold.fact_sales f
# MAGIC JOIN databricks_olist_pranavi.gold.dim_dates d
# MAGIC   ON f.date_key = d.date_key
# MAGIC GROUP BY d.year, d.month

# COMMAND ----------

# DBTITLE 1,Preview monthly sales
# MAGIC %sql
# MAGIC SELECT * FROM databricks_olist_pranavi.gold.vw_monthly_sales ORDER BY month_start
