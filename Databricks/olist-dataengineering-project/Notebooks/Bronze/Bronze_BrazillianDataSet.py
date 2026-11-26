# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Imports
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import builtins
builtins.dbutils = dbutils

import importlib
import src.loaders as loaders
import src.Writers as writers
import config.Constants as constants

importlib.reload(loaders)
importlib.reload(writers)
importlib.reload(constants)

from src.loaders import load_csv
from src.Writers import write_delta

from config.Constants import *

business_date =  "2018-10-18"

print("Business date:", business_date)

# COMMAND ----------

orders = load_csv("orders",business_date)
order_items = load_csv("order_items",business_date)
customers = load_csv("customers",business_date)
order_payments = load_csv("order_payments",business_date)
order_reviews = load_csv("order_reviews",business_date)
products = load_csv("products",business_date)
sellers = load_csv("sellers",business_date)
product_category_name_translation = load_csv("product_category_name_translation",business_date)
geolocation = load_csv("geolocation",business_date)

# COMMAND ----------

# DBTITLE 1,Write bronze Delta tables
BRONZE_SCHEMA = "databricks_olist_pranavi.bronze"

write_delta(orders, f"{BRONZE_SCHEMA}.orders", layer="bronze")
write_delta(order_items, f"{BRONZE_SCHEMA}.order_items", layer="bronze")
write_delta(customers, f"{BRONZE_SCHEMA}.customers", layer="bronze")
write_delta(order_payments, f"{BRONZE_SCHEMA}.order_payments", layer="bronze")
write_delta(order_reviews, f"{BRONZE_SCHEMA}.order_reviews", layer="bronze")
write_delta(products, f"{BRONZE_SCHEMA}.products", layer="bronze")
write_delta(sellers, f"{BRONZE_SCHEMA}.sellers", layer="bronze")
write_delta(product_category_name_translation, f"{BRONZE_SCHEMA}.product_category_name_translation", layer="bronze")
write_delta(geolocation, f"{BRONZE_SCHEMA}.geolocation", layer="bronze")
