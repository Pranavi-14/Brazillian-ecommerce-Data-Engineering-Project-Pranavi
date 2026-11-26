# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Build workspace.gold.dim_dates
# import sys
# sys.path.insert(0, "/Workspace/Repos/BrazilianEcommerce/brazilian-ecommerce-data-pipeline")

from pyspark.sql import functions as F


start_date = "2016-09-04"
end_date = "2018-11-12"

spark.sql("CREATE SCHEMA IF NOT EXISTS databricks_olist_pranavi.gold")

df_Dim_dates = (
    spark.sql(f"""
        SELECT explode(
            sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)
        ) AS fulldate
    """)
    .select(
        F.date_format("fulldate", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("fulldate"),
        F.dayofmonth("fulldate").alias("day"),
        F.month("fulldate").alias("month"),
        F.year("fulldate").alias("year"),
        F.quarter("fulldate").alias("quarter"),
    )
)

# Create dim_dates only if it does not exist yet - reruns leave the existing table untouched
if spark.catalog.tableExists("databricks_olist_pranavi.gold.dim_dates"):
    print("gold.dim_dates already exists - skipped creation.")
else:
    df_Dim_dates.write.saveAsTable("databricks_olist_pranavi.gold.dim_dates")
    print("Created gold.dim_dates.")

display(spark.read.table("databricks_olist_pranavi.gold.dim_dates").orderBy("date_key"))



# COMMAND ----------

# DBTITLE 1,SCD2 merge: silver.products into gold.dim_products
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import Window
from pyspark.sql import functions as F
from src.Writers import write_delta

# --- SCD Type 2 merge: silver.products -> gold.dim_products -----------------
# Match key: (product_id, effective_from_date)
#   initial load (table missing) -> every product inserted once, current
#       version effective from the dataset start date (2016-09-04)
#   changed product -> 2 staging records:
#       close old version (effective_to_date = load_date - 1 day)
#       + insert new version (effective_from_date = load_date,
#                             effective_to_date = 9999-12-31, NEW product_key)
#   new product -> 1 staging record (effective_from_date = load_date)
#   unchanged product -> skipped

TRACKED_COLS = ["product_category_name", "product_category_name_english"]
END_OF_TIME = F.to_date(F.lit("9999-12-31"))
# dataset start (same as dim_dates) — the initial version covers every fact date
INITIAL_FROM_DATE = F.to_date(F.lit("2016-09-04"))

df_silver = (
    spark.read.table("databricks_olist_pranavi.silver.products")
    .select("product_id", *TRACKED_COLS, "load_date")
)

# does the Gold table exist yet? (same check style as Writers._table_exists)
gold_exists = True
try:
    spark.sql("DESCRIBE TABLE databricks_olist_pranavi.gold.dim_products").collect()
except Exception:
    gold_exists = False

if not gold_exists:
    # ---- Initial load: every product is new, one current version each ----
    df_staging = (
        df_silver.select("product_id", *TRACKED_COLS)
        .withColumn(
            "product_key",
            F.row_number().over(Window.orderBy("product_id")).cast("int"),
        )
        .withColumn("effective_from_date", INITIAL_FROM_DATE)
        .withColumn("effective_to_date", END_OF_TIME)
    )
    write_delta(
        df_staging,
        "gold.dim_products",
        layer="gold",
        merge_key=["product_id", "effective_from_date"],
    )
    print(f"Initial load created gold.dim_products with {df_staging.count()} products.")

else:
    df_gold = spark.read.table("databricks_olist_pranavi.gold.dim_products")
    df_gold_current = df_gold.filter(F.col("effective_to_date") == END_OF_TIME)

    # load_date of the incoming batch (the data's own load date — never current_date)
    load_date = df_silver.agg(F.max("load_date")).head()[0]

    # 1) Classify incoming products against the current (open) Gold version
    #    (full outer: products in Gold but missing from Silver are reported,
    #     never deleted — MERGE never removes rows)
    df_diff = (
        df_silver.select("product_id", *TRACKED_COLS)
        .withColumn("_in_silver", F.lit(True)).alias("s")
        .join(
            df_gold_current
            .select("product_id", "product_key", "effective_from_date", *TRACKED_COLS)
            .alias("g"),
            on="product_id",
            how="full_outer",
        )
        .select(
            "product_id",
            F.coalesce("_in_silver", F.lit(False)).alias("in_silver"),
            F.col("s.product_category_name").alias("new_category_pt"),
            F.col("s.product_category_name_english").alias("new_category_en"),
            F.col("g.product_category_name").alias("old_category_pt"),
            F.col("g.product_category_name_english").alias("old_category_en"),
            "product_key",
            "effective_from_date",
        )
    )

    # null-safe comparison (eqNullSafe) so NULL == NULL categories count as unchanged
    is_new = F.col("in_silver") & F.col("product_key").isNull()
    is_changed = (
        F.col("in_silver")
        & F.col("product_key").isNotNull()
        & ~(
            F.col("new_category_pt").eqNullSafe(F.col("old_category_pt"))
            & F.col("new_category_en").eqNullSafe(F.col("old_category_en"))
        )
    )

    df_summary = df_diff.agg(
        F.sum(is_new.cast("int")).alias("new_products"),
        F.sum(is_changed.cast("int")).alias("changed_products"),
        F.sum((~F.col("in_silver")).cast("int")).alias("in_gold_not_in_silver"),
    )
    display(df_summary)

    df_new = df_diff.filter(is_new)
    df_changed = df_diff.filter(is_changed)

    # 2a) Close the old version of every changed product
    #     (matched on the old version's effective_from_date -> update only
    #      effective_to_date; old product_key and categories are carried through)
    df_stage_close = df_changed.select(
        "product_id",
        F.col("old_category_pt").alias("product_category_name"),
        F.col("old_category_en").alias("product_category_name_english"),
        "product_key",          # old version keeps its surrogate key
        "effective_from_date",  # old version's start date (the match key)
        F.date_sub(F.lit(load_date), 1).alias("effective_to_date"),
    )

    # 2b) New versions for changed products + first version for new products
    max_key = df_gold.agg(F.max("product_key")).head()[0]
    df_stage_new = (
        df_changed.select(
            "product_id",
            F.col("new_category_pt").alias("product_category_name"),
            F.col("new_category_en").alias("product_category_name_english"),
        )
        .unionByName(
            df_new.select(
                "product_id",
                F.col("new_category_pt").alias("product_category_name"),
                F.col("new_category_en").alias("product_category_name_english"),
            )
        )
        .withColumn(
            "product_key",
            (F.lit(max_key) + F.row_number().over(Window.orderBy("product_id"))).cast("int"),
        )
        .withColumn("effective_from_date", F.lit(load_date))
        .withColumn("effective_to_date", END_OF_TIME)
    )

    df_staging = (
        df_stage_close.unionByName(df_stage_new)
        .select(
            "product_id", "product_category_name", "product_category_name_english",
            "product_key", "effective_from_date", "effective_to_date",
        )
    )

    print(f"load_date = {load_date} | staging records = {df_staging.count()}")

    # 3) MERGE staging into Gold on the composite key
    #    matched (product_id, effective_from_date) -> update (closes old versions)
    #    unmatched                                -> insert (new versions / products)
    if not df_staging.isEmpty():
        write_delta(
            df_staging,
            "gold.dim_products",
            layer="gold",
            merge_key=["product_id", "effective_from_date"],
        )
    else:
        print("No new or changed products — gold.dim_products is already up to date.")


# COMMAND ----------

# DBTITLE 1,SCD3 merge: silver.sellers into gold.dim_sellers
# import sys
# sys.path.insert(0, "/Workspace/Repos/BrazilianEcommerce/brazilian-ecommerce-data-pipeline")

from pyspark.sql import Window
from pyspark.sql import functions as F
from src.Writers import write_delta

# --- SCD Type 3 merge: silver.sellers -> gold.dim_sellers --------------------
# One row per seller forever: seller_key never changes, MERGE key = seller_id.
#   changed seller -> update IN PLACE:
#       current_<attr>  = new silver value
#       previous_<attr> = old gold value, only for the attribute(s) that
#                         changed; unchanged attributes keep their existing
#                         previous_<attr>
#   new seller     -> insert with previous_<attr> = null
#   unchanged      -> skipped
#   in gold but missing from silver -> reported, never deleted
# (SCD3 keeps exactly ONE level of history — a later change overwrites the
#  previous value. Full version history = dim_products' SCD2 pattern.)

TRACKED_COLS = ["seller_city", "seller_state", "seller_zip_code_prefix"]
PREV_COLS = [f"previous_{c}" for c in TRACKED_COLS]

df_silver = spark.read.table("databricks_olist_pranavi.silver.sellers").select("seller_id", *TRACKED_COLS)

# does the Gold table exist yet? (same check style as Writers._table_exists)
gold_exists = True
try:
    spark.sql("DESCRIBE TABLE databricks_olist_pranavi.gold.dim_sellers").collect()
except Exception:
    gold_exists = False

if not gold_exists:
    # ---- Initial load: every seller is new, previous_* = null ----
    df_staging = (
        df_silver
        .withColumn("seller_key", F.row_number().over(Window.orderBy("seller_id")).cast("int"))
        .select("seller_key", "seller_id", *TRACKED_COLS)
    )
    for c in TRACKED_COLS:
        df_staging = df_staging.withColumn(f"previous_{c}", F.lit(None).cast("string"))
    df_staging = df_staging.select("seller_key", "seller_id", *TRACKED_COLS, *PREV_COLS)

    write_delta(df_staging, "gold.dim_sellers", layer="gold", merge_key="seller_id")
    print(f"Initial load created gold.dim_sellers with {df_staging.count()} sellers.")

else:
    # ---- Subsequent loads: classify vs Gold, update in place ----
    df_gold = spark.read.table("databricks_olist_pranavi.gold.dim_sellers")

    df_diff = (
        df_silver.withColumn("_in_silver", F.lit(True)).alias("s")
        .join(
            df_gold.select("seller_id", "seller_key", *TRACKED_COLS, *PREV_COLS).alias("g"),
            on="seller_id",
            how="full_outer",
        )
        .select(
            "seller_id",
            F.coalesce("_in_silver", F.lit(False)).alias("in_silver"),
            *[F.col(f"s.{c}").alias(f"new_{c}") for c in TRACKED_COLS],
            *[F.col(f"g.{c}").alias(f"old_{c}") for c in TRACKED_COLS],
            "seller_key",
            *[F.col(f"g.{pc}").alias(pc) for pc in PREV_COLS],
        )
    )
    # 1   pqrs  PQR  12345  true
    # 3   ghj   GHJ   29201 true

    # 101   1     pqr    PQR    12345     null  null  null
    # 102  2     abc    ABC    83899    null  null  null

    # 1 true  pqrs  PQR  12345 pqr    PQR    12345  null  null  null
    # 2 false null null  null  abc    ABC    83899   null  null  null
    # 3 true  ghj   GHJ  29201 null   null   null    null  null  null

    #sellerid  in_silver new_seller_city new_seller_state new_Seller_zip
    #old_seller_city old_seller_state old_Seller_zip
    #seller_key
    #previous_seller_city previous_seller_state previous_Seller_zip
    #no change -- same_all true

    # null-safe comparison (eqNullSafe) so NULL == NULL counts as unchanged
    same_all = F.lit(True)
    for c in TRACKED_COLS:
        same_all = same_all & F.col(f"new_{c}").eqNullSafe(F.col(f"old_{c}"))
    is_new = F.col("in_silver") & F.col("seller_key").isNull()
    is_changed = F.col("in_silver") & F.col("seller_key").isNotNull() & ~same_all

    display(df_diff.agg(
        F.sum(is_new.cast("int")).alias("new_sellers"),
        F.sum(is_changed.cast("int")).alias("changed_sellers"),
        F.sum((~F.col("in_silver")).cast("int")).alias("in_gold_not_in_silver"),
    ))

    # changed sellers: current = new values; previous = old value for changed
    # attributes, existing previous value for unchanged attributes
    df_stage_changed = df_diff.filter(is_changed).select(
        "seller_key", "seller_id",
        *[F.col(f"new_{c}").alias(c) for c in TRACKED_COLS],
        *[F.when(F.col(f"new_{c}").eqNullSafe(F.col(f"old_{c}")), F.col(f"previous_{c}"))
           .otherwise(F.col(f"old_{c}")).alias(f"previous_{c}") for c in TRACKED_COLS],
    )

    # new sellers: first version, previous_* = null, new seller_key
    max_key = df_gold.agg(F.max("seller_key")).head()[0]
    df_stage_new = (
        df_diff.filter(is_new)
        .select("seller_id", *[F.col(f"new_{c}").alias(c) for c in TRACKED_COLS])
        .withColumn("seller_key", (F.lit(max_key) + F.row_number().over(Window.orderBy("seller_id"))).cast("int"))
    )
    for c in TRACKED_COLS:
        df_stage_new = df_stage_new.withColumn(f"previous_{c}", F.lit(None).cast("string"))

    df_staging = df_stage_changed.unionByName(df_stage_new).select(
        "seller_key", "seller_id", *TRACKED_COLS, *PREV_COLS)

    print(f"staging records = {df_staging.count()}")

    # MERGE on seller_id: matched -> update in place, unmatched -> insert
    if not df_staging.isEmpty():
        write_delta(df_staging, "gold.dim_sellers", layer="gold", merge_key="seller_id")
    else:
        print("No new or changed sellers — gold.dim_sellers is already up to date.")


# COMMAND ----------

# import sys
# sys.path.insert(0, "/Workspace/Repos/BrazilianEcommerce/brazilian-ecommerce-data-pipeline")

from pyspark.sql import Window
from pyspark.sql import functions as F
from src.Writers import write_delta

# --- SCD Type 2 merge: silver.customers -> gold.dim_customers -----------------
# Match key: (customer_id, effective_from_date)
#   initial load (table missing) -> every customer inserted once, current
#       version effective from the dataset start date (2016-09-04)
#   changed customer -> 2 staging records:
#       close old version (effective_to_date = load_date - 1 day)
#       + insert new version (effective_from_date = load_date,
#                             effective_to_date = 9999-12-31, NEW customer_key)
#   new customer -> 1 staging record (effective_from_date = load_date)
#   unchanged customer -> skipped

TRACKED_COLS = ["customer_city", "customer_state", "customer_zip_code_prefix"]
END_OF_TIME = F.to_date(F.lit("9999-12-31"))
# dataset start (same as dim_dates) — the initial version covers every fact date
INITIAL_FROM_DATE = F.to_date(F.lit("2016-09-04"))

df_silver = (
    spark.read.table("databricks_olist_pranavi.silver.customers")
    .select("customer_id","customer_unique_id", *TRACKED_COLS, "load_date")
)

# does the Gold table exist yet? (same check style as Writers._table_exists)
gold_exists = True
try:
    spark.sql("DESCRIBE TABLE databricks_olist_pranavi.gold.dim_customers").collect()
except Exception:
    gold_exists = False

if not gold_exists:
    # ---- Initial load: every customer is new, one current version each ----
    df_staging = (
        df_silver.select("customer_id", "customer_unique_id", *TRACKED_COLS)
        .withColumn(
            "customer_key",
            F.row_number().over(Window.orderBy("customer_id")).cast("int"),
        )
        .withColumn("effective_from_date", INITIAL_FROM_DATE)
        .withColumn("effective_to_date", END_OF_TIME)
    )
    write_delta(
        df_staging,
        "gold.dim_customers",
        layer="gold",
        merge_key=["customer_id", "effective_from_date"],
    )
    print(f"Initial load created gold.dim_customers with {df_staging.count()} customers.")

else:
    df_gold = spark.read.table("databricks_olist_pranavi.gold.dim_customers")
    df_gold_current = df_gold.filter(F.col("effective_to_date") == END_OF_TIME)

    # load_date of the incoming batch (the data's own load date — never current_date)
    load_date = df_silver.agg(F.max("load_date")).head()[0]

    # 1) Classify incoming customers against the current (open) Gold version
    #    (full outer: customers in Gold but missing from Silver are reported,
    #     never deleted — MERGE never removes rows)
    df_diff = (
        df_silver.select("customer_id","customer_unique_id", *TRACKED_COLS)
        .withColumn("_in_silver", F.lit(True)).alias("s")
        .join(
            df_gold_current
            .select("customer_id","customer_unique_id", "customer_key", "effective_from_date", *TRACKED_COLS)
            .alias("g"),
            on="customer_id",
            how="full_outer",
        )
        .select(
            "customer_id",
            F.coalesce("_in_silver", F.lit(False)).alias("in_silver"),
            F.col("s.customer_unique_id").alias("new_unique_id"),
            F.col("g.customer_unique_id").alias("old_unique_id"),
            F.col("s.customer_city").alias("new_customer_city"),
            F.col("s.customer_state").alias("new_customer_state"),
            F.col("s.customer_zip_code_prefix").alias("new_customer_zip_code_prefix"),
            F.col("g.customer_city").alias("old_customer_city"),
            F.col("g.customer_state").alias("old_customer_state"),
            F.col("g.customer_zip_code_prefix").alias("old_customer_zip_code_prefix"),
            "customer_key",
            "effective_from_date",
        )
    )

    # null-safe comparison (eqNullSafe) so NULL == NULL categories count as unchanged
    is_new = F.col("in_silver") & F.col("customer_key").isNull()
    is_changed = (
        F.col("in_silver")
        & F.col("customer_key").isNotNull()
        & ~(
            F.col("new_customer_city").eqNullSafe(F.col("old_customer_city"))
            & F.col("new_customer_state").eqNullSafe(F.col("old_customer_state"))
            & F.col("new_customer_zip_code_prefix").eqNullSafe(F.col("old_customer_zip_code_prefix"))
        )
    )

    df_summary = df_diff.agg(
        F.sum(is_new.cast("int")).alias("new_customers"),
        F.sum(is_changed.cast("int")).alias("changed_customers"),
        F.sum((~F.col("in_silver")).cast("int")).alias("in_gold_not_in_silver"),
    )
    display(df_summary)

    df_new = df_diff.filter(is_new)
    df_changed = df_diff.filter(is_changed)

    # 2a) Close the old version of every changed customer
    #     (matched on the old version's effective_from_date -> update only
    #      effective_to_date; old customer_key and categories are carried through)
    df_stage_close = df_changed.select(
        "customer_id",
        F.col("old_unique_id").alias("customer_unique_id"),
        F.col("old_customer_city").alias("customer_city"),
        F.col("old_customer_state").alias("customer_state"),
        F.col("old_customer_zip_code_prefix").alias("customer_zip_code_prefix"),
        "customer_key",          # old version keeps its surrogate key
        "effective_from_date",  # old version's start date (the match key)
        F.date_sub(F.lit(load_date), 1).alias("effective_to_date"),
    )

    # 2b) New versions for changed customers + first version for new customers
    max_key = df_gold.agg(F.max("customer_key")).head()[0]
    df_stage_new = (
        df_changed.select(
            "customer_id",
            F.col("new_unique_id").alias("customer_unique_id"),
            F.col("new_customer_city").alias("customer_city"),
            F.col("new_customer_state").alias("customer_state"),
            F.col("new_customer_zip_code_prefix").alias("customer_zip_code_prefix")
        )
        .unionByName(
            df_new.select(
                "customer_id",
                F.col("new_unique_id").alias("customer_unique_id"),
                F.col("new_customer_city").alias("customer_city"),
                F.col("new_customer_state").alias("customer_state"),
                F.col("new_customer_zip_code_prefix").alias("customer_zip_code_prefix")
            )
        )
        .withColumn(
            "customer_key",
            (F.lit(max_key) + F.row_number().over(Window.orderBy("customer_id"))).cast("int"),
        )
        .withColumn("effective_from_date", F.lit(load_date))
        .withColumn("effective_to_date", END_OF_TIME)
    )

    df_staging = (
        df_stage_close.unionByName(df_stage_new)
        .select(
            "customer_id","customer_unique_id", "customer_city", "customer_state","customer_zip_code_prefix",
            "customer_key", "effective_from_date", "effective_to_date",
        )
    )

    print(f"load_date = {load_date} | staging records = {df_staging.count()}")

    # 3) MERGE staging into Gold on the composite key
    #    matched (customer_id, effective_from_date) -> update (closes old versions)
    #    unmatched                                -> insert (new versions / customers)
    if not df_staging.isEmpty():
        write_delta(
            df_staging,
            "gold.dim_customers",
            layer="gold",
            merge_key=["customer_id", "effective_from_date"],
        )
    else:
        print("No new or changed customers — gold.dim_customers is already up to date.")


# COMMAND ----------

# DBTITLE 1,Build gold.fact_sales (order-item grain)
# import sys
# sys.path.insert(0, "/Workspace/Repos/BrazilianEcommerce/brazilian-ecommerce-data-pipeline")

from pyspark.sql import functions as F
from src.Writers import write_delta

# --- Build gold.fact_sales (order-item grain) --------------------------------
# Grain: one row per order item line -> (order_id, order_item_id).
#   order_id / order_item_id : degenerate dimension keys (the grain), not FKs
#   date_key                 : FK dim_dates    (order purchase date)
#   customer_key             : FK dim_customers (SCD2 — version valid ON the purchase date)
#   product_key              : FK dim_products  (SCD2 — version valid ON the purchase date)
#   seller_key               : FK dim_sellers   (SCD3 — stable one-row key)
# Measures: price, freight_value, item_total_value (all from silver.order_items)

df_items = (
    spark.read.table("databricks_olist_pranavi.silver.order_items")
    .select(
        "order_id", "order_item_id", "product_id", "seller_id",
        "price", "freight_value", "item_total_value",
    )
)

# order context: which customer bought, and when
df_order_ctx = (
    spark.read.table("databricks_olist_pranavi.silver.orders")
    .select("order_id", "customer_id", F.to_date("order_purchase_timestamp").alias("purchase_date"))
)

df_dim_dates = spark.read.table("databricks_olist_pranavi.gold.dim_dates")
df_dim_products = spark.read.table("databricks_olist_pranavi.gold.dim_products")
df_dim_sellers = spark.read.table("databricks_olist_pranavi.gold.dim_sellers")
df_dim_customers = spark.read.table("databricks_olist_pranavi.gold.dim_customers")

df_fact_sales = (
    df_items
    .join(df_order_ctx, on="order_id", how="inner")
    # date_key: FK to dim_dates on the purchase date
    .join(
        df_dim_dates.select(F.col("date_key"), F.col("fulldate").alias("purchase_date")),
        on="purchase_date",
        how="inner",
    )
    # product_key: SCD2 point-in-time join — version valid on the purchase date
    .join(
        df_dim_products.select(
            F.col("product_id").alias("p_id"), "product_key",
            F.col("effective_from_date").alias("p_from"),
            F.col("effective_to_date").alias("p_to"),
        ),
        on=[
            F.col("product_id") == F.col("p_id"),
            F.col("purchase_date").between(F.col("p_from"), F.col("p_to")),
        ],
        how="inner",
    )
    # customer_key: SCD2 point-in-time join — version valid on the purchase date
    .join(
        df_dim_customers.select(
            F.col("customer_id").alias("c_id"), "customer_key",
            F.col("effective_from_date").alias("c_from"),
            F.col("effective_to_date").alias("c_to"),
        ),
        on=[
            F.col("customer_id") == F.col("c_id"),
            F.col("purchase_date").between(F.col("c_from"), F.col("c_to")),
        ],
        how="inner",
    )
    # seller_key: SCD3 — one row per seller, stable key
    .join(
        df_dim_sellers.select(F.col("seller_id").alias("s_id"), "seller_key"),
        on=F.col("seller_id") == F.col("s_id"),
        how="inner",
    )
    .select(
        "order_id", "order_item_id", "date_key",
        "customer_key", "product_key", "seller_key",
        "price", "freight_value", "item_total_value",
    )
)

# idempotent MERGE on the grain — rerun updates matched lines, inserts new ones
write_delta(
    df_fact_sales,
    "gold.fact_sales",
    layer="gold",
    merge_key=["order_id", "order_item_id"],
)

print(f"fact_sales rows = {df_fact_sales.count()}")
display(df_fact_sales.orderBy("order_id", "order_item_id").limit(20))
