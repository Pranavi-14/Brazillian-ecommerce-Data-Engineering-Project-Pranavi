from pyspark.sql.functions import col, lit, when

# Referential-integrity rules module.
#
# DESIGN (project pattern): rules NEVER silently drop, filter, or fan out rows.
# They either (a) RETURN the violating rows via left_anti joins so the caller can
# quarantine them with a quarantine_reason, or (b) keep every row and FLAG broken links.
#
# Coverage map - checks that already live in their own silver pipelines (do not
# duplicate them here):
#   payments  -> enrich_with_orders + no_parent_order quarantine (order_payments caller)
#   reviews   -> enrich_with_orders + validate_referential_integrity (order_reviews.py)
#   orders    -> silver.orders is the parent source for every order-keyed check
#   customers -> customer_order_link_missing_flag (orders_customers.py)
#   products  -> category/translation flags + COALESCE fallback (order_products.py)
#   geography -> silver.geolocation_zip_lookup (one row per prefix, conflict-flagged)
#
# Applied by pipelines (rules below):
#   1. order_items -> get_items_without_* getters (order_items.py quarantines
#      orphans; parents kept via left_semi)
#   2. sellers -> flag_seller_geo_consistency (order_sellers.py flags prefix
#      coverage + declared-state consistency onto silver.sellers)
#   3. customers -> flag_customer_geo_consistency (orders_customers.py flags
#      prefix coverage + declared-state consistency onto silver.customers)

# ---------- Order-items referential integrity ----------

# Violation getters: return ONLY the rows whose parent is missing (quarantine candidates).

def get_items_without_orders(order_items_df, orders_df):
    return order_items_df.join(orders_df.select("order_id"), on="order_id", how="left_anti")

def get_items_without_products(order_items_df, products_df):
    return order_items_df.join(products_df.select("product_id"), on="product_id", how="left_anti")

def get_items_without_sellers(order_items_df, sellers_df):
    return order_items_df.join(sellers_df.select("seller_id"), on="seller_id", how="left_anti")

# Keep-and-flag variant: every item survives; each broken link gets its own flag
# (null-safe - an item can be missing more than one parent at a time).

def flag_items_referential_integrity(order_items_df, orders_df, products_df, sellers_df):
    df = order_items_df.join(
        orders_df.select("order_id").withColumn("_has_order", lit(1)), on="order_id", how="left"
    )
    df = df.join(
        products_df.select("product_id").withColumn("_has_product", lit(1)), on="product_id", how="left"
    )
    df = df.join(
        sellers_df.select("seller_id").withColumn("_has_seller", lit(1)), on="seller_id", how="left"
    )
    return (
        df.withColumn("item_order_link_missing_flag", when(col("_has_order").isNull(), lit(1)).otherwise(lit(0)))
          .withColumn("item_product_link_missing_flag", when(col("_has_product").isNull(), lit(1)).otherwise(lit(0)))
          .withColumn("item_seller_link_missing_flag", when(col("_has_seller").isNull(), lit(1)).otherwise(lit(0)))
          .drop("_has_order", "_has_product", "_has_seller")
    )

# ---------- Geolocation consistency (join checks vs silver.geolocation_zip_lookup) ----------
# Pass zip_lookup_df = silver.geolocation_zip_lookup: exactly one row per prefix with
# rep_city / rep_state (documented mode labels), so these joins can never fan out rows.

# Unmatched-prefix getters: rows whose ZIP prefix has no geolocation coverage at all.

def get_customers_without_geolocation_match(customers_df, zip_lookup_df):
    geo = zip_lookup_df.select("geolocation_zip_code_prefix")
    return customers_df.join(
        geo,
        customers_df["customer_zip_code_prefix"] == geo["geolocation_zip_code_prefix"],
        how="left_anti"
    )

def get_sellers_without_geolocation_match(sellers_df, zip_lookup_df):
    geo = zip_lookup_df.select("geolocation_zip_code_prefix")
    return sellers_df.join(
        geo,
        sellers_df["seller_zip_code_prefix"] == geo["geolocation_zip_code_prefix"],
        how="left_anti"
    )

# State-mismatch getters: the record's state disagrees with the lookup's representative
# state for its prefix. These are INVESTIGATE signals (the lookup itself flags 8
# multi-state prefixes with state_label_conflict_flag=1) - never drop on them.

def get_customers_state_mismatch(customers_df, zip_lookup_df):
    geo = zip_lookup_df.select("geolocation_zip_code_prefix", "rep_state")
    joined = customers_df.join(
        geo,
        customers_df["customer_zip_code_prefix"] == geo["geolocation_zip_code_prefix"],
        how="left"
    )
    return joined.filter(
        col("rep_state").isNotNull() & (col("rep_state") != col("customer_state"))
    ).drop("geolocation_zip_code_prefix", "rep_state")

def get_sellers_state_mismatch(sellers_df, zip_lookup_df):
    geo = zip_lookup_df.select("geolocation_zip_code_prefix", "rep_state")
    joined = sellers_df.join(
        geo,
        sellers_df["seller_zip_code_prefix"] == geo["geolocation_zip_code_prefix"],
        how="left"
    )
    return joined.filter(
        col("rep_state").isNotNull() & (col("rep_state") != col("seller_state"))
    ).drop("geolocation_zip_code_prefix", "rep_state")

# Keep-and-flag variant of the two checks above (applied by order_sellers.py so
# the signals live on silver.sellers, not just in the report-only harness).
# One row per prefix in the lookup — the join never fans out. Null-safe: a
# seller with no lookup row gets only the missing-prefix flag.
#   seller_geo_prefix_missing_flag — prefix has no geolocation coverage at all
#   seller_state_geo_mismatch_flag — declared state differs from the lookup's
#                                    representative (mode) state for the prefix
def flag_seller_geo_consistency(sellers_df, zip_lookup_df):
    geo = zip_lookup_df.select(
        col("geolocation_zip_code_prefix").alias("_geo_prefix"),
        col("rep_state").alias("_rep_state"),
    )
    df = sellers_df.join(
        geo,
        sellers_df["seller_zip_code_prefix"] == geo["_geo_prefix"],
        how="left",
    )
    return (
        df.withColumn("seller_geo_prefix_missing_flag",
                when(col("_geo_prefix").isNull(),lit(1)).otherwise(lit(0)))
          .withColumn("seller_state_geo_mismatch_flag",
                when(col("_geo_prefix").isNotNull() & (col("_rep_state") != col("seller_state")),lit(1)).otherwise(lit(0)))
          .drop("_geo_prefix", "_rep_state")
    )

# Customer counterpart of flag_seller_geo_consistency (applied by orders_customers.py
# so the signals live on silver.customers). Same null-safe, no-fan-out join.
#   customer_geo_prefix_missing_flag — prefix has no geolocation coverage at all
#   customer_state_geo_mismatch_flag — declared state differs from the lookup's
#                                      representative (mode) state for the prefix
def flag_customer_geo_consistency(customers_df, zip_lookup_df):
    geo = zip_lookup_df.select(
        col("geolocation_zip_code_prefix").alias("_geo_prefix"),
        col("rep_state").alias("_rep_state"),
    )
    df = customers_df.join(
        geo,
        customers_df["customer_zip_code_prefix"] == geo["_geo_prefix"],
        how="left",
    )
    return (
        df.withColumn("customer_geo_prefix_missing_flag",
                when(col("_geo_prefix").isNull(),lit(1)).otherwise(lit(0)))
          .withColumn("customer_state_geo_mismatch_flag",
                when(col("_geo_prefix").isNotNull() & (col("_rep_state") != col("customer_state")),lit(1)).otherwise(lit(0)))
          .drop("_geo_prefix", "_rep_state")
    )
