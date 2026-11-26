from src.loaders import read_table

import transformations.silver.order_sellers as sellers_tf
import transformations.rules.Referal_Integrity as rules
import src.Writers as writers_module
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

sellers_df = read_table("bronze.sellers")
orders_df = read_table("silver.orders")
order_items_df = read_table("silver.order_items")
zip_lookup_df = read_table("silver.geolocation_zip_lookup")
print("before",sellers_df.count())

# ---------- Phase 1: table-local cleaning (sellers_table_readme.md) ----------
sellers_df = sellers_tf.trimColumns(sellers_df)
print("After trimColumns ",sellers_df.count())
sellers_df = sellers_tf.dropduplicates(sellers_df)
print("After dropduplicates ",sellers_df.count())
sellers_df = sellers_tf.dropnulls(sellers_df)
print("After dropnulls ",sellers_df.count())

# Quarantine keeps every violation; MERGE on (seller_id, quarantine_reason)
# so reruns are idempotent — no drop-first needed.
quarantine_columns = ["seller_id","seller_zip_code_prefix","seller_city","seller_state","ingested_at","source_file","load_date","quarantine_reason"]

zipcode_violations_df = sellers_tf.get_zipcode_violations(sellers_df).withColumn("quarantine_reason", lit("invalid_zipcode_prefix"))
zipcode_violations_count = zipcode_violations_df.count()
print("Sellers with invalid zip code prefix (quarantined):",zipcode_violations_count)
if zipcode_violations_count > 0:
    writers_module.write_delta(zipcode_violations_df.select(*quarantine_columns), "quarantine.sellers", layer="quarantine",
                               merge_key=["seller_id", "quarantine_reason"])
sellers_df = sellers_tf.validate_zipcode(sellers_df)
print("After validate_zipcode ",sellers_df.count())

state_violations_df = sellers_tf.get_state_code_violations(sellers_df).withColumn("quarantine_reason", lit("invalid_state_code"))
state_violations_count = state_violations_df.count()
print("Sellers with invalid state code (quarantined):",state_violations_count)
if state_violations_count > 0:
    writers_module.write_delta(state_violations_df.select(*quarantine_columns), "quarantine.sellers", layer="quarantine",
                              merge_key=["seller_id", "quarantine_reason"])
sellers_df = sellers_tf.validate_brazilian_state_code(sellers_df)
print("After validate_brazilian_state_code ",sellers_df.count())

# ---------- Phase 2: cross-table flags vs silver.orders / silver.order_items ----------
# README §4.2: order_items link is 1..N and NOT mandatory — zero-order sellers are kept and flagged.
sellers_df = sellers_tf.flag_seller_order_activity(sellers_df,orders_df,order_items_df)
print("After flag_seller_order_activity ",sellers_df.count())

sellers_df.groupBy("seller_with_zero_orders","seller_with_100_cancellation_rate").count().show()

# ---------- Phase 3: geolocation consistency flags vs silver.geolocation_zip_lookup ----------
# Rule from Referal_Integrity.py (same module the harness reads its getters from —
# one source of truth). Investigate signals (never drop): 35 sellers declare a
# state absent from every geolocation observation of their prefix (registration-
# vs-location mismatch, mostly 'SP'); 7 prefixes have no coverage at all.
sellers_df = rules.flag_seller_geo_consistency(sellers_df, zip_lookup_df)
print("After flag_seller_geo_consistency ",sellers_df.count())
sellers_df.groupBy("seller_geo_prefix_missing_flag","seller_state_geo_mismatch_flag").count().show()

# MERGE on seller_id (unique PK) — reruns update matched rows and insert new ones.
writers_module.write_delta(sellers_df, "silver.sellers", layer="silver", merge_key="seller_id")
