from src.loaders import read_table

import transformations.silver.orders_customers as customers_tf
import transformations.rules.Referal_Integrity as rules
import src.Writers as writers_module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.getOrCreate()

customers_df = read_table("bronze.customers")
orders_df = read_table("silver.orders")
# Requires geolocation.py to have run first (builds silver.geolocation_zip_lookup)
zip_lookup_df = read_table("silver.geolocation_zip_lookup")
print("before", customers_df.count())

# ---------- Phase 1: table-local cleaning (customer READMEs) ----------
# README: zip prefix stays TEXT — no int cast, leading zeroes preserved (24% of rows)
customers_df = customers_tf.trimColumns(customers_df)
print("After trimColumns ", customers_df.count())
customers_df = customers_tf.dropnulls(customers_df)
print("After dropnulls ", customers_df.count())
customers_df = customers_tf.dropduplicates(customers_df)
print("After dropduplicates ", customers_df.count())
customers_df = customers_tf.validate_brazilian_state_code(customers_df)
print("After validate_brazilian_state_code ", customers_df.count())

# ---------- Quarantine (expect 0 — data verified clean) ----------
zip_violations = customers_tf.get_zip_prefix_violations(customers_df)
state_violations = customers_tf.get_state_code_violations(customers_df)
print("zip prefix violations:", zip_violations.count(), "| state code violations:", state_violations.count())

quarantine_columns = ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                      "customer_city", "customer_state", "ingested_at", "source_file",
                      "load_date", "quarantine_reason"]

def quarantine_batch(violations_df, reason):
    if violations_df.count() > 0:
        writers_module.write_delta(
            violations_df.withColumn("quarantine_reason", lit(reason)).select(quarantine_columns),
            "quarantine.customers",
            layer="quarantine",
            merge_key=["customer_id", "quarantine_reason"]
        )

quarantine_batch(zip_violations, "invalid_zip_prefix")
quarantine_batch(state_violations, "invalid_state_code")

# ---------- Phase 2: enrichment (orders link + repeat-buyer) ----------
# README §5: customer_id joins orders 1:1; 1,403 customers' orders were dropped by the
# orders silver pipeline — kept and flagged. README §4/§7: repeat stats via customer_unique_id.
customers_df = customers_tf.enrich_with_orders(customers_df, orders_df)
print("After enrich_with_orders ", customers_df.count())
customers_df = customers_tf.enrich_with_repeat_stats(customers_df)
print("After enrich_with_repeat_stats ", customers_df.count())

customers_df.groupBy("customer_order_link_missing_flag", "customer_repeat_buyer_flag").count().show()
customers_df.groupBy("customer_total_orders").count().orderBy("customer_total_orders").show(20)

# ---------- Phase 3: geolocation consistency flags vs silver.geolocation_zip_lookup ----------
# Investigate signals (never drop): prefixes with no geolocation coverage, and
# declared states that differ from the lookup's representative state.
customers_df = rules.flag_customer_geo_consistency(customers_df, zip_lookup_df)
print("After flag_customer_geo_consistency ", customers_df.count())
customers_df.groupBy("customer_geo_prefix_missing_flag", "customer_state_geo_mismatch_flag").count().show()

# ---------- Write ----------
# MERGE on customer_id (unique PK) — reruns update matched rows and insert new ones.
writers_module.write_delta(customers_df, "silver.customers", layer="silver", merge_key="customer_id")
