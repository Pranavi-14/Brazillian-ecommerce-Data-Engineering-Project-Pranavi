from src.loaders import read_table

import transformations.silver.order_payments as payments_tf
import src.Writers as writers_module
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

payments_df = read_table("bronze.order_payments")
orders_df = read_table("silver.orders")
orders_items_df = read_table("silver.order_items")
print("before",payments_df.count())

# ---------- Phase 1: table-local cleaning (order_payments_table_readme.md) ----------
payments_df = payments_tf.changeSchema(payments_df)
print("After changeSchema ",payments_df.count())
payments_df = payments_tf.drop_nulls(payments_df)
print("After drop_nulls ",payments_df.count())
payments_df = payments_tf.trim_payment_types(payments_df)
print("After trim_payment_types ",payments_df.count())
payments_df = payments_tf.drop_duplicates(payments_df)
print("After drop_duplicates ",payments_df.count())
payments_df = payments_tf.validate_payment_types(payments_df)
print("After validate_payment_types ",payments_df.count())
payments_df = payments_tf.validate_payment_installments(payments_df)
print("After validate_payment_installments ",payments_df.count())
# Both quarantine batches are written to the same table — keep one shared schema.
# (Violations are quarantined before flags exist; orphans after, so select explicitly.)
quarantine_columns = ["order_id","payment_sequential","payment_type","payment_installments","payment_value","ingested_at","source_file","load_date","quarantine_reason"]

# README §4.2: sequences run 1..N. Violations are dropped from silver but
# quarantined with a reason — never silently filtered.
sequence_violations_df = payments_tf.get_sequence_violations(payments_df) \
    .withColumn("quarantine_reason", lit("invalid_payment_sequence"))
sequence_violations_count = sequence_violations_df.count()
print("Payments with non-contiguous payment_sequential (quarantined):",sequence_violations_count)
# Quarantine keeps every violation; MERGE on (order_id, payment_sequential,
# quarantine_reason) so reruns are idempotent — no drop-first needed.
if sequence_violations_count > 0:
    writers_module.write_delta(sequence_violations_df.select(*quarantine_columns), "quarantine.order_payments", layer="quarantine",
                               merge_key=["order_id", "payment_sequential", "quarantine_reason"])
payments_df = payments_tf.validate_payment_sequence_starts_with_1(payments_df)
print("After validate_payment_sequence_starts_with_1 ",payments_df.count())
# README-documented anomalies are kept and flagged, not filtered
payments_df = payments_tf.flag_zero_payment_value(payments_df)
print("After flag_zero_payment_value ",payments_df.count())
payments_df = payments_tf.flag_nonstandard_installments(payments_df)
print("After flag_nonstandard_installments ",payments_df.count())
payments_df = payments_tf.flag_installments_above_24(payments_df)
print("After flag_installments_above_24 ",payments_df.count())

# ---------- Phase 2: referential integrity vs silver.orders ----------
unmatched_df = payments_df.join(orders_df.select("order_id"), "order_id", "left_anti")
unmatched_count = unmatched_df.count()
print("Payments with no parent order in silver.orders:",unmatched_count)
if unmatched_count > 0:
    unmatched_with_reason_df = unmatched_df.withColumn("quarantine_reason", lit("no_parent_order"))
    writers_module.write_delta(unmatched_with_reason_df.select(*quarantine_columns), "quarantine.order_payments", layer="quarantine",
                              merge_key=["order_id", "payment_sequential", "quarantine_reason"])

payments_df = payments_tf.enrich_with_orders(payments_df,orders_df)
print("After enrich_with_orders (must equal previous count) ",payments_df.count())
payments_df = payments_tf.validate_referential_integrity(payments_df)
print("After validate_referential_integrity ",payments_df.count())
payments_df = payments_tf.drop_orders_columns(payments_df)
print("After drop_orders_columns ",payments_df.count())

# ---------- Phase 3: flag-based reconciliation vs silver.order_items ----------
payments_df = payments_tf.flag_payment_reconciliation(payments_df,orders_items_df)
print("After flag_payment_reconciliation ",payments_df.count())

payments_df.groupBy("payment_status").count().orderBy("count",ascending=False).show()
payments_df.groupBy("zero_payment_value_flag","nonstandard_installments_flag","installments_above_24_flag").count().show()

# Grain is the composite PK (order_id, payment_sequential). MERGE on it: first run
# creates the table, reruns update matched rows and insert new ones (no drop).
writers_module.write_delta(payments_df, "silver.order_payments", layer="silver", merge_key=["order_id", "payment_sequential"])
