from src.loaders import read_table

import transformations.silver.order_items as order_items_tf
import transformations.rules.Referal_Integrity as rules
import src.Writers as writers_module
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

order_items_df = read_table("bronze.order_items")
orders_df = read_table("silver.orders")
products_df = read_table("silver.products")
sellers_df = read_table("silver.sellers")
print("bronze.order_items:", order_items_df.count())

# ---------- Phase 1: table-local cleaning (README olist_order_items_README.md) ----------

order_items_df = order_items_tf.changeSchema(order_items_df)
print("After changeSchema:", order_items_df.count())

# Rule violations are quarantined with a reason (README 6: investigate, never
# silently discard). Data verified clean: 0 nulls, 0 invalid item ids expected.
# Quarantine keeps every violation; MERGE on (order_id, order_item_id,
# quarantine_reason) so reruns are idempotent — no drop-first needed.

def quarantine_items(violations_df, reason):
    if violations_df.count() > 0:
        writers_module.write_delta(
            violations_df.withColumn("quarantine_reason", lit(reason)),
            "quarantine.order_items",
            layer="quarantine",
            merge_key=["order_id", "order_item_id", "quarantine_reason"]
        )

null_violations = order_items_tf.get_null_violations(order_items_df)
invalid_item_id = order_items_tf.get_invalid_item_id_violations(order_items_df)
print("null violations:", null_violations.count(),
      "| invalid item-id violations:", invalid_item_id.count())
quarantine_items(null_violations, "missing_required_field")
quarantine_items(invalid_item_id, "invalid_item_sequence")

order_items_df = order_items_tf.drop_nulls(order_items_df)
print("After drop_nulls:", order_items_df.count())
order_items_df = order_items_tf.validate_order_item_id(order_items_df)
print("After validate_order_item_id:", order_items_df.count())

# ---------- Phase 2: grain + keep-and-flag quality ----------

order_items_df = order_items_tf.drop_duplicates(order_items_df)
print("After composite-key dedupe (order_id, order_item_id):", order_items_df.count())
order_items_df = order_items_tf.flag_negative_monetary(order_items_df)
order_items_df.groupBy("negative_monetary_flag").count().show()

gap_rows = order_items_tf.get_sequence_gap_violations(order_items_df)
print("sequence-gap rows (README 6.1 investigate-only):", gap_rows.count())

# ---------- Phase 3: referential integrity - all three parents (README 6.2) ----------

items_no_order = rules.get_items_without_orders(order_items_df, orders_df)
items_no_product = rules.get_items_without_products(order_items_df, products_df)
items_no_seller = rules.get_items_without_sellers(order_items_df, sellers_df)
print("orphans w/o order:", items_no_order.count(),
      "| w/o product:", items_no_product.count(),
      "| w/o seller:", items_no_seller.count())
quarantine_items(items_no_order, "no_parent_order")
quarantine_items(items_no_product, "no_matching_product")
quarantine_items(items_no_seller, "no_matching_seller")

order_items_df = (
    order_items_df
    .join(orders_df.select("order_id"), "order_id", "left_semi")
    .join(products_df.select("product_id"), "product_id", "left_semi")
    .join(sellers_df.select("seller_id"), "seller_id", "left_semi")
)
print("After referential-integrity keep (orphans quarantined):", order_items_df.count())

# ---------- Phase 4: enrichment + README 8 derived fields ----------

item_columns = order_items_df.columns  # quarantine.order_items schema (pre-enrichment)
order_items_df = order_items_tf.enrich_with_orders(order_items_df, orders_df)
print("After enrich_with_orders (must equal previous count):", order_items_df.count())

# Shipping-limit plausibility (needs order_purchase_timestamp from the enrichment).
# > 60 days after purchase -> quarantined; 31-60 days -> kept and flagged.
implausible_limit = order_items_tf.get_implausible_shipping_limit_violations(order_items_df)
print("implausible shipping-limit rows (> 60 d after purchase):", implausible_limit.count())
quarantine_items(implausible_limit.select(*item_columns), "implausible_shipping_limit")
order_items_df = order_items_tf.drop_implausible_shipping_limit(order_items_df)
print("After drop_implausible_shipping_limit:", order_items_df.count())
order_items_df = order_items_tf.flag_long_shipping_limit(order_items_df)
order_items_df.groupBy("long_shipping_limit_flag").count().show()
order_items_df = order_items_tf.flag_shipping_limit_before_purchase(order_items_df)
order_items_df = order_items_tf.derive_dispatch_delay_hours(order_items_df)
order_items_df = order_items_tf.flag_handover_after_deadline(order_items_df)
order_items_df = order_items_tf.derive_item_total_value(order_items_df)
order_items_df = order_items_tf.drop_orders_columns(order_items_df)
print("After derived fields + drop_orders_columns:", order_items_df.count())

order_items_df.groupBy("negative_monetary_flag",
                       "shipping_limit_before_purchase_flag").count().show()
order_items_df.groupBy("handover_after_deadline_flag").count().show()
order_items_df.selectExpr("max(dispatch_delay_hours) max_delay_h",
                          "min(dispatch_delay_hours) min_delay_h",
                          "count(dispatch_delay_hours) non_null_delay",
                          "sum(case when freight_value = 0 then 1 else 0 end) zero_freight_rows").show()

# ---------- Write ----------
# Item-line grain: MERGE on the composite PK (order_id, order_item_id).
writers_module.write_delta(order_items_df, "silver.order_items", layer="silver", merge_key=["order_id", "order_item_id"])
