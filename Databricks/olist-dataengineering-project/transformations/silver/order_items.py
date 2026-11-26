from pyspark.sql.functions import (col, coalesce, to_timestamp, when, lit, trim,
                                    round, unix_timestamp, row_number, floor)
from pyspark.sql.window import Window

# Order-items transformation module (README/olist_order_items_README.md).
#
# README section 2 (grain): one row = one item line; the ONLY sanctioned uniqueness is
# the composite key (order_id, order_item_id) - repeated order_id values are expected
# for multi-item orders and must never be deduplicated away.
# README sections 6.1-6.4: violations are INVESTIGATED, never silently filtered -
# every former drop-filter is now a keep-and-flag column or a violation getter that
# the caller quarantines with a reason.

def _parse_timestamp(column_name):
    return coalesce(
        to_timestamp(column_name, "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(column_name, "yyyy-MM-dd"),
    )

def changeSchema(df):
    return (
        df.withColumn("order_id", trim(col("order_id")))
          .withColumn("order_item_id", col("order_item_id").cast("int"))
          .withColumn("product_id", trim(col("product_id")))
          .withColumn("seller_id", trim(col("seller_id")))
          .withColumn("shipping_limit_date", _parse_timestamp("shipping_limit_date"))
          .withColumn("price", col("price").cast("double"))
          .withColumn("freight_value", col("freight_value").cast("double"))
    )

# README 6.1/6.4: null checks - quarantine the violating rows (getter mirrors the
# drop_nulls filter so the caller can quarantine with a reason before filtering).
def get_null_violations(df):
    return df.filter(
        col("order_id").isNull() | col("order_item_id").isNull() |
        col("product_id").isNull() | col("seller_id").isNull() |
        col("price").isNull() | col("freight_value").isNull()
    )

def drop_nulls(df):
    return df.dropna(subset=["order_id", "order_item_id", "product_id", "seller_id",
                             "price", "freight_value"])

# README 6.4: profile zero/negative monetary values - investigate, do NOT drop.
# Zero-freight rows (383 in this data) are valid and stay; only negatives are flagged.
def flag_negative_monetary(df):
    return df.withColumn(
        "negative_monetary_flag",
        when((col("price") < 0) | (col("freight_value") < 0), lit(1)).otherwise(lit(0))
    )

# README 6.1/6.2: composite-key dedupe is the only sanctioned dedupe (grain rule).
def drop_duplicates(df):
    return df.dropDuplicates(["order_id", "order_item_id"])

# README 6.1: order_item_id is a positive sequence - getter mirrors the filter so
# violations are quarantined with a reason instead of silently filtered.
def get_invalid_item_id_violations(df):
    return df.filter(col("order_item_id") <= 0)

def validate_order_item_id(df):
    return df.filter(col("order_item_id") > 0)

# README 6.1: sequence gaps or duplicate positions within an order are INVESTIGATE-
# only signals - reported, never auto-invalidated without a defined rule.
def get_sequence_gap_violations(df):
    w = Window.partitionBy("order_id").orderBy("order_item_id")
    return (
        df.withColumn("_position", row_number().over(w))
          .filter(col("order_item_id") != col("_position"))
          .drop("_position")
    )

def enrich_with_orders(df, orders_df):
    return df.join(
        orders_df.select(
            "order_id",
            "order_purchase_timestamp",
            "order_status",
            "order_delivered_carrier_date"
        ),
        how="left",
        on="order_id"
    )

# README 5.4/6.3: a handover deadline earlier than the purchase timestamp is a
# data-quality signal to investigate - kept and flagged (the old code silently
# dropped such rows with a filter).
def flag_shipping_limit_before_purchase(df):
    return df.withColumn(
        "shipping_limit_before_purchase_flag",
        when(
            col("shipping_limit_date").isNotNull() &
            col("order_purchase_timestamp").isNotNull() &
            (col("shipping_limit_date") < col("order_purchase_timestamp")),
            lit(1)
        ).otherwise(lit(0))
    )

# Shipping-limit plausibility (whole days elapsed between purchase and shipping limit).
# Profiled distribution: median 6 d, p99 19 d, p99.9 36 d, max plausible 51 d; nothing
# between 52-67 d, then 6 rows at 68-148 d and 4 corrupt rows dated 2020 (~1,055 d).
#   > SHIPPING_LIMIT_QUARANTINE_DAYS  -> implausible: quarantined, removed from silver
#   > SHIPPING_LIMIT_FLAG_DAYS (<= quarantine limit) -> unusual but plausible: kept + flagged
# Null-safe: rows missing either timestamp are never quarantined or flagged.
SHIPPING_LIMIT_QUARANTINE_DAYS = 60
SHIPPING_LIMIT_FLAG_DAYS = 30

def _shipping_limit_days_after_purchase():
    return floor(
        (unix_timestamp(col("shipping_limit_date")) -
         unix_timestamp(col("order_purchase_timestamp"))) / 86400
    )

def get_implausible_shipping_limit_violations(df):
    return df.filter(_shipping_limit_days_after_purchase() > SHIPPING_LIMIT_QUARANTINE_DAYS)

def drop_implausible_shipping_limit(df):
    days = _shipping_limit_days_after_purchase()
    return df.filter(days.isNull() | (days <= SHIPPING_LIMIT_QUARANTINE_DAYS))

def flag_long_shipping_limit(df):
    days = _shipping_limit_days_after_purchase()
    return df.withColumn(
        "long_shipping_limit_flag",
        when(days.isNotNull() & (days > SHIPPING_LIMIT_FLAG_DAYS) &
             (days <= SHIPPING_LIMIT_QUARANTINE_DAYS), lit(1)).otherwise(lit(0))
    )

# README 5.4/8: dispatch_delay_hours = carrier handover - shipping limit, in hours.
# Null-safe rule: only defined when BOTH timestamps exist; a missing carrier
# handover (not handed over / incomplete lifecycle) stays null, which keeps
# "not yet dispatched" separate from "late" per README 6.3.
def derive_dispatch_delay_hours(df):
    return df.withColumn(
        "dispatch_delay_hours",
        when(
            col("shipping_limit_date").isNotNull() &
            col("order_delivered_carrier_date").isNotNull(),
            round(
                (unix_timestamp(col("order_delivered_carrier_date")) -
                 unix_timestamp(col("shipping_limit_date"))) / 3600.0,
                2
            )
        )
    )

# README 8: is_handover_after_deadline with a defined null-handling rule
# (a null on either side is simply not late: flag = 0). Int 1/0 for consistency
# with the other silver tables.
def flag_handover_after_deadline(df):
    return df.withColumn(
        "handover_after_deadline_flag",
        when(
            col("shipping_limit_date").isNotNull() &
            col("order_delivered_carrier_date").isNotNull() &
            (col("order_delivered_carrier_date") > col("shipping_limit_date")),
            lit(1)
        ).otherwise(lit(0))
    )

# README 8 derived example: item_total_value = price + freight_value.
def derive_item_total_value(df):
    return df.withColumn("item_total_value", col("price") + col("freight_value"))

def drop_orders_columns(df):
    return df.drop("order_purchase_timestamp", "order_status", "order_delivered_carrier_date")
