from pyspark.sql.functions import col, to_timestamp, to_date, datediff, when, lit, coalesce

def _parse_timestamp(column_name):
    return coalesce(
        to_timestamp(column_name, "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(column_name, "yyyy-MM-dd"),
    )

def transform_orders(orders_df):
    orders_df = orders_df \
        .withColumn("order_id", col("order_id").cast("string")) \
        .withColumn("customer_id", col("customer_id").cast("string")) \
        .withColumn("order_status", col("order_status").cast("string")) \
        .withColumn("order_purchase_timestamp", _parse_timestamp("order_purchase_timestamp")) \
        .withColumn("order_approved_at", _parse_timestamp("order_approved_at")) \
        .withColumn("order_delivered_carrier_date", _parse_timestamp("order_delivered_carrier_date")) \
        .withColumn("order_delivered_customer_date", _parse_timestamp("order_delivered_customer_date")) \
        .withColumn("order_estimated_delivery_date", _parse_timestamp("order_estimated_delivery_date"))
    return orders_df

def drop_nulls(df):
    return df.dropna(subset=["order_id", "customer_id","order_purchase_timestamp"])

def drop_duplicates(df):
    return df.dropDuplicates(["order_id"])

def check_order_status(df):
    valid_status = ["delivered","invoiced","shipped","processing","unavailable","canceled","created","approved"]
    return df.filter(col("order_status").isin(valid_status))

def filter_orders_delivered_after_purchase(df):
    return df.filter(
        (
            col("order_delivered_customer_date").isNull()
        ) |
        (
            (col("order_delivered_customer_date") >= col("order_purchase_timestamp")) &
            (
                col("order_approved_at").isNull() |
                (col("order_delivered_customer_date") >= col("order_approved_at"))
            ) &
            (
                col("order_delivered_carrier_date").isNull() |
                (col("order_delivered_customer_date") >= col("order_delivered_carrier_date"))
            )
        )
    )

def filter_orders_approved_after_purchase(df):
    return df.filter(
        col("order_approved_at").isNull() |
        (col("order_approved_at") >= col("order_purchase_timestamp"))
    )

def filter_orders_carrier_picked_after_approved_purchased(df):
    return df.filter(
        (
            col("order_delivered_carrier_date").isNull()
        ) |
        (
            col("order_approved_at").isNotNull() &
            (col("order_delivered_carrier_date") >= col("order_approved_at")) &
            (col("order_delivered_carrier_date") >= col("order_purchase_timestamp"))
        )
    )

def filter_estimated_date_after_purchased_approved(df):
    return df.filter(
        col("order_estimated_delivery_date").isNull() |
        (
            (to_date("order_estimated_delivery_date") >= to_date("order_purchase_timestamp")) &
            (
                col("order_approved_at").isNull() |
                (to_date("order_estimated_delivery_date") >= to_date("order_approved_at"))
            )
        )
    )

def processing_delay_flag(df,reference_date):
    return df.withColumn(
        "processing_delay_flag",
        when(
            (col("order_status") == "processing") &
            col("order_approved_at").isNotNull() &
            (datediff(lit(reference_date), col("order_approved_at")) > 30),
            True
        ).otherwise(False)
    )

def validate_against_approved_date(df):
    return df.filter(
        ~(
            col("order_approved_at").isNull() &
            (
                (~col("order_status").isin(["created", "canceled","unavailable"])) |
                col("order_delivered_carrier_date").isNotNull() |
                col("order_delivered_customer_date").isNotNull()
            )
        )
    )

def canceled_with_delivery_date_flag(df):
    # keep-and-flag (was the drop filter cancelled_orders_delivery_date_isNotNull
    # until 2026-11-12): canceled orders with a non-null delivery date are a
    # data-quality signal — kept and flagged, never dropped
    return df.withColumn(
        "canceled_with_delivery_date_flag",
        when(
            (col("order_status") == "canceled") &
            (col("order_delivered_customer_date").isNotNull()),
            True
        ).otherwise(False)
    )

def carrier_pickup_sla_breach_flag(df):
    return df.withColumn(
        "carrier_pickup_sla_breach",
        when(
            col("order_delivered_carrier_date").isNotNull() &
            col("order_approved_at").isNotNull() &
            (datediff(
                col("order_delivered_carrier_date"),
                col("order_approved_at")
            ) > 7),
            True
        ).otherwise(False)
    )

def stale_shipment_flag(df,reference_date):
    return df.withColumn(
        "stale_shipment_flag",
        when(
            (col("order_status") == "shipped") &
            col("order_delivered_carrier_date").isNotNull() &
            col("order_delivered_customer_date").isNull() &
            (datediff(lit(reference_date), col("order_delivered_carrier_date")) > 30),
            True
        ).otherwise(False)
    )

# def reference_date(df):
#     return df.selectExpr("max(order_purchase_timestamp)").collect()[0][0]

def stale_approval_flag(df,reference_date):
    return df.withColumn(
        "stale_approval_flag",
        when(
            col("order_status").isin(["approved", "processing"]) &
            col("order_approved_at").isNotNull() &
            col("order_delivered_carrier_date").isNull() &
            (datediff(lit(reference_date), col("order_approved_at")) > 30),
            True
        ).otherwise(False)
    )

def long_delivery_flag(df):
    return df.withColumn(
        "long_delivery_flag",
        when(
            col("order_delivered_customer_date").isNotNull() &
            col("order_delivered_carrier_date").isNotNull() &
            (datediff(
                col("order_delivered_customer_date"),
                col("order_delivered_carrier_date")
            ) > 30),
            True
        ).otherwise(False)
    )

def Late_delivery_flag(df):
    return df.withColumn(
        "Late_delivery_flag",
        when(
            col("order_estimated_delivery_date").isNotNull() &
            col("order_delivered_customer_date").isNotNull() &
            (to_date("order_delivered_customer_date") > to_date("order_estimated_delivery_date")),
            True
        ).otherwise(False)
    )
