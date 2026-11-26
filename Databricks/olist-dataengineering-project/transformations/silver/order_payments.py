from pyspark.sql.functions import col,to_timestamp,when,lit,sum,abs,trim,lower,collect_set,size,max,min

def changeSchema(df):
    return df.withColumn("payment_sequential",col("payment_sequential").cast("int"))\
        .withColumn("payment_installments",col("payment_installments").cast("int"))\
            .withColumn("payment_value",col("payment_value").cast("double"))

def drop_nulls(df):
    return df.dropna(subset = ["order_id","payment_type","payment_value","payment_installments","payment_sequential"])

def trim_payment_types(df):
    return df.withColumn("payment_type",trim(lower(col("payment_type"))))

def drop_duplicates(df):
    return df.dropDuplicates(["order_id","payment_sequential"])

# README §6: 9 records legitimately have payment_value = 0.00 (fully discounted
# vouchers / not_defined rows) — keep them and flag, never drop.
def flag_zero_payment_value(df):
    return df.withColumn(
        "zero_payment_value_flag",
        when(col("payment_value") <= 0, lit(1)).otherwise(lit(0))
    )

def validate_payment_types(df):
    valid_payment_types = ["credit_card","boleto","voucher","debit_card","not_defined"]
    return df.filter(col("payment_type").isin(valid_payment_types))

def validate_payment_installments(df):
    return df.filter(col("payment_installments") >= 0)

# README §4.3: boleto/debit/voucher installments are "almost universally" 1 —
# exceptions are documented business nuance, so flag them instead of dropping.
def flag_nonstandard_installments(df):
    return df.withColumn(
        "nonstandard_installments_flag",
        when(
            col("payment_type").isin("boleto","debit_card","voucher") &
            (col("payment_installments") != 1),
            lit(1)
        ).otherwise(lit(0))
    )

# README §3: payment_installments documented range is 0..24.
def flag_installments_above_24(df):
    return df.withColumn(
        "installments_above_24_flag",
        when(col("payment_installments") > 24, lit(1)).otherwise(lit(0))
    )

# Rows dropped by validate_payment_sequence_starts_with_1 (orders whose
# payment_sequential does not run 1..N contiguously) — caller quarantines them
# with quarantine_reason='invalid_payment_sequence' instead of silently dropping.
def get_sequence_violations(df):
    sequence_check = (
        df.groupBy("order_id").agg(
            collect_set("payment_sequential").alias("sequence"),
            size(collect_set("payment_sequential")).alias("sequence_length"),
            min("payment_sequential").alias("min_seq"),
            max("payment_sequential").alias("max_seq")
            )
        .filter(
            ~((col("min_seq") == lit(1)) &
            (col("sequence_length") == col("max_seq")))
            ).select("order_id")
    )
    return df.join(sequence_check, on = "order_id", how = "inner")

def validate_payment_sequence_starts_with_1(df):
    sequence_check = (
        df.groupBy("order_id").agg(
            collect_set("payment_sequential").alias("sequence"),
            size(collect_set("payment_sequential")).alias("sequence_length"),
            min("payment_sequential").alias("min_seq"),
            max("payment_sequential").alias("max_seq")
            )
        .filter(
            (col("min_seq") == lit(1)) &
            (col("sequence_length") == col("max_seq"))
            ).select("order_id")
    )
    return df.join(sequence_check, on = "order_id", how = "inner")

# README §5: every payment must reference an order in olist_orders_dataset (FK).
def enrich_with_orders(df, orders_df):
    return df.join(
        orders_df.select(
            "order_id",
            "order_purchase_timestamp",
            "order_status"
        ),
        how = "left",
        on = "order_id"
    )

def validate_referential_integrity(df):
    return df.filter(col("order_purchase_timestamp").isNotNull())

def drop_orders_columns(df):
    return df.drop("order_purchase_timestamp","order_status")

# README §4.5: payment_value vs (item price + freight) differences are EXPECTED
# (discounts, vouchers, shipping adjustments at checkout) — flag them, never drop.
# Left join so orders without item records are kept and marked "No Order Items".
def flag_payment_reconciliation(df, orders_items_df):
    actual_price_to_pay = orders_items_df.select(
        "order_id",
        (col("price") + col("freight_value")).alias("item_total")
    ).groupBy("order_id").agg(
        sum("item_total").alias("total_price")
    )
    payments_agg = df.groupBy("order_id").agg(
        sum(col("payment_value")).alias("total_payment_value")
    )
    reconciliation = payments_agg.join(
        actual_price_to_pay,
        on = "order_id",
        how = "left"
    ).withColumn("payment_status",
        when(col("total_price").isNull(), lit("No Order Items"))
        .when(abs(col("total_payment_value") - col("total_price")) <= 0.05, "Completed")
        .when(col("total_payment_value") > col("total_price"), "Payment Value Exceeded")
        .otherwise("Partial")
    ).withColumn("payment_value_difference",
        (col("total_payment_value") - col("total_price"))
    )
    return df.join(
        reconciliation.select("order_id","payment_status","payment_value_difference"),
        how = "left",
        on = "order_id"
    )
