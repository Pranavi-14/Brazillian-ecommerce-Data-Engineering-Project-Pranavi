from pyspark.sql.functions import col,to_timestamp,when,lit,sum,abs,count,trim,upper,length

# def changeSchema(df):
#     return df.withColumn("seller_zip_code_prefix",col("seller_zip_code_prefix").cast("String"))

def trimColumns(df):
    return df.withColumn("seller_city",trim(col("seller_city")))\
        .withColumn("seller_state",upper(col("seller_state")))

def dropduplicates(df):
    return df.dropDuplicates(["seller_id"])

def dropnulls(df):
    return df.dropna(subset=[
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    ])

# README §3: seller_zip_code_prefix is the first 5 digits of the seller's CEP.
# Violations are dropped from silver but quarantined with a reason — never silently filtered.
def get_zipcode_violations(df):
    return df.filter(
        length(col("seller_zip_code_prefix").cast("string")) != 5
    )

def validate_zipcode(df):
    return df.filter(
        length(col("seller_zip_code_prefix").cast("string")) == 5
    )

# README §6: 23 of 27 valid Brazilian state codes are represented; any code outside
# the official 27 is invalid. Violations are quarantined, never silently filtered.
def get_state_code_violations(df):
    VALID_BRAZILIAN_STATE_CODES = [
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES",
        "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR",
        "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
        "SP", "SE", "TO"
    ]
    return df.filter(~col("seller_state").isin(VALID_BRAZILIAN_STATE_CODES))

def validate_brazilian_state_code(df):
    VALID_BRAZILIAN_STATE_CODES = [
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES",
        "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR",
        "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
        "SP", "SE", "TO"
    ]
    return df.filter(col("seller_state").isin(VALID_BRAZILIAN_STATE_CODES))

# README §4.2: sellers link to orders only via order_items (1..N, not mandatory).
# A seller with zero order items is still a valid dimension row — LEFT join, keep and flag.
def flag_seller_order_activity(df,orders_df,order_items_df):
    order_seller_df = order_items_df.join(
        orders_df.select("order_id","order_status"),"order_id","inner"
    ).select("order_id","seller_id","order_status").distinct()
    seller_stats = order_seller_df.groupby("seller_id").agg(
        sum(when(col("order_status") == "canceled",lit(1)).otherwise(lit(0))).alias("cancelled_orders"),
        count("order_id").alias("total_orders")
    )
    seller_stats = seller_stats.withColumn(
        "cancellation_rate",(col("cancelled_orders")/col("total_orders"))*100
    )
    df = df.join(
        seller_stats.select("seller_id","cancellation_rate"),"seller_id","left"
    )
    return (
        df.withColumn("seller_with_zero_orders",
                when(col("cancellation_rate").isNull(),lit(1)).otherwise(lit(0)))
          .withColumn("seller_with_100_cancellation_rate",
                when(col("cancellation_rate") == 100,lit(1)).otherwise(lit(0)))
          .drop("cancellation_rate")
    )
