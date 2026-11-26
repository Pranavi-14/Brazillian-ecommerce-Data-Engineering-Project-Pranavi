from pyspark.sql.functions import col,when,lit,trim,upper,count

# Customers transformation module (README/customer_table_readme.md +
# README/README_customers_table.md §3 + README/olist_customers_README.md §3).
#
# README rule: customer_zip_code_prefix is a 5-digit postal-code PREFIX that must stay TEXT —
# 23,995 rows (24%) start with '0' and an int cast would silently corrupt them ("01001" -> 1001).
# It is a code, not a quantity: never cast it.

VALID_BRAZILIAN_STATE_CODES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES",
    "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR",
    "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO"
]

def trimColumns(df):
    return df.withColumn("customer_city", trim(col("customer_city")))\
        .withColumn("customer_state", upper(trim(col("customer_state"))))

def dropnulls(df):
    return df.dropna(subset=["customer_id", "customer_unique_id"])

def dropduplicates(df):
    return df.dropDuplicates(["customer_id"])

def validate_brazilian_state_code(df):
    return df.filter(col("customer_state").isin(VALID_BRAZILIAN_STATE_CODES))

# Violation getters (quarantine support) — README §3: zip prefix is exactly 5 digits;
# §3/§6: 2-letter state code from the 27 valid UFs. Data verified clean: expect 0.

def get_zip_prefix_violations(df):
    return df.filter(~col("customer_zip_code_prefix").rlike("^[0-9]{5}$"))

def get_state_code_violations(df):
    return df.filter(~col("customer_state").isin(VALID_BRAZILIAN_STATE_CODES))

# ---------- Enrichment ----------

# README §5: customers.customer_id joins orders.customer_id 1:1. The orders silver pipeline
# dropped some orders, so some customers have no surviving order — keep them (dimension
# completeness per README §2: 99,441 = full dimension), flag the missing link, and drop the
# joined order_id (link can always be recovered by rejoining silver.orders on customer_id).
def enrich_with_orders(df, orders_df):
    orders = orders_df.select("order_id", "customer_id")
    df = df.join(orders, on="customer_id", how="left")
    return df.withColumn(
        "customer_order_link_missing_flag",
        when(col("order_id").isNull(), lit(1)).otherwise(lit(0))
    ).drop("order_id")

# README §4 + §7: customer_unique_id identifies the underlying person across orders;
# repeat-purchase analysis counts customer_id rows per customer_unique_id.
def enrich_with_repeat_stats(df):
    stats = df.groupBy("customer_unique_id").agg(count(lit(1)).alias("customer_total_orders"))
    df = df.join(stats, on="customer_unique_id", how="left")
    return df.withColumn(
        "customer_repeat_buyer_flag",
        when(col("customer_total_orders") > 1, lit(1)).otherwise(lit(0))
    )
