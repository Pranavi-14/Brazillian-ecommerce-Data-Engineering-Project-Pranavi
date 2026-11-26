from pyspark.sql.functions import col, when, lit, unix_timestamp, expr

def changeSchema(df):
    return (
        df.withColumn("review_score", col("review_score").cast("int"))
          .withColumn(
              "review_comment_message",
              col("review_comment_message").cast("string")
          )
          .withColumn(
              "review_creation_date",
              expr("try_cast(review_creation_date as timestamp)")
          )
          .withColumn(
              "review_answer_timestamp",
              expr("try_cast(review_answer_timestamp as timestamp)")
          )
    )

def dropnulls(df):
    return df.dropna(subset=["review_creation_date","order_id","review_id","review_score"])


def enrich_with_orders(df,orders_df):
    return df.join(
        orders_df.select(
            "order_id",
            "order_purchase_timestamp",
            "order_status",
            "order_delivered_customer_date"
        ),
        how = "left",
        on = "order_id"
    )

def validate_referential_integrity(df):
    return df.filter(col("order_purchase_timestamp").isNotNull())

def validate_review_after_purchase(df):
    return df.filter(col("review_creation_date") >= col("order_purchase_timestamp"))

def low_score_no_comment(df):
    return df.withColumn("low_score_no_comment",when((col("review_score").isin(1,2)) & (col("review_comment_title").isNull()) & (col("review_comment_message").isNull()) ,lit(1)).otherwise(lit(0)))

def validate_review_score(df):
    return df.filter(col("review_score").isin(1,2,3,4,5))

def remove_impossible_timestamps(df):
    return df.filter(
        col("review_answer_timestamp").isNull() |
        (col("review_creation_date") <= col("review_answer_timestamp"))
    )

def calculate_response_time_in_days(df):
    return df.withColumn(
        "response_time_in_days",
        when(
            col("review_answer_timestamp").isNotNull() &
            col("review_creation_date").isNotNull(),
            (
                unix_timestamp("review_answer_timestamp") -
                unix_timestamp("review_creation_date")
            )/86400
        )
    )

def calculate_review_age(df):
    return df.withColumn("review_age",
        when(
            col("review_creation_date").isNotNull() &
            col("order_purchase_timestamp").isNotNull(),
            (
                unix_timestamp("review_creation_date") -
                unix_timestamp("order_purchase_timestamp")
            )/86400
        )
    )

def drop_orders_columns(df):
    return df.drop("order_purchase_timestamp","order_status","order_delivered_customer_date")
