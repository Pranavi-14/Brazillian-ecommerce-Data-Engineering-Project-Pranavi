from src.loaders import read_table

import transformations.silver.order_reviews as reviews_tf
import src.Writers as writers_module
from pyspark.sql.functions import lit

reviews_df = read_table("bronze.order_reviews")
orders_df = read_table("silver.orders")
print("before",reviews_df.count())

#phase 1: table-local cleaning
reviews_df = reviews_tf.changeSchema(reviews_df)
print("After changeSchema ",reviews_df.count())
reviews_df = reviews_tf.dropnulls(reviews_df)
print("After dropnulls ",reviews_df.count())
reviews_df = reviews_tf.validate_review_score(reviews_df)
print("After validate_review_score ",reviews_df.count())

#phase 2: referential integrity + cross-table validations
unmatched_df = reviews_df.join(orders_df.select("order_id"), "order_id", "left_anti")
unmatched_count = unmatched_df.count()
print("Reviews with no parent order in silver.orders:",unmatched_count)
# Quarantine keeps every violation but reruns must not double-append: MERGE on
# the composite (review_id, order_id) — same key that makes silver.reviews idempotent.
if unmatched_count > 0:
    writers_module.write_delta(unmatched_df.withColumn("quarantine_reason", lit("no_parent_order")), "quarantine.order_reviews", layer="quarantine",
                               merge_key=["review_id","order_id"])

reviews_df = reviews_tf.enrich_with_orders(reviews_df,orders_df)
print("After enrich_with_orders (must equal previous count)",reviews_df.count())
reviews_df = reviews_tf.validate_referential_integrity(reviews_df)
print("After validate_referential_integrity ",reviews_df.count())
reviews_df = reviews_tf.validate_review_after_purchase(reviews_df)
print("After validate_review_after_purchase ",reviews_df.count())
reviews_df = reviews_tf.remove_impossible_timestamps(reviews_df)
print("After remove_impossible_timestamps ",reviews_df.count())
reviews_df = reviews_tf.low_score_no_comment(reviews_df)
print("After low_score_no_comment ",reviews_df.count())
reviews_df = reviews_tf.calculate_response_time_in_days(reviews_df)
print("After calculate_response_time_in_days ",reviews_df.count())
reviews_df = reviews_tf.calculate_review_age(reviews_df)
print("After calculate_review_age ",reviews_df.count())
reviews_df = reviews_tf.drop_orders_columns(reviews_df)
print("After drop_orders_columns ",reviews_df.count())

# review_id is NOT unique here (same survey response attached to every order of a
# multi-order customer) — grain is the composite (review_id, order_id), so MERGE
# on both (Writers supports composite merge keys).
writers_module.write_delta(reviews_df, "silver.reviews", layer="silver",merge_key = ["review_id","order_id"])
