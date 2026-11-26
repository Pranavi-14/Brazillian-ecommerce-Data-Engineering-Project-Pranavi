from src.loaders import read_table

import transformations.silver.orders_transformations as orders_tf
import src.Writers as writers_module

orders_df = read_table("bronze.orders")
print("before",orders_df.count())

orders_df = orders_tf.transform_orders(orders_df)
print("After transform_orders ",orders_df.count())

orders_df = orders_tf.drop_nulls(orders_df)
print("After drop_nulls ",orders_df.count())
orders_df = orders_tf.drop_duplicates(orders_df)
print("After drop_duplicates ",orders_df.count())

orders_df = orders_tf.check_order_status(orders_df)
print("After check_order_status ",orders_df.count())

#orders delivered before purchase date
orders_df = orders_tf.filter_orders_delivered_after_purchase(orders_df)
print("After filter_orders_delivered_after_purchase ",orders_df.count())

#orders approved before purchase date
orders_df = orders_tf.filter_orders_approved_after_purchase(orders_df)
print("After filter_orders_approved_after_purchase ",orders_df.count())

#orders picked up by carrier before purchased and approved
orders_df = orders_tf.filter_orders_carrier_picked_after_approved_purchased(orders_df)
print("After filter_orders_carrier_picked_after_approved_purchased ",orders_df.count())

#order_estimated_delivery_date before purchase or approved data
#If the order_estimated_delivery_date is allowed to be null
orders_df = orders_tf.filter_estimated_date_after_purchased_approved(orders_df)
print("After filter_estimated_date_after_purchased_approved ",orders_df.count())

#reference date computed on cleaned, type-cast data (max purchase timestamp)
reference_date = (
    orders_df
    .agg({"order_purchase_timestamp": "max"})
    .collect()[0][0]
)

#orders stuck in processing for more than 30 days
orders_df = orders_tf.processing_delay_flag(orders_df,reference_date)
print("After processing_delay_flag ",orders_df.count())

#if approved date is null -- status -- created/cancelled/unavailable
#order_delivered_carrier_date,order_delivered_customer_date -- null
orders_df = orders_tf.validate_against_approved_date(orders_df)
print("After validate_against_approved_date ",orders_df.count())

orders_df = orders_tf.canceled_with_delivery_date_flag(orders_df)
print("After canceled_with_delivery_date_flag ",orders_df.count())

orders_df = orders_tf.carrier_pickup_sla_breach_flag(orders_df)
print("After carrier_pickup_sla_breach_flag ",orders_df.count())

orders_df = orders_tf.stale_shipment_flag(orders_df, reference_date)
print("After stale_shipment_flag ", orders_df.count())

orders_df = orders_tf.stale_approval_flag(orders_df, reference_date)
print("After stale_approval_flag ", orders_df.count())

orders_df = orders_tf.long_delivery_flag(orders_df)
print("After long_delivery_flag ",orders_df.count())

orders_df = orders_tf.Late_delivery_flag(orders_df)
print("After Late_delivery_flag ",orders_df.count())

writers_module.write_delta(orders_df, "silver.orders", layer="silver",merge_key = "order_id")
