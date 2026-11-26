from src.loaders import read_table

import transformations.rules.Referal_Integrity as rules

# Cross-table referential-integrity harness - kept separate from the individual
# silver pipeline callers so each table pipeline stays self-contained and this file
# owns ONLY cross-table checks (transformations/rules/Referal_Integrity.py).
#
# order_items is validated from SILVER: the order_items pipeline (Notebooks/Silver/
# order_items.py) owns the bronze->silver build and quarantines its own orphans to
# quarantine.order_items. This harness re-checks the SILVER OUTPUT: it must find 0
# broken parent links by construction - anything non-zero means the pipeline and its
# parents have fallen out of sync. REPORT-ONLY for items: it never writes
# quarantine.order_items (that table belongs to the pipeline).

order_items_df = read_table("silver.order_items")
orders_df = read_table("silver.orders")
products_df = read_table("silver.products")
sellers_df = read_table("silver.sellers")
customers_df = read_table("silver.customers")
zip_lookup_df = read_table("silver.geolocation_zip_lookup")

print("silver.order_items:", order_items_df.count())

# ---------- 1. order_items parent-link checks ----------

items_no_order = rules.get_items_without_orders(order_items_df, orders_df)
items_no_product = rules.get_items_without_products(order_items_df, products_df)
items_no_seller = rules.get_items_without_sellers(order_items_df, sellers_df)
print("items w/o parent order:", items_no_order.count(),
      "| w/o product:", items_no_product.count(),
      "| w/o seller:", items_no_seller.count())

# Report only - the pipeline owns quarantine.order_items. A non-zero count here
# means silver.order_items and its parents have fallen out of sync.

# Keep-and-flag distribution across ALL items (report only - does not write).
flagged = rules.flag_items_referential_integrity(order_items_df, orders_df, products_df, sellers_df)
print("flagged rows kept (all items survive):", flagged.count())
flagged.groupBy("item_order_link_missing_flag",
                "item_product_link_missing_flag",
                "item_seller_link_missing_flag").count().show()

# ---------- 2. geolocation consistency vs silver.geolocation_zip_lookup ----------
# Read-only join checks (geolocation README: measure unmatched prefixes, investigate
# state mismatches - signals, not violations to drop).

cust_no_geo = rules.get_customers_without_geolocation_match(customers_df, zip_lookup_df)
sell_no_geo = rules.get_sellers_without_geolocation_match(sellers_df, zip_lookup_df)
cust_mismatch = rules.get_customers_state_mismatch(customers_df, zip_lookup_df)
sell_mismatch = rules.get_sellers_state_mismatch(sellers_df, zip_lookup_df)
print("customers w/o geolocation prefix:", cust_no_geo.count(),
      "| customer state mismatches:", cust_mismatch.count())
print("sellers w/o geolocation prefix:", sell_no_geo.count(),
      "| seller state mismatches:", sell_mismatch.count())

if sell_mismatch.count() > 0:
    print("seller state-mismatch sample (investigate - do not drop):")
    sell_mismatch.select("seller_id", "seller_zip_code_prefix", "seller_state").show(10)
if cust_mismatch.count() > 0:
    print("customer state-mismatch sample (investigate - do not drop):")
    cust_mismatch.select("customer_id", "customer_zip_code_prefix", "customer_state").show(10)
