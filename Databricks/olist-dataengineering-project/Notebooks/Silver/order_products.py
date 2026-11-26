from src.loaders import read_table

import transformations.silver.order_products as products_tf
import src.Writers as writers_module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

products_df = read_table("bronze.products")
translation_df = read_table("bronze.product_category_name_translation")
print("before",products_df.count())

# ---------- Phase 1: table-local cleaning (products_table_readme.md) ----------
products_df = products_tf.changeSchema(products_df)
print("After changeSchema ",products_df.count())
products_df = products_tf.validate_product_category_name(products_df)
print("After validate_product_category_name ",products_df.count())
products_df = products_tf.dropnulls(products_df)
print("After dropnulls ",products_df.count())
# README §4.3: 610 null-category rows are a documented pattern — kept and flagged
products_df = products_tf.flag_category_missing(products_df)
print("After flag_category_missing ",products_df.count())
products_df = products_tf.dropduplicates(products_df)
print("After dropduplicates ",products_df.count())
# README §6: weight spans 0..40,425 g (4 rows at 0 g, 144 at >= 30 kg) — kept and flagged
products_df = products_tf.flag_weight_out_of_range(products_df)
print("After flag_weight_out_of_range ",products_df.count())
products_df = products_tf.validateLength(products_df)
print("After validateLength ",products_df.count())
products_df = products_tf.validateHeight(products_df)
print("After validateHeight ",products_df.count())
products_df = products_tf.validateWidth(products_df)
print("After validateWidth ",products_df.count())
products_df = products_tf.validatephotoqty(products_df)
print("After validatephotoqty ",products_df.count())
products_df = products_tf.validateproductname(products_df)
print("After validateproductname ",products_df.count())
products_df = products_tf.validateproductdescription(products_df)
print("After validateproductdescription ",products_df.count())

# ---------- Phase 2: translation enrichment (product_category_name_translation_readme.md §4.1) ----------
# 2 untranslated categories + 610 null categories kept with COALESCE fallback to 'uncategorized'
products_df = products_tf.enrich_with_translation(products_df,translation_df)
print("After enrich_with_translation ",products_df.count())

products_df.groupBy("category_missing_flag","category_translation_missing_flag","weight_out_of_range_flag").count().show()
products_df.groupBy("product_category_name_resolved").count().orderBy("count",ascending=False).show(5)

# MERGE on product_id (unique PK) — reruns update matched rows and insert new ones.
writers_module.write_delta(products_df, "silver.products", layer="silver", merge_key="product_id")
