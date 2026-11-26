from src.loaders import read_table

import transformations.silver.geolocation as geolocation_tf
import src.Writers as writers_module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.getOrCreate()

geolocation_df = read_table("bronze.geolocation")
print("before", geolocation_df.count())

# ---------- Phase 1: table-local cleaning (olist_geolocation_README.md) ----------
# lat/lng strings -> double; zip prefix stays TEXT (245,733 leading-zero rows).
# Dedupe removes ONLY exact duplicate observations (1,000,163 -> 738,332):
# the zip prefix is not a key (README section 2/section 5).
geolocation_df = geolocation_tf.changeSchema(geolocation_df)
geolocation_df = geolocation_tf.trimColumns(geolocation_df)
geolocation_df = geolocation_tf.dropnulls(geolocation_df)
geolocation_df = geolocation_tf.dropDuplicates(geolocation_df)
print("After exact-observation dedupe ", geolocation_df.count())
geolocation_df = geolocation_tf.validateZipcode(geolocation_df)
geolocation_df = geolocation_tf.validateStatecode(geolocation_df)
geolocation_df = geolocation_tf.flag_out_of_brazil_coords(geolocation_df)
print("After filters/flags ", geolocation_df.count())
geolocation_df.groupBy("out_of_brazil_coords_flag").count().show()

# ---------- Quarantine (expect 0 - data verified clean) ----------
zip_violations = geolocation_tf.get_zip_prefix_violations(geolocation_df)
state_violations = geolocation_tf.get_state_code_violations(geolocation_df)
world_violations = geolocation_tf.get_world_coord_violations(geolocation_df)
print("zip violations:", zip_violations.count(),
      "| state violations:", state_violations.count(),
      "| world coord violations:", world_violations.count())

# Observation grain: one row per exact (prefix, lat, lng, city, state) - the same
# columns the dedupe uses, so this is the table's true unique key.
GEO_OBSERVATION_KEY = ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
                       "geolocation_city", "geolocation_state"]

quarantine_columns = ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
                      "geolocation_city", "geolocation_state", "ingested_at", "source_file",
                      "load_date", "quarantine_reason"]

def quarantine_batch(violations_df, reason):
    if violations_df.count() > 0:
        writers_module.write_delta(
            violations_df.withColumn("quarantine_reason", lit(reason)).select(quarantine_columns),
            "quarantine.geolocation",
            layer="quarantine",
            merge_key=GEO_OBSERVATION_KEY + ["quarantine_reason"]
        )

quarantine_batch(zip_violations, "invalid_zip_prefix")
quarantine_batch(state_violations, "invalid_state_code")
quarantine_batch(world_violations, "invalid_world_coords")

# ---------- Phase 2: curated one-row-per-prefix lookup (README section 5) ----------
zip_lookup = geolocation_tf.build_zip_lookup(geolocation_df)
print("zip lookup rows:", zip_lookup.count())
zip_lookup.groupBy("city_label_conflict_flag", "state_label_conflict_flag",
                   "rep_point_out_of_brazil_flag").count().show()

# ---------- Write ----------
# silver.geolocation keeps the OBSERVATION grain - MERGE on the full observation key
# (never on prefix alone, which is not unique).
writers_module.write_delta(geolocation_df, "silver.geolocation", layer="silver", merge_key=GEO_OBSERVATION_KEY)

# The lookup has a true unique PK (prefix) - MERGE on it.
writers_module.write_delta(zip_lookup, "silver.geolocation_zip_lookup", layer="silver", merge_key="geolocation_zip_code_prefix")
