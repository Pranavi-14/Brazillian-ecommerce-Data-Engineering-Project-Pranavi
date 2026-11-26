from pyspark.sql.functions import col,when,lit,length,trim,upper,avg,count,countDistinct,row_number
from pyspark.sql.window import Window

# Geolocation transformation module (README/olist_geolocation_README.md).
#
# README section 2 (grain): one row = ONE geolocation observation; geolocation_zip_code_prefix
# is NOT unique (19,015 prefixes over 1M rows). README section 5: do not dedupe on prefix alone
# and do not enforce uniqueness on it - only exact duplicate observations may be removed; a
# separate one-row-per-prefix lookup (build_zip_lookup) provides the 1:1 join grain.
#
# README section 3: zip prefix stays TEXT (leading zeroes meaningful - 245,733 rows start with '0').

VALID_BRAZILIAN_STATE_CODES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES",
    "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR",
    "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO"
]

# README section 8: preserve coordinates as numerics with suitable precision - float (32-bit)
# loses precision on values with ~14 decimals; cast to double. Bronze keeps lat/lng as strings.
def changeSchema(df):
    return (
        df.withColumn("geolocation_lat", col("geolocation_lat").cast("double"))
          .withColumn("geolocation_lng", col("geolocation_lng").cast("double"))
    )

def trimColumns(df):
    return (
        df.withColumn("geolocation_state", upper(trim(col("geolocation_state"))))
          .withColumn("geolocation_city", trim(col("geolocation_city")))
    )

def dropnulls(df):
    return df.dropna(subset=[
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_state"
    ])

# README section 5 rule 5: "Do not arbitrarily keep the first row" - the old dropDuplicates
# on prefix alone discarded 981,148 rows (98%). Deduping EXACT duplicate observations only
# (identical on all 5 source columns) preserves the multi-point grain: 1,000,163 -> 738,332.
def dropDuplicates(df):
    return df.dropDuplicates([
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
    ])

# README section 6: coordinates outside the expected Brazil region should be investigated,
# not automatically deleted - keep the 33 offending rows and flag them.
def flag_out_of_brazil_coords(df):
    return df.withColumn(
        "out_of_brazil_coords_flag",
        when(
            (~((col("geolocation_lat") > -33.75) & (col("geolocation_lat") < 5.27))) |
            (~((col("geolocation_lng") > -73.99) & (col("geolocation_lng") < -34.73))),
            lit(1)
        ).otherwise(lit(0))
    )

def validateZipcode(df):
    return df.filter(length(col("geolocation_zip_code_prefix")) == 5)

def validateStatecode(df):
    return df.filter(col("geolocation_state").isin(VALID_BRAZILIAN_STATE_CODES))

# Violation getters (quarantine support) - data verified clean: expect 0.

def get_zip_prefix_violations(df):
    return df.filter(length(col("geolocation_zip_code_prefix")) != 5)

def get_state_code_violations(df):
    return df.filter(~col("geolocation_state").isin(VALID_BRAZILIAN_STATE_CODES))

def get_world_coord_violations(df):
    return df.filter(
        (~((col("geolocation_lat") >= -90) & (col("geolocation_lat") <= 90))) |
        (~((col("geolocation_lng") >= -180) & (col("geolocation_lng") <= 180)))
    )

# ---------- Curated lookup (README section 5 strategies 2+3, section 8) ----------
#
# One row per zip prefix for 1:1 joins with customers/sellers (README 4.1/4.2 warn that
# joining the raw table multiplies rows). Documented rules:
#   - avg latitude / avg longitude per prefix (README section 5 example aggregation)
#   - source_coordinate_count = deduped observations behind the representative point
#   - representative city/state = MODE (most frequent label, ties broken alphabetically)
#   - city_label_conflict_flag / state_label_conflict_flag = prefix maps to >1 distinct label
#     (8,556 city-conflict prefixes, 8 state-conflict prefixes)
#   - rep_point_out_of_brazil_flag = the averaged representative point falls outside Brazil
def build_zip_lookup(df):
    city_freq = df.groupBy("geolocation_zip_code_prefix", "geolocation_city").agg(
        count(lit(1)).alias("city_label_freq")
    )
    city_mode = (
        city_freq.withColumn("rn", row_number().over(
            Window.partitionBy("geolocation_zip_code_prefix")
                 .orderBy(col("city_label_freq").desc(), col("geolocation_city"))
        ))
        .filter(col("rn") == 1)
        .select("geolocation_zip_code_prefix", col("geolocation_city").alias("rep_city"))
    )

    state_freq = df.groupBy("geolocation_zip_code_prefix", "geolocation_state").agg(
        count(lit(1)).alias("state_label_freq")
    )
    state_mode = (
        state_freq.withColumn("rn", row_number().over(
            Window.partitionBy("geolocation_zip_code_prefix")
                 .orderBy(col("state_label_freq").desc(), col("geolocation_state"))
        ))
        .filter(col("rn") == 1)
        .select("geolocation_zip_code_prefix", col("geolocation_state").alias("rep_state"))
    )

    lookup = df.groupBy("geolocation_zip_code_prefix").agg(
        avg("geolocation_lat").alias("avg_geolocation_lat"),
        avg("geolocation_lng").alias("avg_geolocation_lng"),
        count(lit(1)).alias("source_coordinate_count"),
        countDistinct("geolocation_city").alias("distinct_city_labels"),
        countDistinct("geolocation_state").alias("distinct_state_labels")
    )
    lookup = lookup.join(city_mode, on="geolocation_zip_code_prefix", how="left")
    lookup = lookup.join(state_mode, on="geolocation_zip_code_prefix", how="left")
    return (
        lookup.withColumn(
            "city_label_conflict_flag",
            when(col("distinct_city_labels") > 1, lit(1)).otherwise(lit(0))
        )
        .withColumn(
            "state_label_conflict_flag",
            when(col("distinct_state_labels") > 1, lit(1)).otherwise(lit(0))
        )
        .withColumn(
            "rep_point_out_of_brazil_flag",
            when(
                (~((col("avg_geolocation_lat") > -33.75) & (col("avg_geolocation_lat") < 5.27))) |
                (~((col("avg_geolocation_lng") > -73.99) & (col("avg_geolocation_lng") < -34.73))),
                lit(1)
            ).otherwise(lit(0))
        )
    )
