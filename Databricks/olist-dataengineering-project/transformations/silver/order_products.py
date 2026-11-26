from pyspark.sql.functions import col,lower,trim,when,lit,coalesce

# Translation README §4.1: 2 categories (pc_gamer, portateis_cozinha_e_preparadores_de_alimentos)
# have no English translation and 610 products have a null category — always LEFT JOIN and
# COALESCE(t.product_category_name_english, p.product_category_name, 'uncategorized').
def enrich_with_translation(df, translation_df):
    translation = translation_df.select(
        lower(trim(col("product_category_name"))).alias("product_category_name"),
        col("product_category_name_english").alias("product_category_name_english")
    )
    df = df.join(translation, on = "product_category_name", how = "left")
    df = df.withColumn(
        "category_translation_missing_flag",
        when(
            col("product_category_name").isNotNull() & col("product_category_name_english").isNull(),
            lit(1)
        ).otherwise(lit(0))
    )
    return df.withColumn(
        "product_category_name_resolved",
        coalesce(col("product_category_name_english"), col("product_category_name"), lit("uncategorized"))
    )

def changeSchema(df):
    return df.withColumn("product_name_lenght",col("product_name_lenght").cast("double").cast("int"))\
        .withColumn("product_description_lenght",col("product_description_lenght").cast("double").cast("int"))\
        .withColumn("product_photos_qty",col("product_photos_qty").cast("double").cast("int"))\
        .withColumn("product_weight_g",col("product_weight_g").cast("float"))\
        .withColumn("product_length_cm",col("product_length_cm").cast("float"))\
        .withColumn("product_height_cm",col("product_height_cm").cast("float"))\
        .withColumn("product_width_cm",col("product_width_cm").cast("float"))

# README §3: the 'lenght' column-name typo is retained as published on Kaggle — do not rename.

def validate_product_category_name(df):
    return df.withColumn("product_category_name",lower(trim(col("product_category_name"))))

# README §4.3: the 610 rows with a null category (unpopulated catalog content at
# registration) are a documented pattern — keep them and flag; only a null product_id is unusable.
def dropnulls(df):
    return df.dropna(subset = ["product_id"])

def flag_category_missing(df):
    return df.withColumn(
        "category_missing_flag",
        when(col("product_category_name").isNull(), lit(1)).otherwise(lit(0))
    )

def dropduplicates(df):
    return df.dropDuplicates(["product_id"])

# README §6: weight spans 0..40,425 g — flag anything outside the documented range
# (non-positive weight, or at/above the documented max of 40,425 g). Keep and flag, never drop.
def flag_weight_out_of_range(df):
    return df.withColumn(
        "weight_out_of_range_flag",
        when(
            (col("product_weight_g") <= 0) | (col("product_weight_g") >= 40425),
            lit(1)
        ).otherwise(lit(0))
    )

def validateLength(df):
    return df.filter((col("product_length_cm") > 0) | col("product_length_cm").isNull())

def validateHeight(df):
    return df.filter((col("product_height_cm") > 0) | col("product_height_cm").isNull())

def validateWidth(df):
    return df.filter((col("product_width_cm") > 0) | col("product_width_cm").isNull())

def validatephotoqty(df):
    return df.filter(((col("product_photos_qty") > 0) & (col("product_photos_qty") <=20)) | (col("product_photos_qty").isNull()))

def validateproductname(df):
    return df.filter((col("product_name_lenght") > 0) | col("product_name_lenght").isNull())

def validateproductdescription(df):
    return df.filter((col("product_description_lenght") > 0 ) | col("product_description_lenght").isNull())
