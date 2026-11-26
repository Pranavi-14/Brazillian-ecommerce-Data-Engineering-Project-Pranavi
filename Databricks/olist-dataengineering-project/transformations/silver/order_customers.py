from pyspark.sql.functions import col,to_timestamp,when,lit,sum,abs,upper,trim

def changeSchema(df):
    return df.withColumn("customer_zip_code_prefix",col("customer_zip_code_prefix").cast("int"))

def dropnulls(df):
    return df.dropna(subset=["customer_id","customer_unique_id"])

def trimColumns(df):
    return df.withColumn("customer_city",(trim(col("customer_city"))))\
        .withColumn("customer_state",upper(trim(col("customer_state"))))

def dropduplicates(df):
    return df.dropDuplicates(["customer_id"])

def validate_brazilian_state_code(df):
    VALID_BRAZILIAN_STATE_CODES = [
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES",
        "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR",
        "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
        "SP", "SE", "TO"
    ]
    return df.filter(col("customer_state").isin(VALID_BRAZILIAN_STATE_CODES))



#customer unique id can be linked to 50+ orders
