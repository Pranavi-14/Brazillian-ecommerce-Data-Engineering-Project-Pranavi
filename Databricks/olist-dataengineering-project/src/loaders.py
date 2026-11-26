from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()

BRONZE_BASE_PATH = "abfss://bronze@brazillianstorage.dfs.core.windows.net"


def load_csv(dataset_name,business_date):

    if not business_date:
        raise ValueError("business_date is empty")

    filename = f"{dataset_name}.csv"
    path = (
        f"{BRONZE_BASE_PATH}/"
        f"{dataset_name}/"
        f"{business_date}/"
        f"{filename}"
    )

    return (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", "\\")
        .csv(path)
    )
def read_table(table):
    df = spark.read.table(table)
    return df
def load_parquet(path):
    df = spark.read.parquet(path)
