# def write_delta(df,table_name):
#     df.write.mode("overwrite").format("delta").saveAsTable(table_name)

# src/writer.py — idempotent version

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.getOrCreate()

def write_delta(df, table_name, layer, merge_key=None):
    print("entered write delta")
    """
    layer      — 'bronze' / 'silver' / 'gold' / 'quarantine'
    merge_key  — column to deduplicate on (required for silver/gold)
    """

    # Create schema if it doesn't exist
    _ensure_schema_exists(table_name)

    if layer == "bronze":
        # Bronze is a full reload of the source files — overwrite so reruns
        # never duplicate rows. overwriteSchema lets source schema changes land.
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)

    elif layer in ("silver", "gold"):
        # Silver and Gold use MERGE — safe to run multiple times,
        # with automatic schema evolution (new source columns are
        # ALTER-added to the target so the table is never dropped)
        if merge_key is None:
            raise ValueError(f"merge_key is required for {layer} layer")

        _merge_write(df, table_name, merge_key)

    elif layer == "quarantine":
        # Quarantine keeps every violation. With a merge_key, MERGE so reruns of
        # the same source are idempotent (no duplicate quarantine rows); without
        # one, keep the historical append behaviour (caller resets on rebuild).
        if merge_key is None:
            df.write \
              .format("delta") \
              .mode("append") \
              .saveAsTable(table_name)
        else:
            _merge_write(df, table_name, merge_key)


def _ensure_schema_exists(table_name):
    """
    Extract schema from table_name and create it if it doesn't exist.
    Handles table names like 'schema.table' or 'catalog.schema.table'
    """
    parts = table_name.split('.')
    print(f"Parts: {parts}")


    if len(parts) == 2:
        # Format: schema.table
        schema_name = parts[0]
        catalog_name = spark.sql("SELECT current_catalog()").collect()[0][0]
        full_schema = f"{catalog_name}.{schema_name}"
        print(f"Creating schema: {full_schema}")
    elif len(parts) == 3:
        # Format: catalog.schema.table
        catalog_name = parts[0]
        schema_name = parts[1]
        full_schema = f"{catalog_name}.{schema_name}"
        print(f"Creating schema: {full_schema}")
    else:
        # Just table name, use current schema
        return

    # try:
    #     spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    # except Exception as e:
    #     # If schema creation fails, log but don't fail the entire write
    #     print(f"Warning: Could not create schema {full_schema}: {e}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")


def _describe_table(table_name):
    """Return [(col_name, data_type)] for the table's columns, in order."""
    rows = spark.sql(f"DESCRIBE TABLE {table_name}").collect()
    cols = []
    for r in rows:
        name = r["col_name"]
        dtype = r["data_type"]
        # skip section headers ('# Partition Information', etc.) and blank rows
        if name and not str(name).startswith("#") and dtype:
            cols.append((name, dtype))
    return cols


def _merge_write(df, table_name, merge_key):
    """
    If table does not exist yet — just create it.
    If it exists — MERGE with automatic schema evolution:
      1. ALTER TABLE ADD COLUMNS for source columns the target is missing
         (new flag columns land WITHOUT dropping the table, so downstream
         tables that join against it stay intact).
      2. Cast + reorder source columns to match the target exactly
         (updateAll/insertAll require matching column sets).
      3. MERGE on merge_key: matched rows are updated (new flags filled in
         for existing rows), new rows inserted. Never duplicates.
    """
    from delta.tables import DeltaTable

    # First time — table doesn't exist yet, just create it
    if not _table_exists(table_name):
        df.write \
          .format("delta") \
          .saveAsTable(table_name)
        print(f"✓ Created new table: {table_name}")
        return

    # Table exists — evolve its schema first if the source has new columns
    target_cols = _describe_table(table_name)
    target_names = {name for name, _ in target_cols}

    missing_in_target = [
        (name, dtype) for name, dtype in df.dtypes if name not in target_names
    ]
    if missing_in_target:
        ddl = ", ".join(f"`{name}` {dtype}" for name, dtype in missing_in_target)
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({ddl})")
        print(
            f"✓ Schema evolved — added to {table_name}: "
            f"{[n for n, _ in missing_in_target]}"
        )
        target_cols = _describe_table(table_name)

    missing_in_source = [name for name, _ in target_cols if name not in df.columns]
    if missing_in_source:
        raise ValueError(
            f"Cannot merge into {table_name}: target has columns the source "
            f"DataFrame lacks: {missing_in_source}. Add them to the source or "
            f"drop them from the table."
        )

    # Align source to the target's column order + types before MERGE
    aligned_df = df.select(
        *[col(name).cast(dtype).alias(name) for name, dtype in target_cols]
    )

    # Table exists — use MERGE
    delta_table = DeltaTable.forName(spark, table_name)

    # merge_key: single column name, or list of columns for a composite key
    if isinstance(merge_key, (list, tuple)):
        keys = list(merge_key)
    else:
        keys = [merge_key]
    merge_condition = " AND ".join(f"target.`{k}` = source.`{k}`" for k in keys)

    delta_table.alias("target").merge(
        aligned_df.alias("source"),
        merge_condition
    ) .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    # row exists → update it (new flag columns get their values)
    # row is new → insert it

    print(f"✓ Merged into: {table_name}")


def _table_exists(table_name):
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except:
        return False
