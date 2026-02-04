# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion - Northwind Snapshots
# MAGIC 
# MAGIC This notebook ingests the Northwind snapshots (Day 1, Day 2, Day 3) into the Bronze layer with proper change tracking.
# MAGIC 
# MAGIC ## Bronze Layer Specifications
# MAGIC - Maintains historical snapshots
# MAGIC - Tracks change_type: insert, update, delete
# MAGIC - Tracks load_date for each snapshot
# MAGIC - Preserves all source data
# MAGIC 
# MAGIC ## Process
# MAGIC 1. Ingest Day 1 snapshot (all inserts)
# MAGIC 2. Ingest Day 2 snapshot (detect changes)
# MAGIC 3. Ingest Day 3 snapshot (detect changes)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, when, coalesce, md5, concat_ws
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

BRONZE_BASE_PATH = "Tables/bronze"

# Snapshot configurations
SNAPSHOTS = [
    {"day": "day1", "load_date": "2024-01-01"},
    {"day": "day2", "load_date": "2024-01-02"},
    {"day": "day3", "load_date": "2024-01-03"}
]

TABLES = ["categories", "suppliers", "products", "customers", "employees", "orders", "order_details"]

print("Bronze Layer Ingestion Configuration")
print("=" * 80)
print(f"Bronze Base Path: {BRONZE_BASE_PATH}")
print(f"Snapshots: {len(SNAPSHOTS)}")
print(f"Tables: {', '.join(TABLES)}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_primary_key_columns(table_name):
    """Return primary key column(s) for each table"""
    pk_map = {
        "categories": ["CategoryID"],
        "suppliers": ["SupplierID"],
        "products": ["ProductID"],
        "customers": ["CustomerID"],
        "employees": ["EmployeeID"],
        "orders": ["OrderID"],
        "order_details": ["OrderID", "ProductID"]
    }
    return pk_map.get(table_name, [])

def create_record_hash(df, exclude_cols=["change_type", "load_date"]):
    """Create a hash of all columns for change detection"""
    # Get all columns except the ones to exclude
    cols_to_hash = [c for c in df.columns if c not in exclude_cols]
    # Create hash from concatenated string of all column values
    return md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in cols_to_hash]))

def detect_changes(current_df, previous_df, pk_cols, table_name):
    """
    Detect inserts, updates, and deletes between two snapshots
    Returns DataFrame with change_type column
    """
    if previous_df is None:
        # First load - all records are inserts
        return current_df.withColumn("change_type", lit("insert"))
    
    # Add hash columns for comparison
    current_with_hash = current_df.withColumn("_record_hash", create_record_hash(current_df))
    previous_with_hash = previous_df.withColumn("_record_hash", create_record_hash(previous_df))
    
    # Detect inserts (in current but not in previous)
    inserts = current_with_hash.join(
        previous_with_hash.select(*pk_cols).withColumn("_exists", lit(1)),
        on=pk_cols,
        how="left"
    ).filter(col("_exists").isNull()).drop("_exists", "_record_hash").withColumn("change_type", lit("insert"))
    
    # Detect updates (in both but hash differs)
    updates = current_with_hash.alias("curr").join(
        previous_with_hash.alias("prev"),
        on=pk_cols,
        how="inner"
    ).filter(
        col("curr._record_hash") != col("prev._record_hash")
    ).select(
        [col(f"curr.{c}") for c in current_df.columns]
    ).withColumn("change_type", lit("update"))
    
    # Detect deletes (in previous but not in current)
    deletes = previous_with_hash.join(
        current_with_hash.select(*pk_cols).withColumn("_exists", lit(1)),
        on=pk_cols,
        how="left"
    ).filter(col("_exists").isNull()).drop("_exists", "_record_hash").withColumn("change_type", lit("delete"))
    
    # Detect no change (in both and hash matches)
    no_change = current_with_hash.alias("curr").join(
        previous_with_hash.alias("prev"),
        on=pk_cols,
        how="inner"
    ).filter(
        col("curr._record_hash") == col("prev._record_hash")
    ).select(
        [col(f"curr.{c}") for c in current_df.columns]
    ).withColumn("change_type", lit("no_change"))
    
    # Combine all changes
    result = inserts.union(updates).union(deletes).union(no_change)
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Each Table

# COMMAND ----------

# Track statistics
ingestion_stats = []

for table_name in TABLES:
    print(f"\n{'=' * 80}")
    print(f"Processing Table: {table_name.upper()}")
    print(f"{'=' * 80}")
    
    bronze_table_name = f"{BRONZE_BASE_PATH}_{table_name}"
    previous_snapshot = None
    
    # Process each snapshot
    for snapshot in SNAPSHOTS:
        day = snapshot["day"]
        load_date = snapshot["load_date"]
        source_table = f"Tables/source_data/{day}_{table_name}"
        
        print(f"\n  Loading {day} snapshot (load_date: {load_date})...")
        
        try:
            # Load current snapshot
            current_df = spark.table(source_table)
            
            # Get primary key columns
            pk_cols = get_primary_key_columns(table_name)
            
            # Detect changes
            changes_df = detect_changes(current_df, previous_snapshot, pk_cols, table_name)
            
            # Add load_date
            changes_df = changes_df.withColumn("load_date", lit(load_date))
            
            # Count changes by type
            change_counts = changes_df.groupBy("change_type").count().collect()
            counts_dict = {row.change_type: row['count'] for row in change_counts}
            
            print(f"    Inserts: {counts_dict.get('insert', 0)}")
            print(f"    Updates: {counts_dict.get('update', 0)}")
            print(f"    Deletes: {counts_dict.get('delete', 0)}")
            print(f"    No Change: {counts_dict.get('no_change', 0)}")
            
            # Append to bronze table
            changes_df.write.mode("append").format("delta").saveAsTable(bronze_table_name)
            
            # Track stats
            ingestion_stats.append({
                "table": table_name,
                "snapshot": day,
                "load_date": load_date,
                "inserts": counts_dict.get('insert', 0),
                "updates": counts_dict.get('update', 0),
                "deletes": counts_dict.get('delete', 0),
                "no_change": counts_dict.get('no_change', 0),
                "total": current_df.count()
            })
            
            # Set current snapshot as previous for next iteration
            previous_snapshot = current_df
            
        except Exception as e:
            print(f"    ERROR: {str(e)}")
            continue

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("BRONZE LAYER INGESTION COMPLETE")
print("=" * 80)

# Create summary DataFrame
summary_schema = StructType([
    StructField("table", StringType(), False),
    StructField("snapshot", StringType(), False),
    StructField("load_date", StringType(), False),
    StructField("inserts", IntegerType(), False),
    StructField("updates", IntegerType(), False),
    StructField("deletes", IntegerType(), False),
    StructField("no_change", IntegerType(), False),
    StructField("total", LongType(), False)
])

df_summary = spark.createDataFrame(ingestion_stats, summary_schema)

print("\nIngestion Summary:")
df_summary.show(100, False)

# Total counts by change type across all tables and snapshots
print("\nTotal Changes Across All Tables and Snapshots:")
df_summary.groupBy("snapshot").agg(
    {"inserts": "sum", "updates": "sum", "deletes": "sum", "no_change": "sum"}
).orderBy("snapshot").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Tables

# COMMAND ----------

print("\nBronze Table Record Counts:")
print("=" * 80)

for table_name in TABLES:
    bronze_table = f"{BRONZE_BASE_PATH}_{table_name}"
    try:
        count = spark.table(bronze_table).count()
        print(f"  {bronze_table}: {count} records")
        
        # Show sample records
        print(f"\n  Sample records from {bronze_table}:")
        spark.table(bronze_table).select("load_date", "change_type").groupBy("load_date", "change_type").count().orderBy("load_date", "change_type").show()
    except:
        print(f"  {bronze_table}: Table not found or error reading")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Examples
# MAGIC 
# MAGIC Example queries to explore the bronze layer data

# COMMAND ----------

# Example 1: View all changes for a specific table
print("\nExample 1: View all changes for Customers table")
spark.sql(f"""
    SELECT load_date, change_type, COUNT(*) as record_count
    FROM {BRONZE_BASE_PATH}_customers
    GROUP BY load_date, change_type
    ORDER BY load_date, change_type
""").show()

# Example 2: View inserted customers in Day 2
print("\nExample 2: New customers added in Day 2")
spark.sql(f"""
    SELECT CustomerID, CompanyName, ContactName, City, Country
    FROM {BRONZE_BASE_PATH}_customers
    WHERE load_date = '2024-01-02' AND change_type = 'insert'
""").show()

# Example 3: View deleted products
print("\nExample 3: Deleted products")
spark.sql(f"""
    SELECT load_date, ProductID, ProductName, change_type
    FROM {BRONZE_BASE_PATH}_products
    WHERE change_type = 'delete'
    ORDER BY load_date
""").show()

# Example 4: View updated orders
print("\nExample 4: Updated orders in Day 2")
spark.sql(f"""
    SELECT OrderID, CustomerID, OrderDate, ShipCity, ShipCountry, change_type
    FROM {BRONZE_BASE_PATH}_orders
    WHERE load_date = '2024-01-02' AND change_type = 'update'
    LIMIT 10
""").show()

print("\n" + "=" * 80)
print("Bronze layer ingestion completed successfully!")
print("=" * 80)
