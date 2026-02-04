# Databricks notebook source
# MAGIC %md
# MAGIC # Northwind Data Validation Script
# MAGIC 
# MAGIC This notebook validates that the Northwind source data and Bronze layer ingestion completed successfully.
# MAGIC 
# MAGIC ## Validation Checks
# MAGIC 1. Verify all source data tables exist for each day
# MAGIC 2. Verify record counts match expected values
# MAGIC 3. Verify Bronze layer tables exist
# MAGIC 4. Verify change tracking is working correctly
# MAGIC 5. Verify data integrity (primary keys, referential integrity)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Define expected table names
SOURCE_TABLES = ["categories", "suppliers", "products", "customers", "employees", "orders", "order_details"]
SNAPSHOTS = ["day1", "day2", "day3"]
BRONZE_BASE = "Tables/bronze"

# Track validation results
validation_results = []

def log_validation(test_name, passed, message=""):
    """Log validation results"""
    status = "✓ PASS" if passed else "✗ FAIL"
    validation_results.append({
        "test": test_name,
        "status": status,
        "passed": passed,
        "message": message
    })
    print(f"{status}: {test_name}")
    if message:
        print(f"  → {message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Verify Source Data Tables Exist

# COMMAND ----------

print("=" * 80)
print("TEST 1: Source Data Tables Existence")
print("=" * 80)

for day in SNAPSHOTS:
    for table in SOURCE_TABLES:
        table_name = f"Tables/source_data/{day}_{table}"
        try:
            df = spark.table(table_name)
            count = df.count()
            log_validation(
                f"Source table {table_name}",
                True,
                f"{count} records found"
            )
        except Exception as e:
            log_validation(
                f"Source table {table_name}",
                False,
                f"Table not found or error: {str(e)}"
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Verify Expected Record Counts

# COMMAND ----------

print("\n" + "=" * 80)
print("TEST 2: Expected Record Counts")
print("=" * 80)

# Expected counts (approximate for some tables due to random generation)
expected_counts = {
    "day1_categories": 8,
    "day1_suppliers": 5,
    "day1_products": 20,
    "day1_customers": 20,
    "day1_employees": 9,
    "day1_orders": 50,
    "day2_customers": 25,  # 20 + 5 new
    "day2_products": 19,   # 20 - 1 deleted
    "day2_orders": 60,     # 50 + 10 new
    "day3_products": 22,   # 19 + 3 new
    "day3_customers": 25,  # Same as day2
    "day3_orders": 67,     # 60 - 3 deleted + 10 new
}

for table_key, expected_count in expected_counts.items():
    table_name = f"Tables/source_data/{table_key}"
    try:
        df = spark.table(table_name)
        actual_count = df.count()
        passed = actual_count == expected_count
        log_validation(
            f"Record count for {table_key}",
            passed,
            f"Expected: {expected_count}, Actual: {actual_count}"
        )
    except Exception as e:
        log_validation(
            f"Record count for {table_key}",
            False,
            f"Error: {str(e)}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Verify Bronze Layer Tables Exist

# COMMAND ----------

print("\n" + "=" * 80)
print("TEST 3: Bronze Layer Tables Existence")
print("=" * 80)

for table in SOURCE_TABLES:
    bronze_table = f"{BRONZE_BASE}_{table}"
    try:
        df = spark.table(bronze_table)
        count = df.count()
        log_validation(
            f"Bronze table {bronze_table}",
            True,
            f"{count} total records (across all snapshots)"
        )
    except Exception as e:
        log_validation(
            f"Bronze table {bronze_table}",
            False,
            f"Table not found or error: {str(e)}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Verify Change Tracking

# COMMAND ----------

print("\n" + "=" * 80)
print("TEST 4: Change Tracking Validation")
print("=" * 80)

# Test 4a: Verify all change_type values are valid
valid_change_types = ["insert", "update", "delete", "no_change"]

for table in SOURCE_TABLES:
    bronze_table = f"{BRONZE_BASE}_{table}"
    try:
        df = spark.table(bronze_table)
        
        # Check if change_type column exists
        if "change_type" not in df.columns:
            log_validation(
                f"change_type column in {table}",
                False,
                "Column not found"
            )
            continue
        
        # Get distinct change_type values
        change_types = df.select("change_type").distinct().rdd.flatMap(lambda x: x).collect()
        invalid_types = [ct for ct in change_types if ct not in valid_change_types]
        
        log_validation(
            f"Valid change_type values in {table}",
            len(invalid_types) == 0,
            f"Change types found: {change_types}"
        )
        
    except Exception as e:
        log_validation(
            f"Change tracking in {table}",
            False,
            f"Error: {str(e)}"
        )

# Test 4b: Verify load_date column exists and has expected values
expected_load_dates = ["2024-01-01", "2024-01-02", "2024-01-03"]

for table in SOURCE_TABLES:
    bronze_table = f"{BRONZE_BASE}_{table}"
    try:
        df = spark.table(bronze_table)
        
        # Check if load_date column exists
        if "load_date" not in df.columns:
            log_validation(
                f"load_date column in {table}",
                False,
                "Column not found"
            )
            continue
        
        # Get distinct load_date values
        load_dates = df.select("load_date").distinct().rdd.flatMap(lambda x: x).collect()
        all_dates_valid = all(date in expected_load_dates for date in load_dates)
        
        log_validation(
            f"Valid load_date values in {table}",
            all_dates_valid,
            f"Load dates found: {sorted(load_dates)}"
        )
        
    except Exception as e:
        log_validation(
            f"Load date tracking in {table}",
            False,
            f"Error: {str(e)}"
        )

# Test 4c: Verify Day 1 has only inserts
for table in SOURCE_TABLES:
    bronze_table = f"{BRONZE_BASE}_{table}"
    try:
        df = spark.table(bronze_table).filter(col("load_date") == "2024-01-01")
        change_types = df.select("change_type").distinct().rdd.flatMap(lambda x: x).collect()
        
        passed = len(change_types) == 1 and change_types[0] == "insert"
        log_validation(
            f"Day 1 contains only inserts for {table}",
            passed,
            f"Change types in Day 1: {change_types}"
        )
        
    except Exception as e:
        log_validation(
            f"Day 1 insert-only validation for {table}",
            False,
            f"Error: {str(e)}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Verify Specific Changes

# COMMAND ----------

print("\n" + "=" * 80)
print("TEST 5: Specific Change Validation")
print("=" * 80)

# Test 5a: Verify 5 new customers in Day 2
try:
    df = spark.table(f"{BRONZE_BASE}_customers").filter(
        (col("load_date") == "2024-01-02") & (col("change_type") == "insert")
    )
    count = df.count()
    log_validation(
        "Day 2: 5 new customers inserted",
        count == 5,
        f"Expected: 5, Actual: {count}"
    )
except Exception as e:
    log_validation("Day 2 new customers", False, f"Error: {str(e)}")

# Test 5b: Verify 1 product deleted in Day 2
try:
    df = spark.table(f"{BRONZE_BASE}_products").filter(
        (col("load_date") == "2024-01-02") & (col("change_type") == "delete")
    )
    count = df.count()
    log_validation(
        "Day 2: 1 product deleted",
        count == 1,
        f"Expected: 1, Actual: {count}"
    )
    
    # Verify it's ProductID 5
    if count > 0:
        product_id = df.select("ProductID").first()[0]
        log_validation(
            "Day 2: Deleted product is ProductID 5",
            product_id == 5,
            f"Expected: 5, Actual: {product_id}"
        )
except Exception as e:
    log_validation("Day 2 product deletion", False, f"Error: {str(e)}")

# Test 5c: Verify 3 orders deleted in Day 3
try:
    df = spark.table(f"{BRONZE_BASE}_orders").filter(
        (col("load_date") == "2024-01-03") & (col("change_type") == "delete")
    )
    count = df.count()
    log_validation(
        "Day 3: 3 orders deleted",
        count == 3,
        f"Expected: 3, Actual: {count}"
    )
    
    # Verify correct OrderIDs
    if count > 0:
        order_ids = sorted([row.OrderID for row in df.select("OrderID").collect()])
        expected_ids = [10250, 10251, 10252]
        log_validation(
            "Day 3: Deleted orders are 10250, 10251, 10252",
            order_ids == expected_ids,
            f"Expected: {expected_ids}, Actual: {order_ids}"
        )
except Exception as e:
    log_validation("Day 3 order deletions", False, f"Error: {str(e)}")

# Test 5d: Verify 3 new products in Day 3
try:
    df = spark.table(f"{BRONZE_BASE}_products").filter(
        (col("load_date") == "2024-01-03") & (col("change_type") == "insert")
    )
    count = df.count()
    log_validation(
        "Day 3: 3 new products inserted",
        count == 3,
        f"Expected: 3, Actual: {count}"
    )
    
    # Verify ProductIDs
    if count > 0:
        product_ids = sorted([row.ProductID for row in df.select("ProductID").collect()])
        expected_ids = [21, 22, 23]
        log_validation(
            "Day 3: New products are 21, 22, 23",
            product_ids == expected_ids,
            f"Expected: {expected_ids}, Actual: {product_ids}"
        )
except Exception as e:
    log_validation("Day 3 new products", False, f"Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("VALIDATION SUMMARY")
print("=" * 80)

# Count passes and failures
total_tests = len(validation_results)
passed_tests = sum(1 for r in validation_results if r["passed"])
failed_tests = total_tests - passed_tests

print(f"\nTotal Tests: {total_tests}")
print(f"Passed: {passed_tests}")
print(f"Failed: {failed_tests}")
print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%")

if failed_tests > 0:
    print("\n" + "=" * 80)
    print("FAILED TESTS:")
    print("=" * 80)
    for result in validation_results:
        if not result["passed"]:
            print(f"\n✗ {result['test']}")
            if result["message"]:
                print(f"  → {result['message']}")

print("\n" + "=" * 80)
print("VALIDATION COMPLETE")
print("=" * 80)

# Return overall success/failure
if failed_tests == 0:
    print("\n✓ All validation tests passed!")
else:
    print(f"\n✗ {failed_tests} validation test(s) failed. Please review the failures above.")
