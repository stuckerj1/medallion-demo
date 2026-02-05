# Databricks notebook source
# MAGIC %md
# MAGIC # Source Data Snapshot Validation
# MAGIC 
# MAGIC This notebook validates that source data snapshots meet the specification for daily full snapshot ingestion.
# MAGIC 
# MAGIC ## Validation Requirements
# MAGIC The source data must follow a **daily full snapshot load & compare approach**:
# MAGIC - **Day 1**: Initial full snapshot (baseline data)
# MAGIC - **Day 2**: Complete full snapshot with incremental changes (inserts/updates/deletes compared to Day 1)
# MAGIC - **Day 3**: Complete full snapshot with incremental changes (inserts/updates/deletes compared to Day 2)
# MAGIC 
# MAGIC Each day's data represents a **full snapshot** of the source system at that point in time,
# MAGIC NOT just the changes. The Bronze layer will compare snapshots to detect changes.
# MAGIC 
# MAGIC ## Validation Checks
# MAGIC 1. All three daily snapshots exist
# MAGIC 2. Each snapshot contains complete data (not just deltas)
# MAGIC 3. Day 1 establishes the baseline
# MAGIC 4. Day 2 shows measurable changes compared to Day 1
# MAGIC 5. Day 3 shows measurable changes compared to Day 2
# MAGIC 6. Changes include inserts, updates, and deletes as expected

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Define table names and paths
TABLES = ["categories", "suppliers", "products", "customers", "employees", "orders", "order_details"]
DAYS = ["day1", "day2", "day3"]
BASE_PATH = "Tables/source_data"

# Track validation results
validation_results = []
change_summary = []

def log_validation(test_name, passed, message="", severity="INFO"):
    """Log validation results"""
    status = "✓ PASS" if passed else "✗ FAIL"
    validation_results.append({
        "test": test_name,
        "status": status,
        "passed": passed,
        "message": message,
        "severity": severity
    })
    print(f"{status}: {test_name}")
    if message:
        print(f"  → {message}")

def log_change(day, table, change_type, count_value, details=""):
    """Log detected changes"""
    change_summary.append({
        "day": day,
        "table": table,
        "change_type": change_type,
        "count": count_value,
        "details": details
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Verify All Snapshot Tables Exist

# COMMAND ----------

print("=" * 80)
print("TEST 1: Daily Snapshot Table Existence")
print("=" * 80)
print("\nVerifying all three daily snapshots exist for each table...")
print()

for table in TABLES:
    for day in DAYS:
        table_name = f"{BASE_PATH}/{day}_{table}"
        try:
            df = spark.table(table_name)
            count = df.count()
            log_validation(
                f"Snapshot exists: {day}_{table}",
                True,
                f"{count} records",
                "INFO"
            )
        except Exception as e:
            log_validation(
                f"Snapshot exists: {day}_{table}",
                False,
                f"Table not found: {str(e)}",
                "CRITICAL"
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Verify Each Day is a FULL Snapshot (Not Just Deltas)

# COMMAND ----------

print("\n" + "=" * 80)
print("TEST 2: Full Snapshot Validation")
print("=" * 80)
print("\nVerifying each day contains FULL snapshots, not just incremental changes...")
print()

# Expected minimum record counts to verify we have full data, not just changes
expected_minimums = {
    "categories": 8,
    "suppliers": 5,
    "employees": 9,
    "products": 15,  # Will have deletes/adds but should stay substantial
    "customers": 20,  # Will grow
    "orders": 40,     # Will grow and have some deletes
    "order_details": 50  # Will grow with new orders
}

for day in DAYS:
    print(f"\n{day.upper()} Snapshot Contents:")
    print("-" * 60)
    
    for table in TABLES:
        table_name = f"{BASE_PATH}/{day}_{table}"
        try:
            df = spark.table(table_name)
            count = df.count()
            
            # Check if this is a full snapshot (has substantial data)
            min_expected = expected_minimums.get(table, 1)
            is_full_snapshot = count >= min_expected
            
            log_validation(
                f"{day}_{table} is full snapshot",
                is_full_snapshot,
                f"{count} records (minimum expected: {min_expected})",
                "CRITICAL" if not is_full_snapshot else "INFO"
            )
            
        except Exception as e:
            log_validation(
                f"{day}_{table} full snapshot check",
                False,
                f"Error: {str(e)}",
                "CRITICAL"
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Verify Day 1 Baseline Data

# COMMAND ----------

print("\n" + "=" * 80)
print("TEST 3: Day 1 Baseline Validation")
print("=" * 80)
print("\nVerifying Day 1 contains the expected baseline data...")
print()

# Expected exact counts for Day 1
day1_expected = {
    "categories": 8,
    "suppliers": 5,
    "products": 20,
    "customers": 20,
    "employees": 9,
    "orders": 50,
}

for table, expected_count in day1_expected.items():
    table_name = f"{BASE_PATH}/day1_{table}"
    try:
        df = spark.table(table_name)
        actual_count = df.count()
        
        passed = actual_count == expected_count
        log_validation(
            f"Day 1 {table} count",
            passed,
            f"Expected: {expected_count}, Actual: {actual_count}",
            "ERROR" if not passed else "INFO"
        )
        
    except Exception as e:
        log_validation(
            f"Day 1 {table} validation",
            False,
            f"Error: {str(e)}",
            "CRITICAL"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Detect Changes Between Day 1 and Day 2

# COMMAND ----------

print("\n" + "=" * 80)
print("TEST 4: Day 1 → Day 2 Change Detection")
print("=" * 80)
print("\nComparing Day 2 full snapshot to Day 1 full snapshot to detect changes...")
print()

# Expected changes from Day 1 to Day 2
expected_changes_d1_d2 = {
    "customers": {
        "day1_count": 20,
        "day2_count": 25,
        "inserts": 5,
        "deletes": 0,
        "net_change": +5
    },
    "products": {
        "day1_count": 20,
        "day2_count": 19,
        "inserts": 0,
        "deletes": 1,  # Product ID 5 deleted
        "net_change": -1
    },
    "orders": {
        "day1_count": 50,
        "day2_count": 60,
        "inserts": 10,
        "deletes": 0,
        "net_change": +10
    }
}

for table, expectations in expected_changes_d1_d2.items():
    try:
        df_day1 = spark.table(f"{BASE_PATH}/day1_{table}")
        df_day2 = spark.table(f"{BASE_PATH}/day2_{table}")
        
        count_day1 = df_day1.count()
        count_day2 = df_day2.count()
        net_change = count_day2 - count_day1
        
        # Validate counts
        count_match = (count_day1 == expectations["day1_count"] and 
                      count_day2 == expectations["day2_count"])
        
        log_validation(
            f"Day 1→2 {table} counts correct",
            count_match,
            f"Day1: {count_day1} (exp: {expectations['day1_count']}), "
            f"Day2: {count_day2} (exp: {expectations['day2_count']}), "
            f"Net change: {net_change:+d}",
            "ERROR" if not count_match else "INFO"
        )
        
        # Validate net change
        net_change_match = net_change == expectations["net_change"]
        log_validation(
            f"Day 1→2 {table} net change",
            net_change_match,
            f"Expected: {expectations['net_change']:+d}, Actual: {net_change:+d}",
            "ERROR" if not net_change_match else "INFO"
        )
        
        # Log the change for summary
        log_change("Day 1→2", table, "net_change", net_change,
                  f"{expectations['inserts']} inserts, {expectations['deletes']} deletes")
        
    except Exception as e:
        log_validation(
            f"Day 1→2 {table} comparison",
            False,
            f"Error: {str(e)}",
            "ERROR"
        )

# Verify specific change: Product ID 5 deleted
print("\nVerifying specific deletion: Product ID 5...")
try:
    df_day1_products = spark.table(f"{BASE_PATH}/day1_products")
    df_day2_products = spark.table(f"{BASE_PATH}/day2_products")
    
    product_5_in_day1 = df_day1_products.filter(col("ProductID") == 5).count()
    product_5_in_day2 = df_day2_products.filter(col("ProductID") == 5).count()
    
    deleted_correctly = product_5_in_day1 == 1 and product_5_in_day2 == 0
    log_validation(
        "Day 1→2 Product ID 5 deleted",
        deleted_correctly,
        f"Day 1: {product_5_in_day1}, Day 2: {product_5_in_day2}",
        "ERROR" if not deleted_correctly else "INFO"
    )
except Exception as e:
    log_validation(
        "Day 1→2 Product ID 5 deletion check",
        False,
        f"Error: {str(e)}",
        "ERROR"
    )

# Verify specific change: 5 new customers added
print("\nVerifying specific insertions: 5 new customers...")
expected_new_customers = ["FRANK", "FRANS", "FURIB", "GALED", "GODOS"]
try:
    df_day1_customers = spark.table(f"{BASE_PATH}/day1_customers")
    df_day2_customers = spark.table(f"{BASE_PATH}/day2_customers")
    
    day1_customer_ids = set([row.CustomerID for row in df_day1_customers.select("CustomerID").collect()])
    day2_customer_ids = set([row.CustomerID for row in df_day2_customers.select("CustomerID").collect()])
    
    new_customers = day2_customer_ids - day1_customer_ids
    new_customers_correct = new_customers == set(expected_new_customers)
    
    log_validation(
        "Day 1→2 New customers added",
        new_customers_correct,
        f"Expected: {expected_new_customers}, Actual: {sorted(list(new_customers))}",
        "ERROR" if not new_customers_correct else "INFO"
    )
except Exception as e:
    log_validation(
        "Day 1→2 New customers check",
        False,
        f"Error: {str(e)}",
        "ERROR"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Detect Changes Between Day 2 and Day 3

# COMMAND ----------

print("\n" + "=" * 80)
print("TEST 5: Day 2 → Day 3 Change Detection")
print("=" * 80)
print("\nComparing Day 3 full snapshot to Day 2 full snapshot to detect changes...")
print()

# Expected changes from Day 2 to Day 3
expected_changes_d2_d3 = {
    "products": {
        "day2_count": 19,
        "day3_count": 22,
        "inserts": 3,  # Products 21, 22, 23 added
        "deletes": 0,
        "net_change": +3
    },
    "customers": {
        "day2_count": 25,
        "day3_count": 25,
        "inserts": 0,
        "deletes": 0,
        "net_change": 0  # Count stays same, but 15 ContactTitle values updated
    },
    "orders": {
        "day2_count": 60,
        "day3_count": 67,
        "inserts": 10,
        "deletes": 3,  # Orders 10250, 10251, 10252 deleted
        "net_change": +7
    }
}

for table, expectations in expected_changes_d2_d3.items():
    try:
        df_day2 = spark.table(f"{BASE_PATH}/day2_{table}")
        df_day3 = spark.table(f"{BASE_PATH}/day3_{table}")
        
        count_day2 = df_day2.count()
        count_day3 = df_day3.count()
        net_change = count_day3 - count_day2
        
        # Validate counts
        count_match = (count_day2 == expectations["day2_count"] and 
                      count_day3 == expectations["day3_count"])
        
        log_validation(
            f"Day 2→3 {table} counts correct",
            count_match,
            f"Day2: {count_day2} (exp: {expectations['day2_count']}), "
            f"Day3: {count_day3} (exp: {expectations['day3_count']}), "
            f"Net change: {net_change:+d}",
            "ERROR" if not count_match else "INFO"
        )
        
        # Validate net change
        net_change_match = net_change == expectations["net_change"]
        log_validation(
            f"Day 2→3 {table} net change",
            net_change_match,
            f"Expected: {expectations['net_change']:+d}, Actual: {net_change:+d}",
            "ERROR" if not net_change_match else "INFO"
        )
        
        # Log the change for summary
        log_change("Day 2→3", table, "net_change", net_change,
                  f"{expectations['inserts']} inserts, {expectations['deletes']} deletes")
        
    except Exception as e:
        log_validation(
            f"Day 2→3 {table} comparison",
            False,
            f"Error: {str(e)}",
            "ERROR"
        )

# Verify specific changes: 3 new products added
print("\nVerifying specific insertions: 3 new products...")
expected_new_product_ids = [21, 22, 23]
try:
    df_day2_products = spark.table(f"{BASE_PATH}/day2_products")
    df_day3_products = spark.table(f"{BASE_PATH}/day3_products")
    
    day2_product_ids = set([row.ProductID for row in df_day2_products.select("ProductID").collect()])
    day3_product_ids = set([row.ProductID for row in df_day3_products.select("ProductID").collect()])
    
    new_products = day3_product_ids - day2_product_ids
    new_products_correct = new_products == set(expected_new_product_ids)
    
    log_validation(
        "Day 2→3 New products added",
        new_products_correct,
        f"Expected: {expected_new_product_ids}, Actual: {sorted(list(new_products))}",
        "ERROR" if not new_products_correct else "INFO"
    )
except Exception as e:
    log_validation(
        "Day 2→3 New products check",
        False,
        f"Error: {str(e)}",
        "ERROR"
    )

# Verify specific deletions: 3 orders deleted
print("\nVerifying specific deletions: 3 orders...")
expected_deleted_orders = [10250, 10251, 10252]
try:
    df_day2_orders = spark.table(f"{BASE_PATH}/day2_orders")
    df_day3_orders = spark.table(f"{BASE_PATH}/day3_orders")
    
    day2_order_ids = set([row.OrderID for row in df_day2_orders.select("OrderID").collect()])
    day3_order_ids = set([row.OrderID for row in df_day3_orders.select("OrderID").collect()])
    
    deleted_orders = day2_order_ids - day3_order_ids
    deleted_orders_correct = deleted_orders == set(expected_deleted_orders)
    
    log_validation(
        "Day 2→3 Orders deleted",
        deleted_orders_correct,
        f"Expected: {expected_deleted_orders}, Actual: {sorted(list(deleted_orders))}",
        "ERROR" if not deleted_orders_correct else "INFO"
    )
except Exception as e:
    log_validation(
        "Day 2→3 Deleted orders check",
        False,
        f"Error: {str(e)}",
        "ERROR"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Verify Update Operations Can Be Detected

# COMMAND ----------

print("\n" + "=" * 80)
print("TEST 6: Update Detection Validation")
print("=" * 80)
print("\nVerifying that updates can be detected by comparing snapshots...")
print()

# Day 2→Day 3: 15 customers should have updated ContactTitle
print("Checking for customer ContactTitle updates in Day 3...")
try:
    df_day2_customers = spark.table(f"{BASE_PATH}/day2_customers")
    df_day3_customers = spark.table(f"{BASE_PATH}/day3_customers")
    
    # Join on CustomerID and compare ContactTitle
    df_joined = df_day2_customers.alias("d2").join(
        df_day3_customers.alias("d3"),
        col("d2.CustomerID") == col("d3.CustomerID"),
        "inner"
    )
    
    # Count where ContactTitle changed
    updates = df_joined.filter(col("d2.ContactTitle") != col("d3.ContactTitle")).count()
    
    # Should have 15 updates
    updates_correct = updates == 15
    log_validation(
        "Day 2→3 Customer ContactTitle updates",
        updates_correct,
        f"Expected: 15 updates, Actual: {updates} updates detected",
        "WARNING" if not updates_correct else "INFO"
    )
    
    log_change("Day 2→3", "customers", "updates", updates, "ContactTitle field changes")
    
except Exception as e:
    log_validation(
        "Day 2→3 Customer update detection",
        False,
        f"Error: {str(e)}",
        "WARNING"
    )

# Day 1→Day 2: 20 orders should have updated ShipCity
print("\nChecking for order ShipCity updates in Day 2...")
try:
    df_day1_orders = spark.table(f"{BASE_PATH}/day1_orders")
    df_day2_orders = spark.table(f"{BASE_PATH}/day2_orders")
    
    # Join on OrderID and compare ShipCity
    df_joined = df_day1_orders.alias("d1").join(
        df_day2_orders.alias("d2"),
        col("d1.OrderID") == col("d2.OrderID"),
        "inner"
    )
    
    # Count where ShipCity changed
    updates = df_joined.filter(col("d1.ShipCity") != col("d2.ShipCity")).count()
    
    # Should have 20 updates
    updates_correct = updates == 20
    log_validation(
        "Day 1→2 Order ShipCity updates",
        updates_correct,
        f"Expected: 20 updates, Actual: {updates} updates detected",
        "WARNING" if not updates_correct else "INFO"
    )
    
    log_change("Day 1→2", "orders", "updates", updates, "ShipCity field changes")
    
except Exception as e:
    log_validation(
        "Day 1→2 Order update detection",
        False,
        f"Error: {str(e)}",
        "WARNING"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Verify Data Integrity Across Snapshots

# COMMAND ----------

print("\n" + "=" * 80)
print("TEST 7: Data Integrity Validation")
print("=" * 80)
print("\nVerifying referential integrity within each snapshot...")
print()

for day in DAYS:
    print(f"\n{day.upper()} Integrity Checks:")
    print("-" * 60)
    
    try:
        # Check Orders reference valid Customers
        df_orders = spark.table(f"{BASE_PATH}/{day}_orders")
        df_customers = spark.table(f"{BASE_PATH}/{day}_customers")
        
        customer_ids = set([row.CustomerID for row in df_customers.select("CustomerID").collect()])
        order_customer_ids = [row.CustomerID for row in df_orders.select("CustomerID").collect()]
        
        orphaned_orders = [cid for cid in order_customer_ids if cid not in customer_ids]
        
        log_validation(
            f"{day} Orders → Customers integrity",
            len(orphaned_orders) == 0,
            f"Found {len(orphaned_orders)} orphaned orders" if orphaned_orders else "All orders reference valid customers",
            "ERROR" if orphaned_orders else "INFO"
        )
        
        # Check OrderDetails reference valid Orders
        df_order_details = spark.table(f"{BASE_PATH}/{day}_order_details")
        
        order_ids = set([row.OrderID for row in df_orders.select("OrderID").collect()])
        detail_order_ids = [row.OrderID for row in df_order_details.select("OrderID").collect()]
        
        orphaned_details = [oid for oid in detail_order_ids if oid not in order_ids]
        
        log_validation(
            f"{day} OrderDetails → Orders integrity",
            len(orphaned_details) == 0,
            f"Found {len(orphaned_details)} orphaned order details" if orphaned_details else "All order details reference valid orders",
            "ERROR" if orphaned_details else "INFO"
        )
        
        # Check OrderDetails reference valid Products
        df_products = spark.table(f"{BASE_PATH}/{day}_products")
        
        product_ids = set([row.ProductID for row in df_products.select("ProductID").collect()])
        detail_product_ids = [row.ProductID for row in df_order_details.select("ProductID").collect()]
        
        orphaned_products = [pid for pid in detail_product_ids if pid not in product_ids]
        
        log_validation(
            f"{day} OrderDetails → Products integrity",
            len(orphaned_products) == 0,
            f"Found {len(orphaned_products)} orphaned product references" if orphaned_products else "All order details reference valid products",
            "ERROR" if orphaned_products else "INFO"
        )
        
    except Exception as e:
        log_validation(
            f"{day} integrity checks",
            False,
            f"Error: {str(e)}",
            "ERROR"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change Summary Report

# COMMAND ----------

print("\n" + "=" * 80)
print("CHANGE SUMMARY REPORT")
print("=" * 80)
print("\nThis report shows what changed between each daily snapshot.")
print("This validates the 'daily full snapshot load & compare' approach.\n")

if change_summary:
    print("Day 1 → Day 2 Changes:")
    print("-" * 60)
    for change in [c for c in change_summary if c["day"] == "Day 1→2"]:
        print(f"  {change['table']:15s} : {change['change_type']:12s} = {change['count']:+4d}  ({change['details']})")
    
    print("\nDay 2 → Day 3 Changes:")
    print("-" * 60)
    for change in [c for c in change_summary if c["day"] == "Day 2→3"]:
        print(f"  {change['table']:15s} : {change['change_type']:12s} = {change['count']:+4d}  ({change['details']})")
else:
    print("No changes were tracked.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("VALIDATION SUMMARY")
print("=" * 80)

# Count results by severity
total_tests = len(validation_results)
passed_tests = sum(1 for r in validation_results if r["passed"])
failed_tests = total_tests - passed_tests

critical_failures = sum(1 for r in validation_results if not r["passed"] and r["severity"] == "CRITICAL")
error_failures = sum(1 for r in validation_results if not r["passed"] and r["severity"] == "ERROR")
warning_failures = sum(1 for r in validation_results if not r["passed"] and r["severity"] == "WARNING")

print(f"\nTotal Tests: {total_tests}")
print(f"Passed: {passed_tests}")
print(f"Failed: {failed_tests}")
print(f"  - Critical: {critical_failures}")
print(f"  - Errors: {error_failures}")
print(f"  - Warnings: {warning_failures}")
print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%")

if failed_tests > 0:
    print("\n" + "=" * 80)
    print("FAILED TESTS:")
    print("=" * 80)
    
    if critical_failures > 0:
        print("\nCRITICAL FAILURES (prevent Bronze ingestion):")
        print("-" * 60)
        for result in validation_results:
            if not result["passed"] and result["severity"] == "CRITICAL":
                print(f"✗ {result['test']}")
                if result["message"]:
                    print(f"  → {result['message']}")
    
    if error_failures > 0:
        print("\nERROR FAILURES (data does not match specification):")
        print("-" * 60)
        for result in validation_results:
            if not result["passed"] and result["severity"] == "ERROR":
                print(f"✗ {result['test']}")
                if result["message"]:
                    print(f"  → {result['message']}")
    
    if warning_failures > 0:
        print("\nWARNINGS (potential issues):")
        print("-" * 60)
        for result in validation_results:
            if not result["passed"] and result["severity"] == "WARNING":
                print(f"✗ {result['test']}")
                if result["message"]:
                    print(f"  → {result['message']}")

print("\n" + "=" * 80)
print("READINESS FOR BRONZE LAYER INGESTION")
print("=" * 80)

# Determine overall readiness
if critical_failures > 0:
    print("\n✗ CRITICAL FAILURES DETECTED")
    print("  Source data is NOT ready for Bronze layer ingestion.")
    print("  Fix critical issues before proceeding.")
elif error_failures > 0:
    print("\n⚠ ERRORS DETECTED")
    print("  Source data has issues but may be usable for Bronze layer ingestion.")
    print("  Review errors and determine if they impact your use case.")
else:
    print("\n✓ ALL VALIDATION TESTS PASSED")
    print("  Source data meets specification for daily full snapshot ingestion.")
    print("  Data is READY for Bronze layer ingestion demo!")

print("\n" + "=" * 80)
print("VALIDATION COMPLETE")
print("=" * 80)
