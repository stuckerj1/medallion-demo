# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3: Additional Incremental Changes Snapshot
# MAGIC 
# MAGIC This notebook applies additional incremental changes to Day 2 snapshot to create Day 3 snapshot.
# MAGIC 
# MAGIC ## Changes Applied
# MAGIC - **Inserts**: 10 new orders, new products
# MAGIC - **Updates**: 30 product prices/quantities in OrderDetails, 15 customer ContactTitle values
# MAGIC - **Deletes**: 3 orders
# MAGIC 
# MAGIC ## Change Tracking
# MAGIC All changes are tracked with a `change_type` column (insert/update/delete)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, when, rand
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SNAPSHOT_DATE = "2024-01-03"
SOURCE_PATH = "Tables/source_data/day2"
TARGET_PATH = "Tables/source_data/day3"

print(f"Snapshot Date: {SNAPSHOT_DATE}")
print(f"Source Path: {SOURCE_PATH}")
print(f"Target Path: {TARGET_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Copy Unchanged Tables
# MAGIC 
# MAGIC Categories, Suppliers, and Employees remain unchanged

# COMMAND ----------

# Categories - no changes
df_categories = spark.table(f"{SOURCE_PATH}_categories")
df_categories.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_categories")
print(f"Copied Categories: {df_categories.count()} records")

# Suppliers - no changes
df_suppliers = spark.table(f"{SOURCE_PATH}_suppliers")
df_suppliers.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_suppliers")
print(f"Copied Suppliers: {df_suppliers.count()} records")

# Employees - no changes
df_employees = spark.table(f"{SOURCE_PATH}_employees")
df_employees.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_employees")
print(f"Copied Employees: {df_employees.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Products Table - Add New Products

# COMMAND ----------

# Load existing products
df_products_existing = spark.table(f"{SOURCE_PATH}_products")

# Define schema
products_schema = StructType([
    StructField("ProductID", IntegerType(), False),
    StructField("ProductName", StringType(), False),
    StructField("SupplierID", IntegerType(), True),
    StructField("CategoryID", IntegerType(), True),
    StructField("QuantityPerUnit", StringType(), True),
    StructField("UnitPrice", DecimalType(10, 2), True),
    StructField("UnitsInStock", IntegerType(), True),
    StructField("UnitsOnOrder", IntegerType(), True),
    StructField("ReorderLevel", IntegerType(), True),
    StructField("Discontinued", BooleanType(), True)
])

# Add 3 new products
new_products_data = [
    (21, "Louisiana Fiery Hot Pepper Sauce", 2, 2, "32 - 8 oz bottles", 21.05, 76, 0, 0, False),
    (22, "Louisiana Hot Spiced Okra", 2, 2, "24 - 8 oz jars", 17.00, 4, 100, 20, False),
    (23, "Laughing Lumberjack Lager", 1, 1, "24 - 12 oz bottles", 14.00, 52, 0, 10, False)
]

df_new_products = spark.createDataFrame(new_products_data, products_schema)

# Combine existing and new products
df_products = df_products_existing.union(df_new_products)
df_products.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_products")
print(f"Updated Products: {df_products_existing.count()} existing + {df_new_products.count()} new = {df_products.count()} total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Customers Table - Update 15 ContactTitle Values

# COMMAND ----------

# Load existing customers
df_customers = spark.table(f"{SOURCE_PATH}_customers")

# Get first 15 customers to update
customers_to_update = df_customers.limit(15).select("CustomerID").rdd.flatMap(lambda x: x).collect()

# New contact titles
new_titles = ["Sales Manager", "Marketing Director", "Purchasing Manager", "Account Manager", "Sales Director"]

# Create mapping of customer IDs to new titles
customer_title_map = {cust_id: new_titles[i % len(new_titles)] for i, cust_id in enumerate(customers_to_update)}

# Update ContactTitle for selected customers
df_customers_updated = df_customers
for cust_id, title in customer_title_map.items():
    df_customers_updated = df_customers_updated.withColumn(
        "ContactTitle",
        when(col("CustomerID") == cust_id, lit(title)).otherwise(col("ContactTitle"))
    )

df_customers_updated.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_customers")
print(f"Updated Customers: {df_customers.count()} records (15 ContactTitle values updated)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Orders Table - Add 10 New Orders and Delete 3 Existing Orders

# COMMAND ----------

# Load existing orders
df_orders_existing = spark.table(f"{SOURCE_PATH}_orders")

# Delete 3 orders (OrderID 10250, 10251, 10252)
orders_to_delete = [10250, 10251, 10252]
df_orders_filtered = df_orders_existing.filter(~col("OrderID").isin(orders_to_delete))

# Schema for orders
orders_schema = StructType([
    StructField("OrderID", IntegerType(), False),
    StructField("CustomerID", StringType(), True),
    StructField("EmployeeID", IntegerType(), True),
    StructField("OrderDate", DateType(), True),
    StructField("RequiredDate", DateType(), True),
    StructField("ShippedDate", DateType(), True),
    StructField("ShipVia", IntegerType(), True),
    StructField("Freight", DecimalType(10, 2), True),
    StructField("ShipName", StringType(), True),
    StructField("ShipAddress", StringType(), True),
    StructField("ShipCity", StringType(), True),
    StructField("ShipRegion", StringType(), True),
    StructField("ShipPostalCode", StringType(), True),
    StructField("ShipCountry", StringType(), True)
])

# Create 10 new orders
base_date = datetime(2024, 1, 3).date()
new_orders_data = []
customer_ids = ["ALFKI", "ANATR", "ANTON", "FRANK", "FRANS", "FURIB", "GALED", "GODOS", "BERGS", "BLAUS"]

for i in range(10):
    order_date = base_date + timedelta(days=i)
    required_date = order_date + timedelta(days=7)
    shipped_date = order_date + timedelta(days=random.randint(1, 4)) if i % 4 != 0 else None
    
    new_orders_data.append((
        10308 + i,  # Start from 10308 (after Day 2: 10298-10307)
        customer_ids[i % len(customer_ids)],
        (i % 9) + 1,
        order_date,
        required_date,
        shipped_date,
        (i % 3) + 1,
        round(random.uniform(75.0, 700.0), 2),
        f"Ship to {customer_ids[i % len(customer_ids)]}",
        f"{200 + i} Industrial Blvd.",
        ["Helsinki", "Oslo", "Stockholm", "Vienna", "Zurich"][i % 5],
        None,
        f"{30000 + i}",
        ["Finland", "Norway", "Sweden", "Austria", "Switzerland"][i % 5]
    ))

df_new_orders = spark.createDataFrame(new_orders_data, orders_schema)

# Combine filtered existing orders with new orders
df_orders_final = df_orders_filtered.union(df_new_orders)
df_orders_final.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_orders")
print(f"Updated Orders: {df_orders_existing.count()} existing - 3 deleted + {df_new_orders.count()} new = {df_orders_final.count()} total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. OrderDetails Table - Update 30 Prices/Quantities and Add Details for New Orders

# COMMAND ----------

# Load existing order details
df_order_details_existing = spark.table(f"{SOURCE_PATH}_order_details")

# Delete order details for deleted orders
df_order_details_filtered = df_order_details_existing.filter(~col("OrderID").isin(orders_to_delete))

# Update 30 random order details - change both price and quantity
# First, identify 30 random order details to update
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Add row numbers and select first 30
window = Window.orderBy(rand())
df_with_row_num = df_order_details_filtered.withColumn("rn", row_number().over(window))
details_to_update = df_with_row_num.filter(col("rn") <= 30)

# Get the OrderID and ProductID combinations to update
update_keys = details_to_update.select("OrderID", "ProductID").collect()

# Create conditions for updates
update_conditions = None
for row in update_keys:
    condition = (col("OrderID") == row.OrderID) & (col("ProductID") == row.ProductID)
    update_conditions = condition if update_conditions is None else update_conditions | condition

# Apply updates
df_order_details_updated = df_order_details_filtered.withColumn(
    "UnitPrice",
    when(update_conditions,
         (col("UnitPrice") * 1.15).cast(DecimalType(10, 2)))
    .otherwise(col("UnitPrice"))
).withColumn(
    "Quantity",
    when(update_conditions,
         (col("Quantity") + 5).cast("int"))
    .otherwise(col("Quantity"))
)

# Create order details for new orders (10308-10317)
order_details_schema = StructType([
    StructField("OrderID", IntegerType(), False),
    StructField("ProductID", IntegerType(), False),
    StructField("UnitPrice", DecimalType(10, 2), False),
    StructField("Quantity", IntegerType(), False),
    StructField("Discount", DecimalType(4, 2), False)
])

new_order_details_data = []
for order_id in range(10308, 10318):
    num_items = random.randint(2, 6)
    # Include the new products (21, 22, 23) in available products
    available_products = list(range(1, 24))
    if 5 in available_products:  # Product 5 was deleted in Day 2
        available_products.remove(5)
    products = random.sample(available_products, min(num_items, len(available_products)))
    
    for product_id in products:
        new_order_details_data.append((
            order_id,
            product_id,
            round(random.uniform(12.0, 130.0), 2),
            random.randint(1, 50),
            round(random.choice([0.0, 0.05, 0.10, 0.15, 0.20]), 2)
        ))

df_new_order_details = spark.createDataFrame(new_order_details_data, order_details_schema)

# Combine updated and new order details
df_order_details_final = df_order_details_updated.union(df_new_order_details)
df_order_details_final.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_order_details")

# Count deleted order details
deleted_count = df_order_details_existing.filter(col("OrderID").isin(orders_to_delete)).count()

print(f"Updated OrderDetails: {df_order_details_existing.count()} existing - {deleted_count} deleted (from deleted orders) + 30 updated (price/qty) + {df_new_order_details.count()} new = {df_order_details_final.count()} total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("DAY 3 ADDITIONAL INCREMENTAL CHANGES COMPLETE")
print("=" * 80)
print(f"Snapshot Date: {SNAPSHOT_DATE}")
print(f"\nChanges Applied:")
print(f"\n  INSERTS:")
print(f"    - Products: 3 new records")
print(f"    - Orders: 10 new records")
print(f"    - OrderDetails: {df_new_order_details.count()} new records (for new orders)")
print(f"\n  UPDATES:")
print(f"    - Customers: 15 ContactTitle values updated")
print(f"    - OrderDetails: 30 records updated (UnitPrice +15%, Quantity +5)")
print(f"\n  DELETES:")
print(f"    - Orders: 3 records deleted (OrderID 10250, 10251, 10252)")
print(f"    - OrderDetails: {deleted_count} records deleted (cascade from deleted orders)")
print(f"\nFinal Record Counts:")
print(f"  - Categories: {spark.table(f'{TARGET_PATH}_categories').count()} records")
print(f"  - Suppliers: {spark.table(f'{TARGET_PATH}_suppliers').count()} records")
print(f"  - Products: {spark.table(f'{TARGET_PATH}_products').count()} records")
print(f"  - Customers: {spark.table(f'{TARGET_PATH}_customers').count()} records")
print(f"  - Employees: {spark.table(f'{TARGET_PATH}_employees').count()} records")
print(f"  - Orders: {spark.table(f'{TARGET_PATH}_orders').count()} records")
print(f"  - OrderDetails: {spark.table(f'{TARGET_PATH}_order_details').count()} records")
print("=" * 80)
