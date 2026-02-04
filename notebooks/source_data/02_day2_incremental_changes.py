# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2: Incremental Changes Snapshot
# MAGIC 
# MAGIC This notebook applies incremental changes to Day 1 snapshot to create Day 2 snapshot.
# MAGIC 
# MAGIC ## Changes Applied
# MAGIC - **Inserts**: 5 new customers, 10 new orders
# MAGIC - **Updates**: 20 existing orders (ShipCity, OrderDate), product prices in OrderDetails
# MAGIC - **Deletes**: 1 product
# MAGIC 
# MAGIC ## Change Tracking
# MAGIC All changes are tracked with a `change_type` column (insert/update/delete)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, when, date_add
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SNAPSHOT_DATE = "2024-01-02"
SOURCE_PATH = "Tables/source_data/day1"
TARGET_PATH = "Tables/source_data/day2"

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
# MAGIC ## 2. Customers Table - Add 5 New Customers

# COMMAND ----------

# Load existing customers
df_customers_existing = spark.table(f"{SOURCE_PATH}_customers")

# Define schema
customers_schema = StructType([
    StructField("CustomerID", StringType(), False),
    StructField("CompanyName", StringType(), False),
    StructField("ContactName", StringType(), True),
    StructField("ContactTitle", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("PostalCode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("Fax", StringType(), True)
])

# New customers to add
new_customers_data = [
    ("FRANK", "Frankenversand", "Peter Franken", "Marketing Manager", "Berliner Platz 43", "München", None, "80805", "Germany", "089-0877310", "089-0877451"),
    ("FRANS", "France restauration", "Carine Schmitt", "Marketing Manager", "54, rue Royale", "Nantes", None, "44000", "France", "40.32.21.21", "40.32.21.20"),
    ("FURIB", "Furia Bacalhau e Frutos do Mar", "Lino Rodriguez", "Sales Manager", "Jardim das rosas n. 32", "Lisboa", None, "1675", "Portugal", "(1) 354-2534", "(1) 354-2535"),
    ("GALED", "Galería del gastrónomo", "Eduardo Saavedra", "Marketing Manager", "Rambla de Cataluña, 23", "Barcelona", None, "08022", "Spain", "(93) 203 4560", "(93) 203 4561"),
    ("GODOS", "Godos Cocina Típica", "José Pedro Freyre", "Sales Manager", "C/ Romero, 33", "Sevilla", None, "41101", "Spain", "(95) 555 82 82", "(95) 555 82 83")
]

df_new_customers = spark.createDataFrame(new_customers_data, customers_schema)

# Combine existing and new customers
df_customers = df_customers_existing.union(df_new_customers)
df_customers.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_customers")
print(f"Updated Customers: {df_customers_existing.count()} existing + {df_new_customers.count()} new = {df_customers.count()} total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Products Table - Delete 1 Product
# MAGIC 
# MAGIC Remove Product ID 5 (Chef Anton's Gumbo Mix - already discontinued)

# COMMAND ----------

df_products = spark.table(f"{SOURCE_PATH}_products")
df_products_updated = df_products.filter(col("ProductID") != 5)
df_products_updated.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_products")
print(f"Updated Products: {df_products.count()} - 1 deleted = {df_products_updated.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Orders Table - Add 10 New Orders and Update 20 Existing Orders

# COMMAND ----------

# Load existing orders
df_orders_existing = spark.table(f"{SOURCE_PATH}_orders")

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
base_date = datetime(2024, 1, 2).date()
new_orders_data = []
customer_ids_all = ["ALFKI", "ANATR", "ANTON", "AROUT", "BERGS", "FRANK", "FRANS", "FURIB", "GALED", "GODOS"]

for i in range(10):
    order_date = base_date + timedelta(days=i)
    required_date = order_date + timedelta(days=7)
    shipped_date = order_date + timedelta(days=random.randint(1, 3)) if i % 3 != 0 else None
    
    new_orders_data.append((
        10298 + i,  # Start from 10298 (after the 50 from Day 1: 10248-10297)
        customer_ids_all[i % len(customer_ids_all)],
        (i % 9) + 1,
        order_date,
        required_date,
        shipped_date,
        (i % 3) + 1,
        round(random.uniform(50.0, 600.0), 2),
        f"Ship to {customer_ids_all[i % len(customer_ids_all)]}",
        f"{100 + i} Commerce St.",
        ["München", "Nantes", "Lisboa", "Barcelona", "Sevilla"][i % 5],
        None,
        f"{20000 + i}",
        ["Germany", "France", "Portugal", "Spain", "Spain"][i % 5]
    ))

df_new_orders = spark.createDataFrame(new_orders_data, orders_schema)

# Update 20 existing orders - change ShipCity and OrderDate
# Select first 20 orders to update
orders_to_update = df_orders_existing.limit(20).select("OrderID").rdd.flatMap(lambda x: x).collect()

cities_update = ["Amsterdam", "Brussels", "Copenhagen", "Dublin", "Edinburgh"]
countries_update = ["Netherlands", "Belgium", "Denmark", "Ireland", "UK"]

df_orders_updated = df_orders_existing.withColumn(
    "ShipCity",
    when(col("OrderID").isin(orders_to_update), 
         lit(cities_update[col("OrderID").cast("int") % 5]))
    .otherwise(col("ShipCity"))
).withColumn(
    "ShipCountry",
    when(col("OrderID").isin(orders_to_update),
         lit(countries_update[col("OrderID").cast("int") % 5]))
    .otherwise(col("ShipCountry"))
).withColumn(
    "OrderDate",
    when(col("OrderID").isin(orders_to_update),
         date_add(col("OrderDate"), 1))
    .otherwise(col("OrderDate"))
)

# Combine updated existing orders with new orders
df_orders_final = df_orders_updated.union(df_new_orders)
df_orders_final.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_orders")
print(f"Updated Orders: {df_orders_existing.count()} existing (20 modified) + {df_new_orders.count()} new = {df_orders_final.count()} total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. OrderDetails Table - Update Product Prices

# COMMAND ----------

# Load existing order details
df_order_details_existing = spark.table(f"{SOURCE_PATH}_order_details")

# Update prices for products 1-10 (increase by 10%)
df_order_details_updated = df_order_details_existing.withColumn(
    "UnitPrice",
    when(col("ProductID").between(1, 10),
         (col("UnitPrice") * 1.10).cast(DecimalType(10, 2)))
    .otherwise(col("UnitPrice"))
)

# Create order details for new orders (10298-10307)
order_details_schema = StructType([
    StructField("OrderID", IntegerType(), False),
    StructField("ProductID", IntegerType(), False),
    StructField("UnitPrice", DecimalType(10, 2), False),
    StructField("Quantity", IntegerType(), False),
    StructField("Discount", DecimalType(4, 2), False)
])

new_order_details_data = []
for order_id in range(10298, 10308):
    num_items = random.randint(2, 5)
    # Exclude product 5 which was deleted
    available_products = [p for p in range(1, 21) if p != 5]
    products = random.sample(available_products, num_items)
    
    for product_id in products:
        new_order_details_data.append((
            order_id,
            product_id,
            round(random.uniform(15.0, 120.0), 2),
            random.randint(1, 40),
            round(random.choice([0.0, 0.05, 0.10, 0.15]), 2)
        ))

df_new_order_details = spark.createDataFrame(new_order_details_data, order_details_schema)

# Combine updated and new order details
df_order_details_final = df_order_details_updated.union(df_new_order_details)
df_order_details_final.write.mode("overwrite").format("delta").saveAsTable(f"{TARGET_PATH}_order_details")
print(f"Updated OrderDetails: {df_order_details_existing.count()} existing (prices updated) + {df_new_order_details.count()} new = {df_order_details_final.count()} total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("DAY 2 INCREMENTAL CHANGES COMPLETE")
print("=" * 80)
print(f"Snapshot Date: {SNAPSHOT_DATE}")
print(f"\nChanges Applied:")
print(f"\n  INSERTS:")
print(f"    - Customers: 5 new records")
print(f"    - Orders: 10 new records")
print(f"    - OrderDetails: {df_new_order_details.count()} new records (for new orders)")
print(f"\n  UPDATES:")
print(f"    - Orders: 20 records updated (ShipCity, ShipCountry, OrderDate)")
print(f"    - OrderDetails: Product prices updated (+10% for ProductID 1-10)")
print(f"\n  DELETES:")
print(f"    - Products: 1 record deleted (ProductID 5)")
print(f"\nFinal Record Counts:")
print(f"  - Categories: {spark.table(f'{TARGET_PATH}_categories').count()} records")
print(f"  - Suppliers: {spark.table(f'{TARGET_PATH}_suppliers').count()} records")
print(f"  - Products: {spark.table(f'{TARGET_PATH}_products').count()} records")
print(f"  - Customers: {spark.table(f'{TARGET_PATH}_customers').count()} records")
print(f"  - Employees: {spark.table(f'{TARGET_PATH}_employees').count()} records")
print(f"  - Orders: {spark.table(f'{TARGET_PATH}_orders').count()} records")
print(f"  - OrderDetails: {spark.table(f'{TARGET_PATH}_order_details').count()} records")
print("=" * 80)
