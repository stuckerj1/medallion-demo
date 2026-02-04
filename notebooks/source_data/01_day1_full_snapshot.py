# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1: Full Northwind Snapshot Generation
# MAGIC 
# MAGIC This notebook creates the initial Day 1 snapshot of the Northwind database.
# MAGIC 
# MAGIC ## Overview
# MAGIC - Loads the complete Northwind schema
# MAGIC - Creates all core tables: Customers, Products, Orders, OrderDetails, Employees
# MAGIC - Represents the initial state of the operational database
# MAGIC 
# MAGIC ## Tables Created
# MAGIC - Customers
# MAGIC - Products  
# MAGIC - Orders
# MAGIC - OrderDetails
# MAGIC - Employees
# MAGIC - Categories
# MAGIC - Suppliers

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Set snapshot date
SNAPSHOT_DATE = "2024-01-01"
BASE_PATH = "Tables/source_data/day1"

print(f"Snapshot Date: {SNAPSHOT_DATE}")
print(f"Base Path: {BASE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Categories Table

# COMMAND ----------

categories_schema = StructType([
    StructField("CategoryID", IntegerType(), False),
    StructField("CategoryName", StringType(), False),
    StructField("Description", StringType(), True)
])

categories_data = [
    (1, "Beverages", "Soft drinks, coffees, teas, beers, and ales"),
    (2, "Condiments", "Sweet and savory sauces, relishes, spreads, and seasonings"),
    (3, "Confections", "Desserts, candies, and sweet breads"),
    (4, "Dairy Products", "Cheeses"),
    (5, "Grains/Cereals", "Breads, crackers, pasta, and cereal"),
    (6, "Meat/Poultry", "Prepared meats"),
    (7, "Produce", "Dried fruit and bean curd"),
    (8, "Seafood", "Seaweed and fish")
]

df_categories = spark.createDataFrame(categories_data, categories_schema)
df_categories.write.mode("overwrite").format("delta").saveAsTable(f"{BASE_PATH}_categories")
print(f"Created Categories table with {df_categories.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Suppliers Table

# COMMAND ----------

suppliers_schema = StructType([
    StructField("SupplierID", IntegerType(), False),
    StructField("CompanyName", StringType(), False),
    StructField("ContactName", StringType(), True),
    StructField("ContactTitle", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("PostalCode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Phone", StringType(), True)
])

suppliers_data = [
    (1, "Exotic Liquids", "Charlotte Cooper", "Purchasing Manager", "49 Gilbert St.", "London", None, "EC1 4SD", "UK", "(171) 555-2222"),
    (2, "New Orleans Cajun Delights", "Shelley Burke", "Order Administrator", "P.O. Box 78934", "New Orleans", "LA", "70117", "USA", "(100) 555-4822"),
    (3, "Grandma Kelly's Homestead", "Regina Murphy", "Sales Representative", "707 Oxford Rd.", "Ann Arbor", "MI", "48104", "USA", "(313) 555-5735"),
    (4, "Tokyo Traders", "Yoshi Nagase", "Marketing Manager", "9-8 Sekimai Musashino-shi", "Tokyo", None, "100", "Japan", "(03) 3555-5011"),
    (5, "Cooperativa de Quesos 'Las Cabras'", "Antonio del Valle Saavedra", "Export Administrator", "Calle del Rosal 4", "Oviedo", "Asturias", "33007", "Spain", "(98) 598 76 54")
]

df_suppliers = spark.createDataFrame(suppliers_data, suppliers_schema)
df_suppliers.write.mode("overwrite").format("delta").saveAsTable(f"{BASE_PATH}_suppliers")
print(f"Created Suppliers table with {df_suppliers.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Products Table

# COMMAND ----------

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

products_data = [
    (1, "Chai", 1, 1, "10 boxes x 20 bags", 18.00, 39, 0, 10, False),
    (2, "Chang", 1, 1, "24 - 12 oz bottles", 19.00, 17, 40, 25, False),
    (3, "Aniseed Syrup", 1, 2, "12 - 550 ml bottles", 10.00, 13, 70, 25, False),
    (4, "Chef Anton's Cajun Seasoning", 2, 2, "48 - 6 oz jars", 22.00, 53, 0, 0, False),
    (5, "Chef Anton's Gumbo Mix", 2, 2, "36 boxes", 21.35, 0, 0, 0, True),
    (6, "Grandma's Boysenberry Spread", 3, 2, "12 - 8 oz jars", 25.00, 120, 0, 25, False),
    (7, "Uncle Bob's Organic Dried Pears", 3, 7, "12 - 1 lb pkgs.", 30.00, 15, 0, 10, False),
    (8, "Northwoods Cranberry Sauce", 3, 2, "12 - 12 oz jars", 40.00, 6, 0, 0, False),
    (9, "Mishi Kobe Niku", 4, 6, "18 - 500 g pkgs.", 97.00, 29, 0, 0, True),
    (10, "Ikura", 4, 8, "12 - 200 ml jars", 31.00, 31, 0, 0, False),
    (11, "Queso Cabrales", 5, 4, "1 kg pkg.", 21.00, 22, 30, 30, False),
    (12, "Queso Manchego La Pastora", 5, 4, "10 - 500 g pkgs.", 38.00, 86, 0, 0, False),
    (13, "Konbu", 4, 8, "2 kg box", 6.00, 24, 0, 5, False),
    (14, "Tofu", 4, 7, "40 - 100 g pkgs.", 23.25, 35, 0, 0, False),
    (15, "Genen Shouyu", 4, 2, "24 - 250 ml bottles", 15.50, 39, 0, 5, False),
    (16, "Pavlova", 3, 3, "32 - 500 g boxes", 17.45, 29, 0, 10, False),
    (17, "Alice Mutton", 3, 6, "20 - 1 kg tins", 39.00, 0, 0, 0, True),
    (18, "Carnarvon Tigers", 3, 8, "16 kg pkg.", 62.50, 42, 0, 0, False),
    (19, "Teatime Chocolate Biscuits", 3, 3, "10 boxes x 12 pieces", 9.20, 25, 0, 5, False),
    (20, "Sir Rodney's Marmalade", 3, 3, "30 gift boxes", 81.00, 40, 0, 0, False)
]

df_products = spark.createDataFrame(products_data, products_schema)
df_products.write.mode("overwrite").format("delta").saveAsTable(f"{BASE_PATH}_products")
print(f"Created Products table with {df_products.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Customers Table

# COMMAND ----------

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

customers_data = [
    ("ALFKI", "Alfreds Futterkiste", "Maria Anders", "Sales Representative", "Obere Str. 57", "Berlin", None, "12209", "Germany", "030-0074321", "030-0076545"),
    ("ANATR", "Ana Trujillo Emparedados y helados", "Ana Trujillo", "Owner", "Avda. de la Constitución 2222", "México D.F.", None, "05021", "Mexico", "(5) 555-4729", "(5) 555-3745"),
    ("ANTON", "Antonio Moreno Taquería", "Antonio Moreno", "Owner", "Mataderos 2312", "México D.F.", None, "05023", "Mexico", "(5) 555-3932", None),
    ("AROUT", "Around the Horn", "Thomas Hardy", "Sales Representative", "120 Hanover Sq.", "London", None, "WA1 1DP", "UK", "(171) 555-7788", "(171) 555-6750"),
    ("BERGS", "Berglunds snabbköp", "Christina Berglund", "Order Administrator", "Berguvsvägen 8", "Luleå", None, "S-958 22", "Sweden", "0921-12 34 65", "0921-12 34 67"),
    ("BLAUS", "Blauer See Delikatessen", "Hanna Moos", "Sales Representative", "Forsterstr. 57", "Mannheim", None, "68306", "Germany", "0621-08460", "0621-08924"),
    ("BLONP", "Blondesddsl père et fils", "Frédérique Citeaux", "Marketing Manager", "24, place Kléber", "Strasbourg", None, "67000", "France", "88.60.15.31", "88.60.15.32"),
    ("BOLID", "Bólido Comidas preparadas", "Martín Sommer", "Owner", "C/ Araquil, 67", "Madrid", None, "28023", "Spain", "(91) 555 22 82", "(91) 555 91 99"),
    ("BONAP", "Bon app'", "Laurence Lebihan", "Owner", "12, rue des Bouchers", "Marseille", None, "13008", "France", "91.24.45.40", "91.24.45.41"),
    ("BOTTM", "Bottom-Dollar Markets", "Elizabeth Lincoln", "Accounting Manager", "23 Tsawassen Blvd.", "Tsawassen", "BC", "T2F 8M4", "Canada", "(604) 555-4729", "(604) 555-3745"),
    ("BSBEV", "B's Beverages", "Victoria Ashworth", "Sales Representative", "Fauntleroy Circus", "London", None, "EC2 5NT", "UK", "(171) 555-1212", None),
    ("CACTU", "Cactus Comidas para llevar", "Patricio Simpson", "Sales Agent", "Cerrito 333", "Buenos Aires", None, "1010", "Argentina", "(1) 135-5555", "(1) 135-4892"),
    ("CENTC", "Centro comercial Moctezuma", "Francisco Chang", "Marketing Manager", "Sierras de Granada 9993", "México D.F.", None, "05022", "Mexico", "(5) 555-3392", "(5) 555-7293"),
    ("CHOPS", "Chop-suey Chinese", "Yang Wang", "Owner", "Hauptstr. 29", "Bern", None, "3012", "Switzerland", "0452-076545", None),
    ("COMMI", "Comércio Mineiro", "Pedro Afonso", "Sales Associate", "Av. dos Lusíadas, 23", "São Paulo", "SP", "05432-043", "Brazil", "(11) 555-7647", None),
    ("CONSH", "Consolidated Holdings", "Elizabeth Brown", "Sales Representative", "Berkeley Gardens 12 Brewery", "London", None, "WX1 6LT", "UK", "(171) 555-2282", "(171) 555-9199"),
    ("DRACD", "Drachenblut Delikatessen", "Sven Ottlieb", "Order Administrator", "Walserweg 21", "Aachen", None, "52066", "Germany", "0241-039123", "0241-059428"),
    ("DUMON", "Du monde entier", "Janine Labrune", "Owner", "67, rue des Cinquante Otages", "Nantes", None, "44000", "France", "40.67.88.88", "40.67.89.89"),
    ("EASTC", "Eastern Connection", "Ann Devon", "Sales Agent", "35 King George", "London", None, "WX3 6FW", "UK", "(171) 555-0297", "(171) 555-3373"),
    ("ERNSH", "Ernst Handel", "Roland Mendel", "Sales Manager", "Kirchgasse 6", "Graz", None, "8010", "Austria", "7675-3425", "7675-3426")
]

df_customers = spark.createDataFrame(customers_data, customers_schema)
df_customers.write.mode("overwrite").format("delta").saveAsTable(f"{BASE_PATH}_customers")
print(f"Created Customers table with {df_customers.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Employees Table

# COMMAND ----------

employees_schema = StructType([
    StructField("EmployeeID", IntegerType(), False),
    StructField("LastName", StringType(), False),
    StructField("FirstName", StringType(), False),
    StructField("Title", StringType(), True),
    StructField("TitleOfCourtesy", StringType(), True),
    StructField("BirthDate", DateType(), True),
    StructField("HireDate", DateType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("PostalCode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("HomePhone", StringType(), True),
    StructField("Extension", StringType(), True),
    StructField("ReportsTo", IntegerType(), True)
])

employees_data = [
    (1, "Davolio", "Nancy", "Sales Representative", "Ms.", datetime(1968, 12, 8).date(), datetime(2022, 5, 1).date(), "507 - 20th Ave. E. Apt. 2A", "Seattle", "WA", "98122", "USA", "(206) 555-9857", "5467", 2),
    (2, "Fuller", "Andrew", "Vice President, Sales", "Dr.", datetime(1962, 2, 19).date(), datetime(2022, 8, 14).date(), "908 W. Capital Way", "Tacoma", "WA", "98401", "USA", "(206) 555-9482", "3457", None),
    (3, "Leverling", "Janet", "Sales Representative", "Ms.", datetime(1973, 8, 30).date(), datetime(2022, 4, 1).date(), "722 Moss Bay Blvd.", "Kirkland", "WA", "98033", "USA", "(206) 555-3412", "3355", 2),
    (4, "Peacock", "Margaret", "Sales Representative", "Mrs.", datetime(1958, 9, 19).date(), datetime(2023, 5, 3).date(), "4110 Old Redmond Rd.", "Redmond", "WA", "98052", "USA", "(206) 555-8122", "5176", 2),
    (5, "Buchanan", "Steven", "Sales Manager", "Mr.", datetime(1975, 3, 4).date(), datetime(2023, 10, 17).date(), "14 Garrett Hill", "London", None, "SW1 8JR", "UK", "(71) 555-4848", "3453", 2),
    (6, "Suyama", "Michael", "Sales Representative", "Mr.", datetime(1981, 7, 2).date(), datetime(2023, 10, 17).date(), "Coventry House Miner Rd.", "London", None, "EC2 7JR", "UK", "(71) 555-7773", "428", 5),
    (7, "King", "Robert", "Sales Representative", "Mr.", datetime(1980, 5, 29).date(), datetime(2024, 1, 2).date(), "Edgeham Hollow Winchester Way", "London", None, "RG1 9SP", "UK", "(71) 555-5598", "465", 5),
    (8, "Callahan", "Laura", "Inside Sales Coordinator", "Ms.", datetime(1978, 1, 9).date(), datetime(2024, 3, 5).date(), "4726 - 11th Ave. N.E.", "Seattle", "WA", "98105", "USA", "(206) 555-1189", "2344", 2),
    (9, "Dodsworth", "Anne", "Sales Representative", "Ms.", datetime(1988, 1, 27).date(), datetime(2024, 11, 15).date(), "7 Houndstooth Rd.", "London", None, "WG2 7LT", "UK", "(71) 555-4444", "452", 5)
]

df_employees = spark.createDataFrame(employees_data, employees_schema)
df_employees.write.mode("overwrite").format("delta").saveAsTable(f"{BASE_PATH}_employees")
print(f"Created Employees table with {df_employees.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Orders Table

# COMMAND ----------

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

# Generate 50 orders for Day 1
base_date = datetime(2023, 12, 1).date()
orders_data = []
for i in range(1, 51):
    order_date = base_date + timedelta(days=(i-1) % 30)
    required_date = order_date + timedelta(days=7)
    shipped_date = order_date + timedelta(days=random.randint(1, 5)) if i % 10 != 0 else None
    
    customer_ids = ["ALFKI", "ANATR", "ANTON", "AROUT", "BERGS", "BLAUS", "BLONP", "BOLID", "BONAP", "BOTTM"]
    customer_id = customer_ids[i % len(customer_ids)]
    
    orders_data.append((
        10248 + i - 1,
        customer_id,
        (i % 9) + 1,
        order_date,
        required_date,
        shipped_date,
        (i % 3) + 1,
        round(random.uniform(10.0, 500.0), 2),
        f"Ship to {customer_id}",
        f"{i} Main Street",
        ["Berlin", "London", "Paris", "Madrid", "Rome"][i % 5],
        None,
        f"{10000 + i}",
        ["Germany", "UK", "France", "Spain", "Italy"][i % 5]
    ))

df_orders = spark.createDataFrame(orders_data, orders_schema)
df_orders.write.mode("overwrite").format("delta").saveAsTable(f"{BASE_PATH}_orders")
print(f"Created Orders table with {df_orders.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. OrderDetails Table

# COMMAND ----------

order_details_schema = StructType([
    StructField("OrderID", IntegerType(), False),
    StructField("ProductID", IntegerType(), False),
    StructField("UnitPrice", DecimalType(10, 2), False),
    StructField("Quantity", IntegerType(), False),
    StructField("Discount", DecimalType(4, 2), False)
])

# Generate 2-5 order details for each order
order_details_data = []
for order_id in range(10248, 10248 + 50):
    num_items = random.randint(2, 5)
    products = random.sample(range(1, 21), num_items)
    
    for product_id in products:
        order_details_data.append((
            order_id,
            product_id,
            round(random.uniform(10.0, 100.0), 2),
            random.randint(1, 50),
            round(random.choice([0.0, 0.05, 0.10, 0.15, 0.20]), 2)
        ))

df_order_details = spark.createDataFrame(order_details_data, order_details_schema)
df_order_details.write.mode("overwrite").format("delta").saveAsTable(f"{BASE_PATH}_order_details")
print(f"Created OrderDetails table with {df_order_details.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("DAY 1 SNAPSHOT CREATION COMPLETE")
print("=" * 80)
print(f"Snapshot Date: {SNAPSHOT_DATE}")
print(f"\nTables Created:")
print(f"  - Categories: {df_categories.count()} records")
print(f"  - Suppliers: {df_suppliers.count()} records")
print(f"  - Products: {df_products.count()} records")
print(f"  - Customers: {df_customers.count()} records")
print(f"  - Employees: {df_employees.count()} records")
print(f"  - Orders: {df_orders.count()} records")
print(f"  - OrderDetails: {df_order_details.count()} records")
print("=" * 80)
