# Source Data Initialization Specification for Northwind
### Objective
Define the process for ingesting and initializing the Northwind data as source data for building a Medallion architecture pipeline. The goal is to process snapshots of data, focusing on 3-day incremental updates, and structure the data into the Bronze layer.

### Approach

#### **Northwind Database Setup**
1. Utilize the Northwind schema as the foundational dataset.
   - Load the schema and entry data from the provided SQL file.
   - Key tables: `Customers`, `Orders`, `OrderDetails`, and `Products`.
2. Initialize the Northwind database in a SQL warehouse using the Fabric PySpark notebook.
3. Create full snapshots of the data for three consecutive days with incremental changes applied on Days 2 and 3. These snapshots are used to simulate real-world operational database changes:
   - **Day 1**: Load the full Northwind dataset.
   - **Day 2**: Apply changes such as additional customers, modifications to orders, and removal of one product.
   - **Day 3**: Apply further changes such as updating product details, adding orders, and removing orders.

---

#### **Ingestion Workflow**
- Use **Microsoft Fabric notebooks** for ingesting the data into the Bronze layer.
- Each day’s snapshot is ingested and stored as separate tables in the **Bronze layer**.
- An additional metadata layer is added with:
  - `change_type`: To track if the row is an `insert`, `update`, or `delete`.
  - `load_date`: The timestamp of when the data was ingested.

---

