#### Context
This update adds retroactive documentation for the code implemented in the merged branch `copilot/build-medallion-architecture-model`. Specifically, we detail the architecture design and highlight high-level implementation steps including:
- Northwind database setup.
- Notebook usage for ingestion and validation workflows.
- Integration with the Medallion architecture layers (Bronze, Silver).

---

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

### High-Level Design Overview:

#### Medallion Architecture Layers
The project follows the Medallion Architecture pattern for structured data processing:

1. **Bronze Layer**:
   - Raw ingested data from the Northwind database.
   - Full snapshots of data for three consecutive days.
   - Retains record of all inserts, updates, and deletes.
2. **Silver Layer**:
   - Applies cleaning and transformation on the Bronze layer data.
   - Adds business logic and additional metadata required for analytics.
3. **Gold Layer**:
   - Represents the final curated dataset cleaned in the Silver layer.
   - Used for visualization and reporting.

---