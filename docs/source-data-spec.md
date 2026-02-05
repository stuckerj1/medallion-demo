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


### Error Handling

For comprehensive error handling strategies in source data generation scripts, see:
- [Error Handling in Source Data Generation Scripts](error-handling-source-data.md) - Detailed documentation on capturing, logging, and resolving errors
- [Source Data Error Handling Feature](../features/bronze/source_data_error_handling.feature) - BDD scenarios for error handling

**Key Error Handling Principles:**

1. **No Data Loss**: All source data must be preserved, even when errors occur
2. **Quarantine Pattern**: Problematic data is isolated for investigation rather than discarded
3. **Structured Logging**: All errors are logged with sufficient context for debugging
4. **Continue on Error**: Processing continues for valid data when individual records fail
5. **Alerting**: Critical errors trigger immediate notifications to operations

**Common Error Types:**
- Schema mismatches between source and expected structure
- NULL values in required fields
- Invalid data formats (dates, emails, etc.)
- Referential integrity violations
- Transformation failures
- System/network errors

**Error Resolution:**
- Quarantined data can be corrected and reprocessed
- Error logs track resolution status and notes
- Metrics monitor error rates and resolution times
- Integration with data quality framework for comprehensive monitoring

---
