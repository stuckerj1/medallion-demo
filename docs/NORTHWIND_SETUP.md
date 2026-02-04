# Northwind Source Data Initialization

## Overview

This implementation provides a complete source data environment using the Northwind schema for the Medallion architecture demo. It simulates three days of snapshots with incremental changes for ingestion into the Bronze layer.

## Architecture

```
Source Data (Northwind)
  ├── Day 1: Full Snapshot (2024-01-01)
  ├── Day 2: Incremental Changes (2024-01-02)
  └── Day 3: Additional Changes (2024-01-03)
          ↓
Bronze Layer (with change tracking)
  ├── change_type column (insert/update/delete/no_change)
  └── load_date column (snapshot date)
```

## Notebooks

### Source Data Generation

1. **`01_day1_full_snapshot.py`**
   - Creates the initial Day 1 snapshot
   - Loads complete Northwind schema
   - Tables: Customers, Products, Orders, OrderDetails, Employees, Categories, Suppliers
   - All records marked with snapshot date: 2024-01-01

2. **`02_day2_incremental_changes.py`**
   - Applies incremental changes to Day 1
   - **Inserts**: 5 new customers, 10 new orders
   - **Updates**: 20 existing orders (ShipCity, OrderDate), product prices (+10% for products 1-10)
   - **Deletes**: 1 product (ProductID 5)
   - Snapshot date: 2024-01-02

3. **`03_day3_incremental_changes.py`**
   - Applies additional incremental changes to Day 2
   - **Inserts**: 3 new products, 10 new orders
   - **Updates**: 15 customer ContactTitle values, 30 order detail prices/quantities (+15% price, +5 quantity)
   - **Deletes**: 3 orders (OrderID 10250, 10251, 10252)
   - Snapshot date: 2024-01-03

### Bronze Layer Ingestion

4. **`bronze_ingestion_northwind.py`**
   - Ingests all three snapshots into Bronze layer
   - Detects and tracks changes between snapshots
   - Adds `change_type` column (insert/update/delete/no_change)
   - Adds `load_date` column for snapshot tracking
   - Generates ingestion statistics and summaries

## Tables

### Core Tables

| Table | Primary Key | Description |
|-------|-------------|-------------|
| Categories | CategoryID | Product categories |
| Suppliers | SupplierID | Product suppliers |
| Products | ProductID | Products catalog |
| Customers | CustomerID | Customer information |
| Employees | EmployeeID | Employee records |
| Orders | OrderID | Order headers |
| OrderDetails | OrderID, ProductID | Order line items |

## Change Summary

### Day 1 (2024-01-01)
- **Full initial load**: All records are inserts
- Categories: 8 records
- Suppliers: 5 records
- Products: 20 records
- Customers: 20 records
- Employees: 9 records
- Orders: 50 records
- OrderDetails: ~150-200 records (2-5 items per order)

### Day 2 (2024-01-02)
- **Customers**: +5 inserts (FRANK, FRANS, FURIB, GALED, GODOS)
- **Products**: -1 delete (ProductID 5)
- **Orders**: +10 inserts, 20 updates (ShipCity, ShipCountry, OrderDate)
- **OrderDetails**: +~30 inserts, prices updated for ProductID 1-10 (+10%)

### Day 3 (2024-01-03)
- **Products**: +3 inserts (ProductID 21, 22, 23)
- **Customers**: 15 updates (ContactTitle changes)
- **Orders**: +10 inserts, -3 deletes (OrderID 10250, 10251, 10252)
- **OrderDetails**: +~40 inserts, 30 updates (UnitPrice +15%, Quantity +5), ~10 deletes (cascade)

## Bronze Layer Schema

All Bronze tables include the following additional columns:

```sql
-- Original table columns
...
-- Bronze layer metadata
change_type STRING,  -- 'insert', 'update', 'delete', 'no_change'
load_date STRING     -- '2024-01-01', '2024-01-02', '2024-01-03'
```

## Usage

### Running in Microsoft Fabric

1. **Create a Microsoft Fabric Lakehouse**
   - Open your Fabric workspace
   - Create a new Lakehouse

2. **Import Notebooks**
   - Import the notebooks from `/notebooks/source_data/` and `/notebooks/bronze_layer/`
   - Attach them to your Lakehouse

3. **Execute in Order**
   ```
   Step 1: Run 01_day1_full_snapshot.py
   Step 2: Run 02_day2_incremental_changes.py
   Step 3: Run 03_day3_incremental_changes.py
   Step 4: Run bronze_ingestion_northwind.py
   ```

4. **Verify Results**
   - Check the Bronze layer tables
   - Review ingestion statistics
   - Query examples are provided in the bronze ingestion notebook

### Sample Queries

#### View all changes for a specific table
```sql
SELECT load_date, change_type, COUNT(*) as record_count
FROM Tables.bronze_customers
GROUP BY load_date, change_type
ORDER BY load_date, change_type
```

#### Find new customers added in Day 2
```sql
SELECT CustomerID, CompanyName, ContactName, City, Country
FROM Tables.bronze_customers
WHERE load_date = '2024-01-02' AND change_type = 'insert'
```

#### Track product deletions
```sql
SELECT load_date, ProductID, ProductName, change_type
FROM Tables.bronze_products
WHERE change_type = 'delete'
ORDER BY load_date
```

#### View updated orders
```sql
SELECT OrderID, CustomerID, OrderDate, ShipCity, ShipCountry, change_type
FROM Tables.bronze_orders
WHERE load_date = '2024-01-02' AND change_type = 'update'
```

#### Get snapshot of current state (Day 3)
```sql
SELECT *
FROM Tables.bronze_customers
WHERE load_date = '2024-01-03' AND change_type != 'delete'
```

## Change Detection Logic

The Bronze ingestion notebook uses hash-based change detection:

1. **Hash Generation**: Creates MD5 hash of all column values (excluding metadata)
2. **Insert Detection**: Records in current snapshot but not in previous
3. **Update Detection**: Records in both snapshots with different hash values
4. **Delete Detection**: Records in previous snapshot but not in current
5. **No Change Detection**: Records in both snapshots with same hash values

## Data Quality

The implementation includes:
- Primary key integrity (no duplicates within a snapshot)
- Referential integrity (OrderDetails references valid Orders and Products)
- Type safety (proper data types for all columns)
- Change tracking completeness (all changes are tracked)

## Next Steps

After completing the Bronze layer ingestion, you can:

1. **Implement Silver Layer** (Data Vault 2.0)
   - Create Hubs (Customer, Product, Order)
   - Create Links (Customer-Order, Order-Product)
   - Create Satellites (with Type 2 SCD for historical tracking)

2. **Implement Gold Layer**
   - Build conformed dimensions
   - Create fact tables
   - Pre-calculate KPIs

3. **Add Data Quality Checks**
   - Implement quality gates between layers
   - Add validation rules
   - Create monitoring dashboards

## Technical Notes

### Delta Lake Format
All tables use Delta Lake format for:
- ACID transactions
- Time travel capabilities
- Schema evolution
- Efficient upserts

### Performance Considerations
- Partitioning: Consider partitioning by load_date for large datasets
- Indexing: Delta Lake automatically optimizes file layout
- Caching: Use caching for frequently accessed tables

### Incremental Processing
The Bronze ingestion notebook supports incremental processing:
- Processes snapshots sequentially
- Detects changes from previous snapshot
- Appends only changes to Bronze tables
- Maintains full history

## References

- [Northwind Database](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs)
- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Data Vault 2.0](https://datavaultalliance.com/)

## License

This is a demonstration project for educational purposes.
