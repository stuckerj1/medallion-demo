# Source Data Files Structure

## Overview

The source data is organized as **three complete snapshots** (Day 1, Day 2, Day 3) for each table. This follows the "daily full snapshot load & compare" pattern where each day represents a complete state of the source system.

## File/Table Structure

Each table has three separate files/tables:

```
Tables/source_data/
├── day1_categories
├── day1_suppliers
├── day1_products
├── day1_customers
├── day1_employees
├── day1_orders
├── day1_order_details
├── day2_categories
├── day2_suppliers
├── day2_products
├── day2_customers
├── day2_employees
├── day2_orders
├── day2_order_details
├── day3_categories
├── day3_suppliers
├── day3_products
├── day3_customers
├── day3_employees
├── day3_orders
└── day3_order_details
```

**Total: 21 tables** (7 tables × 3 days)

## Data by Day

### Day 1 - Initial Baseline Snapshot

**Purpose**: Complete initial load of all source data

**Tables Created**:
| Table | Records | Description |
|-------|---------|-------------|
| day1_categories | 8 | Product categories (Beverages, Condiments, etc.) |
| day1_suppliers | 5 | Supplier companies |
| day1_products | 20 | Products with prices, stock levels |
| day1_customers | 20 | Customer companies and contacts |
| day1_employees | 9 | Employee records |
| day1_orders | 50 | Customer orders from Dec 2023 |
| day1_order_details | ~150+ | Line items for orders |

**Generation Script**: `notebooks/source_data/01_day1_full_snapshot.py`

**Key Characteristics**:
- All records are new
- Establishes the baseline for future comparisons
- No change tracking needed (everything is an insert from Bronze perspective)

---

### Day 2 - First Incremental Snapshot

**Purpose**: Full snapshot showing first day of changes

**Tables Created**:
| Table | Records | Changes from Day 1 |
|-------|---------|---------------------|
| day2_categories | 8 | No changes (copied from day1) |
| day2_suppliers | 5 | No changes (copied from day1) |
| day2_employees | 9 | No changes (copied from day1) |
| day2_products | 19 | **DELETE**: 1 product removed (ProductID 5) |
| day2_customers | 25 | **INSERT**: 5 new customers added |
| day2_orders | 60 | **INSERT**: 10 new orders<br>**UPDATE**: 20 orders modified (ShipCity, ShipCountry) |
| day2_order_details | ~200+ | **INSERT**: New order details for new orders<br>**UPDATE**: Price increases (10%) for products 1-10 |

**Generation Script**: `notebooks/source_data/02_day2_incremental_changes.py`

**Real-World Changes Simulated**:

1. **Customer Inserts** (5 new customers):
   - FRANK - Frankenversand (Germany)
   - FRANS - France restauration (France)
   - FURIB - Furia Bacalhau e Frutos do Mar (Portugal)
   - GALED - Galería del gastrónomo (Spain)
   - GODOS - Godos Cocina Típica (Spain)

2. **Product Delete**:
   - ProductID 5 "Chef Anton's Gumbo Mix" removed (was already discontinued)
   - Simulates product discontinuation and removal from catalog

3. **Order Inserts** (10 new orders):
   - OrderIDs 10298-10307
   - Placed by existing and new customers
   - Mix of shipped and pending orders

4. **Order Updates** (20 orders modified):
   - First 20 orders have changed ShipCity and ShipCountry
   - Simulates address corrections or shipping changes
   - OrderDate adjusted by +1 day

5. **OrderDetails Updates**:
   - Price increases for products 1-10 (10% increase)
   - Simulates market price adjustments
   - New details added for new orders

---

### Day 3 - Second Incremental Snapshot

**Purpose**: Full snapshot showing continued evolution

**Tables Created**:
| Table | Records | Changes from Day 2 |
|-------|---------|---------------------|
| day3_categories | 8 | No changes (copied from day2) |
| day3_suppliers | 5 | No changes (copied from day2) |
| day3_employees | 9 | No changes (copied from day2) |
| day3_products | 22 | **INSERT**: 3 new products added |
| day3_customers | 25 | **UPDATE**: 15 ContactTitle values changed |
| day3_orders | 67 | **DELETE**: 3 orders removed<br>**INSERT**: 10 new orders |
| day3_order_details | ~250+ | **DELETE**: Details for deleted orders<br>**INSERT**: Details for new orders<br>**UPDATE**: 30 price/quantity changes |

**Generation Script**: `notebooks/source_data/03_day3_incremental_changes.py`

**Real-World Changes Simulated**:

1. **Product Inserts** (3 new products):
   - ProductID 21: "Louisiana Fiery Hot Pepper Sauce"
   - ProductID 22: "Louisiana Hot Spiced Okra"
   - ProductID 23: "Laughing Lumberjack Lager"
   - Simulates new product launches

2. **Customer Updates** (15 records):
   - ContactTitle field changed for first 15 customers
   - Simulates organizational changes (promotions, role changes)
   - Customer count stays at 25, but data within records changes

3. **Order Deletes** (3 orders):
   - OrderIDs 10250, 10251, 10252 removed
   - Simulates order cancellations or data cleanup
   - Associated OrderDetails also removed

4. **Order Inserts** (10 new orders):
   - OrderIDs 10308-10317
   - Placed by existing customers
   - Different shipping destinations (Helsinki, Oslo, Stockholm, etc.)

5. **OrderDetails Updates** (30 records):
   - Random 30 order details get 15% price increase
   - Quantity increased by 5 units
   - Simulates price adjustments and quantity corrections

## Change Summary

### Day 1 → Day 2 Changes

| Table | Inserts | Deletes | Updates | Net Change |
|-------|---------|---------|---------|------------|
| categories | 0 | 0 | 0 | 0 |
| suppliers | 0 | 0 | 0 | 0 |
| employees | 0 | 0 | 0 | 0 |
| products | 0 | 1 | 0 | -1 |
| customers | 5 | 0 | 0 | +5 |
| orders | 10 | 0 | 20 | +10 |
| order_details | ~50 | 0 | ~150 | +50 |

**Total Operations**: ~65 inserts, 1 delete, ~170 updates

### Day 2 → Day 3 Changes

| Table | Inserts | Deletes | Updates | Net Change |
|-------|---------|---------|---------|------------|
| categories | 0 | 0 | 0 | 0 |
| suppliers | 0 | 0 | 0 | 0 |
| employees | 0 | 0 | 0 | 0 |
| products | 3 | 0 | 0 | +3 |
| customers | 0 | 0 | 15 | 0 |
| orders | 10 | 3 | 0 | +7 |
| order_details | ~50 | ~10 | 30 | +40 |

**Total Operations**: ~63 inserts, ~13 deletes, 45 updates

## Real-World Patterns Demonstrated

### 1. **Static Reference Data**
- Categories, Suppliers, Employees rarely change
- Demonstrates tables that are copied forward unchanged

### 2. **Master Data with Slow Changes**
- Products and Customers change occasionally
- Shows inserts and occasional updates
- Demonstrates data quality corrections (updates)

### 3. **Transactional Data**
- Orders and OrderDetails are actively changing
- High volume of inserts
- Some deletes (cancellations)
- Updates for corrections

### 4. **Referential Integrity**
- OrderDetails reference valid Orders and Products
- Orders reference valid Customers and Employees
- Deleted entities properly cascade (Product 5 not referenced after deletion)

### 5. **Business Realism**
- Price increases happen in batches
- New products are added periodically
- Orders can be cancelled
- Customer information gets updated (job titles change)
- Shipping information can be corrected

## Validation

To validate this structure, run:

```
notebooks/validation/validate_source_data_snapshots.py
```

This validation script will:
1. Verify all 21 tables exist
2. Confirm each day has FULL snapshots (not just deltas)
3. Validate expected record counts
4. Detect and verify changes between days
5. Check referential integrity within each snapshot

## Usage in Bronze Layer

The Bronze layer ingestion will:

1. **Load Day 1 snapshot** → All records marked as `change_type='insert'`
2. **Load Day 2 snapshot** → Compare to Day 1:
   - New records → `change_type='insert'`
   - Missing records → `change_type='delete'`
   - Changed records → `change_type='update'`
   - Unchanged records → `change_type='no_change'`
3. **Load Day 3 snapshot** → Compare to Day 2 (same logic)

## How to Generate Source Data

Run the scripts in order:

```bash
# Step 1: Generate Day 1 baseline
python notebooks/source_data/01_day1_full_snapshot.py

# Step 2: Generate Day 2 with first changes
python notebooks/source_data/02_day2_incremental_changes.py

# Step 3: Generate Day 3 with second changes
python notebooks/source_data/03_day3_incremental_changes.py

# Step 4: Validate all data
python notebooks/validation/validate_source_data_snapshots.py
```

## File Format

All tables are stored as **Delta Lake tables** with format:
- **Format**: Delta Lake (Apache Parquet with transaction log)
- **Mode**: Overwrite (each day creates fresh snapshot)
- **Schema**: Strongly typed with explicit schemas
- **Path Pattern**: `Tables/source_data/{day}_{table}`

## Benefits of This Approach

1. **Simplicity**: Easy to understand - each file is a complete snapshot
2. **Independence**: Each day's data is self-contained
3. **Testability**: Easy to validate each snapshot independently
4. **Reprocessability**: Any day can be reprocessed without dependencies
5. **Debuggability**: Clear separation of each day's state
6. **Realistic**: Mirrors how many real source systems work (nightly extracts)

---

**Related Documentation**:
- [Source Data Validation](source-data-validation.md)
- [Source Data Specification](source-data-spec.md)
- [Bronze Layer Data Ingestion](../features/bronze/data_ingestion.feature)
