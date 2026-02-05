# Source Data Snapshot Validation

## Overview

This document explains the validation approach for source data snapshots used in the medallion architecture Bronze layer ingestion demo.

## Daily Full Snapshot Load & Compare Approach

The source data follows a **daily full snapshot load & compare** pattern, which is a common approach for batch data ingestion:

### Pattern Definition

**Full Snapshot**: Each day's data represents a complete snapshot of the source system at that point in time, not just the changes (deltas).

**Load & Compare**: The Bronze layer ingestion process loads each full snapshot and compares it to the previous day's snapshot to detect changes (inserts, updates, deletes).

### Why This Approach?

1. **Simplicity**: Source systems don't need to track changes; they just export their current state
2. **Reliability**: Each snapshot is self-contained and can be independently verified
3. **Reprocessability**: Any day's data can be reprocessed without dependencies on previous days
4. **Auditability**: Complete historical state is preserved for each day

## Source Data Specification

### Day 1: Initial Baseline

**Purpose**: Establish the baseline state of the source system

**Contents**:
- Full snapshot of all tables
- All records are new (will be treated as inserts in Bronze)
- No change tracking needed at source level

**Expected Data**:
- Categories: 8 records
- Suppliers: 5 records
- Products: 20 records
- Customers: 20 records
- Employees: 9 records
- Orders: 50 records
- OrderDetails: ~150+ records (varies due to random generation)

### Day 2: First Incremental Snapshot

**Purpose**: Simulate a typical day's changes in the source system

**Contents**:
- Full snapshot of all tables reflecting changes
- Changes compared to Day 1:
  - **Customers**: 5 new customers added (FRANK, FRANS, FURIB, GALED, GODOS)
  - **Products**: 1 product deleted (ProductID 5 - "Chef Anton's Gumbo Mix")
  - **Orders**: 10 new orders added, 20 existing orders updated (ShipCity, ShipCountry)
  - **OrderDetails**: New order details for new orders, price updates for existing products

**Expected Data**:
- Categories: 8 records (unchanged)
- Suppliers: 5 records (unchanged)
- Employees: 9 records (unchanged)
- Products: 19 records (20 - 1 deleted)
- Customers: 25 records (20 + 5 new)
- Orders: 60 records (50 + 10 new)
- OrderDetails: ~200+ records

**Detectable Changes**:
- **Inserts**: New customers, new orders, new order details
- **Deletes**: Product ID 5 removed
- **Updates**: Order shipping information changed

### Day 3: Second Incremental Snapshot

**Purpose**: Demonstrate continued evolution of source data

**Contents**:
- Full snapshot of all tables reflecting additional changes
- Changes compared to Day 2:
  - **Products**: 3 new products added (ProductIDs 21, 22, 23)
  - **Customers**: 15 customers have updated ContactTitle (count stays 25)
  - **Orders**: 3 orders deleted (10250, 10251, 10252), 10 new orders added
  - **OrderDetails**: Details for deleted orders removed, new details added, 30 price/quantity updates

**Expected Data**:
- Categories: 8 records (unchanged)
- Suppliers: 5 records (unchanged)
- Employees: 9 records (unchanged)
- Products: 22 records (19 + 3 new)
- Customers: 25 records (same count, but 15 updated)
- Orders: 67 records (60 - 3 deleted + 10 new)
- OrderDetails: ~250+ records

**Detectable Changes**:
- **Inserts**: New products, new orders
- **Deletes**: 3 orders removed
- **Updates**: Customer contact titles changed, order detail prices/quantities changed

## Validation Strategy

### 1. Snapshot Existence Validation

**Purpose**: Verify all required snapshot tables exist

**Test**: For each combination of day (1, 2, 3) and table, verify table exists and is readable

**Failure Impact**: CRITICAL - prevents any further processing

### 2. Full Snapshot Validation

**Purpose**: Verify each day contains FULL snapshots, not just deltas

**Test**: Check that each day's tables contain substantial data (not just a few changed records)

**Key Point**: This distinguishes full snapshot approach from delta/CDC approaches

**Failure Impact**: CRITICAL - indicates misunderstanding of the pattern

### 3. Baseline Validation

**Purpose**: Verify Day 1 establishes correct baseline

**Test**: Verify Day 1 record counts match expected values

**Failure Impact**: ERROR - impacts all subsequent change detection

### 4. Change Detection (Day 1 → Day 2)

**Purpose**: Verify changes can be detected between snapshots

**Tests**:
- Count changes match expectations
- Net changes (inserts - deletes) are correct
- Specific changes are present (e.g., Product 5 deleted, 5 new customers)

**Failure Impact**: ERROR - indicates data generation issues

### 5. Change Detection (Day 2 → Day 3)

**Purpose**: Verify continued change detection works

**Tests**:
- Count changes match expectations
- Net changes are correct
- Specific changes are present (e.g., 3 orders deleted, 3 products added)

**Failure Impact**: ERROR - indicates data generation issues

### 6. Update Detection

**Purpose**: Verify that updates (same key, different values) can be detected

**Tests**:
- Compare matching records between snapshots
- Count records where non-key fields changed
- Examples: Customer ContactTitle changes, Order ShipCity changes

**Failure Impact**: WARNING - updates should be detectable but might not be explicitly tracked

### 7. Data Integrity Validation

**Purpose**: Verify referential integrity within each snapshot

**Tests**:
- Orders reference valid Customers
- OrderDetails reference valid Orders and Products
- No orphaned records

**Failure Impact**: ERROR - indicates data quality issues

## How Bronze Layer Uses This Data

The Bronze layer ingestion process will:

1. **Load Each Snapshot**: Read each day's full snapshot into Bronze tables
2. **Add Metadata**: Add `load_date`, `source_system`, `ingestion_timestamp`
3. **Compare Snapshots**: Use hash-based comparison to detect changes:
   - Records in Day N but not Day N-1 = **INSERT**
   - Records in Day N-1 but not Day N = **DELETE**  
   - Records in both with different hash = **UPDATE**
   - Records in both with same hash = **NO CHANGE**
4. **Track Changes**: Add `change_type` column based on comparison

## Running the Validation

### Prerequisites
- Source data generation scripts (Day 1, 2, 3) have been executed
- All snapshot tables exist in the catalog

### Execution
Run the validation notebook:
```
notebooks/validation/validate_source_data_snapshots.py
```

### Expected Output

**Successful Validation**:
```
VALIDATION SUMMARY
==========================================
Total Tests: 60+
Passed: 60+
Failed: 0
Success Rate: 100.0%

✓ ALL VALIDATION TESTS PASSED
  Source data meets specification for daily full snapshot ingestion.
  Data is READY for Bronze layer ingestion demo!
```

**Failed Validation**:
```
VALIDATION SUMMARY
==========================================
Total Tests: 60+
Passed: 55
Failed: 5
  - Critical: 2
  - Errors: 3
  - Warnings: 0
Success Rate: 91.7%

✗ CRITICAL FAILURES DETECTED
  Source data is NOT ready for Bronze layer ingestion.
  Fix critical issues before proceeding.
```

## Validation Test Categories

### Critical Tests (Must Pass)
- All snapshot tables exist
- Each snapshot is a full snapshot (not just deltas)
- Minimum record counts met

### Error Tests (Should Pass)
- Exact baseline counts (Day 1)
- Expected change counts (Day 1→2, Day 2→3)
- Specific inserts/deletes/updates
- Referential integrity

### Warning Tests (Nice to Have)
- Update detection counts
- Data consistency checks

## Troubleshooting

### "Table not found" errors
**Problem**: Source data generation scripts not run or failed
**Solution**: Execute Day 1, Day 2, Day 3 generation scripts in order

### "Not a full snapshot" errors
**Problem**: Script only created changes, not full data
**Solution**: Review generation script - should copy unchanged data forward

### "Count mismatch" errors
**Problem**: Different number of records than expected
**Solution**: Check generation script for correct insert/delete logic

### "Referential integrity" errors
**Problem**: Orphaned records (e.g., OrderDetail references non-existent Product)
**Solution**: Ensure deleted entities are removed from dependent tables too

## Success Criteria

The source data is ready for Bronze layer ingestion when:

1. ✅ All three daily snapshots exist
2. ✅ Each snapshot contains full data (not just deltas)
3. ✅ Day 1 baseline is correct
4. ✅ Day 1→2 changes are detectable and correct
5. ✅ Day 2→3 changes are detectable and correct
6. ✅ Updates can be detected by comparing snapshots
7. ✅ Referential integrity is maintained within each snapshot

## Next Steps

After successful validation:

1. **Execute Bronze Ingestion**: Run Bronze layer ingestion notebook
2. **Verify Change Tracking**: Confirm Bronze tables have correct `change_type` values
3. **Validate Lineage**: Check that Bronze records link back to source snapshots
4. **Test Reprocessing**: Verify any day can be reprocessed independently

## Related Documentation

- [Source Data Specification](source-data-spec.md)
- [Error Handling in Source Data Generation](error-handling-source-data.md)
- [Data Quality Framework](data-quality-framework.md)
- [Bronze Layer Data Ingestion Feature](../features/bronze/data_ingestion.feature)

---

**Version**: 1.0  
**Last Updated**: 2026-02-05  
**Owner**: Data Engineering Team
