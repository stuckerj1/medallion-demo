# Medallion Architecture Specification

## Overview

This document specifies the medallion architecture for Microsoft Fabric, implementing a three-layer data processing pipeline: Bronze, Silver, and Gold. This architecture follows best practices for data lakehouse implementations and borrows principles from Data Vault 2.0 for the silver layer.

## Architecture Layers

### Bronze Layer (Raw Data)
The bronze layer stores raw data in its original format with minimal transformations. This layer serves as the source of truth and provides data lineage traceability.

**Characteristics:**
- Stores data exactly as received from source systems
- Append-only architecture (immutable)
- Includes metadata: ingestion timestamp, source system, file name
- Preserves data lineage and audit trail
- Supports full reprocessing capabilities
- File formats: Parquet, JSON, CSV, or source native format

**Data Organization:**
```
bronze/
  ├── source_system_1/
  │   ├── table_1/
  │   │   └── YYYY/MM/DD/HH/data.parquet
  │   └── table_2/
  │       └── YYYY/MM/DD/HH/data.parquet
  └── source_system_2/
      └── table_3/
          └── YYYY/MM/DD/HH/data.parquet
```

### Silver Layer (Normalized & Conformed)
The silver layer performs data cleansing, normalization, and implements slowly changing dimensions (Type 1 & Type 2). This layer applies Data Vault 2.0 principles for modeling.

**Characteristics:**
- Cleansed and validated data
- Standardized data types and formats
- De-duplicated records
- Implements Type 1 SCD (overwrites) and Type 2 SCD (historical tracking)
- Data Vault 2.0 components: Hubs, Links, Satellites
- Business keys established
- Referential integrity enforced
- File format: Delta Lake tables

**Data Vault 2.0 Components:**

1. **Hubs**: Core business entities
   - Contains business key and metadata (hash key, load date, source)
   - Examples: Customer, Product, Order

2. **Links**: Relationships between Hubs
   - Contains hash keys from related Hubs
   - Examples: Customer-Order, Order-Product

3. **Satellites**: Descriptive attributes of Hubs/Links
   - Contains historical changes (Type 2 SCD)
   - Includes effective dates and hash diff for change detection
   - Multiple satellites can exist for different change frequencies

**Type 1 SCD**: Current value only (overwrites)
- Used for corrections or non-historical attributes
- Example: Customer address correction

**Type 2 SCD**: Historical tracking
- Maintains full history of changes
- Includes: effective_from, effective_to, is_current flags
- Example: Customer address changes over time

**Data Organization:**
```
silver/
  ├── hubs/
  │   ├── hub_customer/
  │   ├── hub_product/
  │   └── hub_order/
  ├── links/
  │   ├── link_customer_order/
  │   └── link_order_product/
  └── satellites/
      ├── sat_customer_details/
      ├── sat_customer_address/
      ├── sat_product_details/
      └── sat_order_details/
```

### Gold Layer (Business-Ready)
The gold layer contains curated, aggregated, and business-ready datasets optimized for analytics and reporting.

**Characteristics:**
- Business-level aggregations and calculations
- Denormalized for query performance
- Conformed dimensions and fact tables
- Star/snowflake schemas
- Pre-calculated KPIs and metrics
- Optimized for BI tool consumption
- File format: Delta Lake tables

**Data Organization:**
```
gold/
  ├── dimensions/
  │   ├── dim_customer/
  │   ├── dim_product/
  │   ├── dim_date/
  │   └── dim_geography/
  └── facts/
      ├── fact_sales/
      ├── fact_inventory/
      └── fact_customer_activity/
```

## Data Flow

```
Source Systems → Bronze (Raw) → Silver (Normalized/SCD/DV2.0) → Gold (Business-Ready) → Analytics/BI
```

## Quality Gates

Each layer implements specific quality checks:

**Bronze → Silver:**
- Schema validation
- Null checks on required fields
- Data type validation
- Duplicate detection
- Business key validation

**Silver → Gold:**
- Referential integrity checks
- Completeness checks
- Business rule validation
- Aggregation validation
- Metric calculation verification

## Metadata Management

All layers include metadata tracking:
- Load timestamp
- Source system
- Record count
- Data quality metrics
- Processing duration
- Error counts

## Recovery & Reprocessing

The architecture supports:
- Point-in-time recovery
- Full reprocessing from bronze
- Incremental processing
- Late-arriving data handling
- Error recovery and retry mechanisms
