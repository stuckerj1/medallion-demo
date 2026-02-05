# Medallion Architecture Demo for Microsoft Fabric

## Overview

This repository demonstrates a **medallion architecture** implementation for Microsoft Fabric, showcasing best practices for building a modern data lakehouse with bronze, silver, and gold layers. The silver layer incorporates **Data Vault 2.0** principles for flexible, auditable, and scalable data modeling, including **Type 1 and Type 2 Slowly Changing Dimensions (SCDs)**.

## Project Status

**Current Phase**: Phase 0 - Planning & Specification

This project is currently in the planning phase, focusing on:
- Detailed architecture specifications
- Behavior-Driven Development (BDD) scenarios
- Data quality framework definition
- Implementation roadmap

## Architecture Layers

### 🥉 Bronze Layer (Raw Data)
The bronze layer stores raw, unprocessed data in its original format. It serves as the immutable source of truth with complete data lineage.

**Key Features:**
- Append-only storage
- Preserves original data formats
- Metadata capture (timestamps, source systems, file names)
- Supports full reprocessing

### 🥈 Silver Layer (Normalized & Conformed)
The silver layer applies data cleansing, normalization, and implements Data Vault 2.0 modeling with slowly changing dimensions.

**Key Features:**
- Data Vault 2.0 components: Hubs, Links, Satellites
- Type 1 SCD (overwrites for corrections)
- Type 2 SCD (historical tracking)
- Business key management
- Hash-based change detection
- Referential integrity

### 🥇 Gold Layer (Business-Ready)
The gold layer provides curated, aggregated data optimized for analytics and reporting.

**Key Features:**
- Conformed dimensions and fact tables
- Star/snowflake schemas
- Pre-calculated KPIs and metrics
- Denormalized for query performance
- BI-tool ready

## Data Vault 2.0 Integration

The silver layer leverages Data Vault 2.0 principles for:

- **Hubs**: Core business entities with immutable business keys
- **Links**: Relationships between entities
- **Satellites**: Descriptive attributes with full historical tracking (Type 2 SCD)
- **Hash Keys**: Consistent, deterministic key generation
- **Auditability**: Complete load tracking and source attribution

## Documentation

### Specifications
- [Architecture Specification](docs/architecture.md) - Detailed medallion architecture design
- [Data Vault 2.0 Principles](docs/data-vault-principles.md) - Data Vault implementation patterns
- [Data Quality Framework](docs/data-quality-framework.md) - Quality gates and validation rules
- [Error Handling in Source Data Generation](docs/error-handling-source-data.md) - Error capture, logging, and resolution strategies
- [Implementation Roadmap](docs/implementation-roadmap.md) - Phased implementation plan
- [Northwind Setup Guide](docs/northwind-setup.md) - Source data initialization with 3-day snapshots

### BDD Feature Files

#### Bronze Layer
- [Data Ingestion](features/bronze/data_ingestion.feature) - Raw data ingestion scenarios
- [Source Data Error Handling](features/bronze/source_data_error_handling.feature) - Error handling and recovery scenarios

#### Silver Layer
- [Hub Management](features/silver/hub_management.feature) - Business entity hub scenarios
- [Satellite Management](features/silver/satellite_management.feature) - Type 2 SCD and historical tracking
- [Link Management](features/silver/link_management.feature) - Entity relationship scenarios

#### Gold Layer
- [Dimension Tables](features/gold/dimension_tables.feature) - Conformed dimension scenarios
- [Fact Tables](features/gold/fact_tables.feature) - Fact table and metric scenarios

## Key Concepts

### Slowly Changing Dimensions (SCDs)

**Type 1 SCD**: Overwrites existing values (no history)
- Use case: Data corrections, non-significant changes
- Example: Fixing a misspelled customer name

**Type 2 SCD**: Maintains full historical versions
- Use case: Tracking changes over time
- Example: Customer address changes, price changes
- Implementation: Effective dates, current flags, hash diff for change detection

### Data Vault 2.0 Components

**Hubs**: 
```
hub_customer
  - hub_customer_hashkey (PK)
  - business_key (customer_id)
  - load_date
  - record_source
```

**Links**: 
```
link_customer_order
  - link_hashkey (PK)
  - hub_customer_hashkey (FK)
  - hub_order_hashkey (FK)
  - load_date
  - record_source
```

**Satellites**: 
```
sat_customer_details
  - hub_customer_hashkey (PK, FK)
  - load_date (PK)
  - load_end_date
  - hash_diff
  - is_current
  - first_name
  - last_name
  - email
  - ...
```

## Implementation Phases

1. **Phase 0**: Planning & Specification ✅ (Current)
2. **Phase 1**: Foundation & Bronze Layer
3. **Phase 2**: Silver Layer - Hubs
4. **Phase 3**: Silver Layer - Links
5. **Phase 4**: Silver Layer - Satellites & SCD
6. **Phase 5**: Data Quality Framework
7. **Phase 6**: Gold Layer - Dimensions
8. **Phase 7**: Gold Layer - Facts
9. **Phase 8**: Integration & Optimization
10. **Phase 9**: Production Deployment
11. **Phase 10**: Continuous Improvement

See [Implementation Roadmap](docs/implementation-roadmap.md) for detailed timelines and deliverables.

## Technology Stack

- **Platform**: Microsoft Fabric
- **Storage**: Delta Lake / Parquet
- **Processing**: Spark / SQL
- **Orchestration**: Data Pipelines
- **Testing**: BDD (Behavior-Driven Development)
- **Version Control**: Git

## Getting Started

### Prerequisites
- Microsoft Fabric workspace
- Access to source systems
- Development environment setup

### Quick Start with Northwind Sample Data

To get started with a working example using the Northwind database:

1. **Review the Setup Guide**: See [Northwind Setup Documentation](docs/northwind-setup.md) for detailed instructions
2. **Run the Source Data Notebooks**: Execute the notebooks in `/notebooks/source_data/` to generate three days of snapshots
3. **Ingest into Bronze Layer**: Run the Bronze ingestion notebook to load data with change tracking
4. **Validate the Setup**: Use the validation notebook to verify everything is working correctly

The Northwind implementation demonstrates:
- Source data snapshots (Day 1: full load, Days 2-3: incremental changes)
- Bronze layer ingestion with change tracking (`change_type`, `load_date`)
- Insert, update, and delete operations across snapshots
- Change detection using hash-based comparison

### Current Phase Activities

As we are in Phase 0 (Planning), the current focus is on:

1. **Reviewing Specifications**: Examine the architecture and Data Vault documents
2. **Analyzing BDD Scenarios**: Review feature files to understand expected behaviors
3. **Providing Feedback**: Iterate on specifications before code implementation
4. **Stakeholder Alignment**: Ensure all parties agree on the approach

### Next Steps

Once Phase 0 is complete:

1. Set up Microsoft Fabric environment
2. Implement bronze layer ingestion framework
3. Begin silver layer Hub implementation
4. Continue through subsequent phases per the roadmap

## Contributing

During the planning phase, contributions should focus on:
- Reviewing and refining specifications
- Adding or modifying BDD scenarios
- Identifying edge cases and requirements
- Suggesting improvements to the architecture

## License

[To be determined]

## Contact

[To be determined]

---

**Note**: This is a demonstration project showcasing medallion architecture patterns with Data Vault 2.0 principles in Microsoft Fabric. The specifications and BDD scenarios are designed to be comprehensive and instructive for similar implementations.