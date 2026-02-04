# Data Vault 2.0 Principles for Silver Layer

## Introduction

The silver layer adopts Data Vault 2.0 modeling principles to provide a flexible, scalable, and auditable data architecture. This document outlines how Data Vault concepts are applied in our medallion architecture.

## Core Components

### 1. Hubs

**Purpose**: Represent core business entities with their business keys.

**Structure**:
```
hub_<entity_name>
  - hub_<entity>_hashkey (PK): Hash of business key
  - business_key: Natural business identifier
  - load_date: When the record was loaded
  - record_source: Source system identifier
```

**Example - Hub Customer**:
```
hub_customer_hashkey | business_key (customer_id) | load_date           | record_source
---------------------|----------------------------|---------------------|---------------
ABC123...            | CUST-12345                 | 2024-01-15 10:30:00 | ERP_SYSTEM
DEF456...            | CUST-67890                 | 2024-01-15 10:35:00 | CRM_SYSTEM
```

**Key Principles**:
- Business keys are immutable
- One row per unique business entity
- No descriptive attributes (those go in Satellites)
- Insert-only (never updated or deleted)

### 2. Links

**Purpose**: Represent relationships between business entities (Hubs).

**Structure**:
```
link_<entity1>_<entity2>
  - link_hashkey (PK): Hash of all foreign hub keys
  - hub_<entity1>_hashkey (FK): Reference to first Hub
  - hub_<entity2>_hashkey (FK): Reference to second Hub
  - load_date: When the relationship was loaded
  - record_source: Source system identifier
```

**Example - Link Customer Order**:
```
link_hashkey | hub_customer_hashkey | hub_order_hashkey | load_date           | record_source
-------------|---------------------|-------------------|---------------------|---------------
XYZ789...    | ABC123...           | MNO345...         | 2024-01-15 11:00:00 | ORDER_SYSTEM
```

**Key Principles**:
- Represents many-to-many relationships
- Transaction-less (no attributes, only keys)
- Insert-only (never updated or deleted)
- Can link multiple Hubs (3-way, 4-way links)

### 3. Satellites

**Purpose**: Store descriptive attributes and track historical changes.

**Structure**:
```
sat_<entity>_<context>
  - hub/link_hashkey (PK, FK): Reference to parent Hub or Link
  - load_date (PK): When the record was loaded
  - load_end_date: When the record became inactive (NULL for current)
  - hash_diff: Hash of all descriptive attributes
  - is_current: Boolean flag for current record
  - record_source: Source system identifier
  - <attribute1>: Descriptive attribute
  - <attribute2>: Descriptive attribute
  - ...
```

**Example - Satellite Customer Details**:
```
hub_customer_hashkey | load_date           | load_end_date       | hash_diff | is_current | first_name | last_name | email
---------------------|---------------------|---------------------|-----------|------------|------------|-----------|-------------------
ABC123...            | 2024-01-15 10:30:00 | 2024-02-01 14:00:00 | OLD_HASH  | false      | John       | Smith     | john@old.com
ABC123...            | 2024-02-01 14:00:00 | NULL                | NEW_HASH  | true       | John       | Smith     | john@new.com
```

**Key Principles**:
- Type 2 SCD by default (full history)
- Multiple satellites per Hub for different rates of change
- Hash diff enables efficient change detection
- Only insert new records when data changes

## Implementation Patterns

### Hub Loading Pattern

1. Extract business keys from source
2. Generate hash keys using consistent hashing algorithm
3. Check if business key already exists in Hub
4. Insert only new business keys (avoid duplicates)

```python
# Pseudocode
new_hubs = source_data.select(business_key).distinct()
existing_hubs = hub_table.select(business_key)
inserts = new_hubs.subtract(existing_hubs)
hub_table.insert(inserts.with_hashkey().with_metadata())
```

### Satellite Loading Pattern (Type 2 SCD)

1. Calculate hash_diff for incoming records
2. Join with existing current records
3. Compare hash_diff to detect changes
4. For changed records:
   - End-date the old record (set load_end_date, is_current=false)
   - Insert new record with current date
5. For new records: Insert with is_current=true

```python
# Pseudocode
source_with_hash = source_data.with_hash_diff()
current_records = satellite_table.filter(is_current == true)
joined = source_with_hash.join(current_records, on=hub_hashkey)

# Detect changes
changes = joined.filter(source.hash_diff != current.hash_diff)

# End-date old records
end_dated = changes.select(current_record).with_end_date(current_timestamp)

# Insert new records
new_records = source_with_hash.with_load_date(current_timestamp).with_is_current(true)

# Apply updates
satellite_table.update(end_dated)
satellite_table.insert(new_records)
```

### Link Loading Pattern

1. Extract relationship keys from source
2. Generate hash keys for related Hubs
3. Generate link hash key from Hub hash keys
4. Insert only new relationships

```python
# Pseudocode
relationships = source_data.select(customer_id, order_id)
with_hub_keys = relationships.with_hub_hashkeys()
with_link_key = with_hub_keys.with_link_hashkey()
existing_links = link_table.select(link_hashkey)
new_links = with_link_key.subtract(existing_links)
link_table.insert(new_links.with_metadata())
```

## Hash Key Generation

**Consistent hashing is critical**:
- Use SHA-256 or MD5 for hash generation
- Concatenate business keys in consistent order
- Handle nulls consistently
- Convert all values to strings before hashing
- Use same hashing algorithm across all layers

**Example**:
```python
import hashlib

def generate_hash_key(*values):
    # Convert to strings and handle nulls
    str_values = [str(v).upper() if v is not None else 'NULL' for v in values]
    # Concatenate with delimiter
    combined = '||'.join(str_values)
    # Generate hash
    return hashlib.sha256(combined.encode()).hexdigest()
```

## Type 1 vs Type 2 SCD in Data Vault

### Type 2 SCD (Default)
- Implemented in standard Satellites
- Full history preserved
- Use for: addresses, prices, status changes

### Type 1 SCD (Overwrite)
- Special case Satellites or simple updates
- No history preserved
- Use for: data corrections, non-significant changes
- Can be implemented as a separate satellite with single row per entity

## Advantages of Data Vault 2.0

1. **Agility**: Easy to add new data sources without breaking existing structures
2. **Auditability**: Complete audit trail with load dates and sources
3. **Flexibility**: Supports multiple sources for same entity
4. **Scalability**: Parallel loading of Hubs, Links, and Satellites
5. **Historical Accuracy**: Type 2 SCD by default
6. **Business-Centric**: Organized around business concepts, not source systems

## Design Considerations

### When to Create a Hub
- Represents a core business concept
- Has a natural business key
- Appears in multiple contexts/relationships

### When to Create a Link
- Represents a relationship between entities
- May have transactional nature
- Can have its own descriptive attributes (via Link Satellites)

### When to Create a Satellite
- Contains descriptive attributes
- Attributes change at different rates
- Need historical tracking
- Separate satellites for different rates of change

### Satellite Splitting
Create multiple satellites when:
- Attributes change at different frequencies
- Attributes come from different sources
- Attributes have different security/privacy requirements

**Example**: Customer Hub might have:
- sat_customer_details: name, date of birth (rarely changes)
- sat_customer_contact: email, phone (changes occasionally)
- sat_customer_preferences: marketing preferences (changes frequently)

## Integration with Medallion Architecture

**Bronze to Silver**:
- Bronze contains raw source data
- Silver transformation extracts business keys and creates Hubs
- Descriptive attributes loaded into Satellites
- Relationships identified and loaded into Links

**Silver to Gold**:
- Join Hubs, Links, and Satellites to create denormalized views
- Current records only (is_current = true) for point-in-time views
- Historical analysis uses all Satellite records
- Transform into star/snowflake schemas for analytics
