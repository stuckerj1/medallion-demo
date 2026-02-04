# Data Quality Framework

## Overview

This document specifies the data quality framework for the medallion architecture. Quality gates are implemented at each layer to ensure data accuracy, completeness, and consistency.

## Quality Dimensions

### 1. Accuracy
Data correctly represents the real-world values.

**Checks:**
- Value range validation
- Format validation (email, phone, postal codes)
- Cross-reference validation with trusted sources
- Business rule validation

### 2. Completeness
Required data elements are present.

**Checks:**
- NULL checks on mandatory fields
- Record count validation
- Required relationship validation
- Minimum data threshold checks

### 3. Consistency
Data is consistent across systems and over time.

**Checks:**
- Cross-system consistency
- Referential integrity
- Data type consistency
- Naming convention consistency

### 4. Timeliness
Data is available when needed and up-to-date.

**Checks:**
- Data freshness validation
- SLA compliance checks
- Load time monitoring
- Data latency measurement

### 5. Uniqueness
No unintended duplicate records exist.

**Checks:**
- Primary key uniqueness
- Business key uniqueness
- Duplicate detection algorithms
- Fuzzy matching for near-duplicates

### 6. Validity
Data conforms to defined formats and business rules.

**Checks:**
- Data type validation
- Domain value validation
- Pattern matching (regex)
- Business logic validation

## Quality Gates by Layer

### Bronze Layer Quality Gates

**Purpose**: Ensure raw data is captured correctly and completely.

**Checks:**
1. **File Validation**
   - File exists and is readable
   - File size within expected range
   - File format matches expected format
   - File not empty

2. **Schema Validation**
   - Expected columns present
   - Column data types can be inferred
   - No unexpected columns (warning only)

3. **Basic Completeness**
   - Record count > 0
   - No completely empty rows
   - File metadata captured

4. **Metadata Capture**
   - Source system recorded
   - Ingestion timestamp recorded
   - File name and path recorded
   - Record count logged

**Actions on Failure:**
- Move file to quarantine area
- Log error details
- Send alert notification
- Continue processing other files

### Silver Layer Quality Gates

**Purpose**: Ensure data is clean, normalized, and conforms to business rules.

**Checks:**
1. **Business Key Validation**
   - Business keys are not NULL
   - Business keys are unique within source
   - Business key format is valid
   - No invalid characters in business keys

2. **Data Type Validation**
   - Dates are valid dates
   - Numbers are within expected ranges
   - Strings don't exceed max length
   - Boolean values are true/false

3. **Referential Integrity**
   - Foreign keys reference existing parent records
   - No orphaned records
   - Link tables reference existing Hubs
   - Satellites reference existing Hubs/Links

4. **Business Rule Validation**
   - Email addresses are valid format
   - Phone numbers match pattern
   - Postal codes match country format
   - Dates are in logical order (start_date < end_date)

5. **Duplicate Detection**
   - No duplicate business keys in Hubs
   - No duplicate Hash keys
   - Satellite changes are genuine changes
   - No duplicate Links

6. **Data Completeness**
   - Required attributes are not NULL
   - Minimum data quality score met
   - Critical fields populated
   - Record counts match expected volumes

**Actions on Failure:**
- Reject invalid records
- Log data quality issues
- Route to exception handling
- Create data quality report
- Alert data stewards

### Gold Layer Quality Gates

**Purpose**: Ensure business-ready data is accurate and consistent for analytics.

**Checks:**
1. **Dimension Validation**
   - All dimension keys are unique
   - No NULL surrogate keys
   - All business keys present
   - Special members (Unknown, N/A) exist

2. **Fact Validation**
   - All dimension foreign keys valid
   - No NULL measures (unless business rule allows)
   - Additive measures are numeric
   - Derived calculations are correct

3. **Referential Integrity**
   - All fact foreign keys reference dimension tables
   - No orphaned facts
   - Date keys exist in date dimension
   - Geography keys exist in geography dimension

4. **Aggregation Validation**
   - Aggregated values match detail
   - No data loss during aggregation
   - Aggregate grain is correct
   - Rollup calculations are accurate

5. **Business Logic Validation**
   - KPI calculations are correct
   - Metric formulas applied correctly
   - Business rules enforced
   - Derived attributes calculated properly

6. **Historical Accuracy**
   - Type 2 SCD versions are correct
   - Effective dates are sequential
   - Current flags are accurate
   - No overlapping date ranges

**Actions on Failure:**
- Block publication to BI layer
- Alert business users
- Create data quality dashboard entry
- Trigger investigation workflow
- Roll back if critical

## Data Quality Metrics

Track these metrics for each layer and pipeline:

### Bronze Layer Metrics
```
- Files ingested successfully / total files
- Records ingested successfully / total records
- Data volume (GB) ingested per day
- Average ingestion time per file
- Number of files quarantined
- Schema validation failure rate
```

### Silver Layer Metrics
```
- Records passing validation / total records
- Business key validation failure rate
- Referential integrity failure rate
- Duplicate detection rate
- Data completeness score (% of required fields populated)
- Data accuracy score (% passing business rules)
```

### Gold Layer Metrics
```
- Dimension load success rate
- Fact load success rate
- Referential integrity score
- Aggregation accuracy rate
- SCD version correctness rate
- Query performance (avg response time)
```

## Quality Monitoring & Reporting

### Real-time Monitoring
- Dashboard showing current data quality scores
- Alerts for quality threshold breaches
- Pipeline execution status
- Error counts and types

### Historical Reporting
- Trend analysis of quality metrics
- Improvement tracking over time
- Root cause analysis reports
- Data lineage impact analysis

### Quality Scorecards
```
Layer: Silver
Entity: Customer
Date: 2024-01-15

Quality Dimension    | Score | Status | Issues
--------------------|-------|--------|--------
Accuracy            | 98.5% | PASS   | 15 invalid emails
Completeness        | 99.2% | PASS   | 8 missing phone numbers
Consistency         | 100%  | PASS   | None
Uniqueness          | 99.8% | PASS   | 2 duplicate business keys
Validity            | 97.5% | WARN   | 25 invalid date formats
Overall             | 98.6% | PASS   | See details
```

## Quality Rules Repository

Maintain a centralized repository of quality rules:

**Structure:**
```
Rule ID: DQ-001
Rule Name: Email Format Validation
Description: Email addresses must match standard email format
Layer: Silver
Entity: Customer
Attribute: email
Validation: REGEX '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
Severity: WARNING
Action: Flag for review
Owner: Data Steward - Customer Domain
```

## Exception Handling

### Exception Types
1. **Technical Exceptions**: System errors, connectivity issues
2. **Data Exceptions**: Quality rule violations
3. **Business Exceptions**: Business rule violations

### Exception Workflow
1. **Capture**: Log exception details
2. **Classify**: Determine exception type and severity
3. **Route**: Send to appropriate handler
4. **Resolve**: Fix or accept exception
5. **Track**: Monitor resolution progress
6. **Learn**: Update rules and processes

### Exception Storage
Store exceptions in dedicated tables:

```sql
exception_log
  - exception_id
  - exception_timestamp
  - layer (bronze/silver/gold)
  - entity_name
  - rule_id
  - severity (ERROR/WARNING/INFO)
  - record_identifier
  - exception_details
  - status (OPEN/RESOLVED/ACCEPTED)
  - assigned_to
  - resolution_date
  - resolution_notes
```

## Data Quality Improvement Process

1. **Measure**: Establish baseline quality metrics
2. **Analyze**: Identify patterns and root causes
3. **Improve**: Implement fixes and enhancements
4. **Control**: Monitor improvements and sustain gains
5. **Iterate**: Continuously refine quality processes

## Quality Certification

Before promoting data to production:
- All critical quality checks must pass
- Quality score must meet minimum threshold (typically 95%+)
- Data steward must review and approve
- Documentation must be updated
- User acceptance testing completed
