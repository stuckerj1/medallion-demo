# Error Handling in Source Data Generation Scripts

## Overview

This document defines comprehensive error handling strategies for source data generation scripts used in the Bronze phase of the medallion architecture. It covers error capture, logging, classification, and resolution procedures to ensure data integrity and pipeline reliability.

## Objectives

1. **Prevent Data Loss**: Ensure no source data is lost due to processing errors
2. **Enable Debugging**: Provide detailed error information for troubleshooting
3. **Maintain Pipeline Continuity**: Allow pipelines to continue processing despite individual errors
4. **Support Reprocessing**: Enable failed data to be easily identified and reprocessed
5. **Ensure Auditability**: Track all errors for compliance and quality reporting

## Error Classification

### 1. Technical Errors

**Description**: System-level failures that prevent data processing.

**Examples**:
- Network connectivity failures
- File system I/O errors
- Memory/resource exhaustion
- Spark cluster failures
- Authentication/authorization failures
- Database connection timeouts

**Severity**: CRITICAL or ERROR

**Handling Strategy**: 
- Immediate retry with exponential backoff
- Alert operations team
- Log full stack trace
- Halt pipeline if persistent

### 2. Schema Errors

**Description**: Data structure mismatches between source and expected schema.

**Examples**:
- Missing required columns
- Unexpected column names
- Data type incompatibilities
- Nested structure mismatches
- Column count mismatches

**Severity**: ERROR

**Handling Strategy**:
- Quarantine entire file
- Log schema differences
- Notify data stewards
- Attempt schema evolution if configured
- Continue with other files

### 3. Data Quality Errors

**Description**: Data content issues that violate business rules or format expectations.

**Examples**:
- NULL values in required fields
- Invalid data formats (dates, emails, phone numbers)
- Values outside expected ranges
- Referential integrity violations
- Duplicate records

**Severity**: WARNING or ERROR (based on rule)

**Handling Strategy**:
- Quarantine invalid records (row-level)
- Log validation failures
- Continue processing valid records
- Generate data quality report
- Route to exception workflow

### 4. Transformation Errors

**Description**: Failures during data transformation or enrichment.

**Examples**:
- Type conversion failures
- Calculation errors (division by zero)
- String parsing failures
- Date arithmetic errors
- UDF execution failures

**Severity**: ERROR

**Handling Strategy**:
- Isolate failing records
- Log transformation context
- Use default values if configured
- Continue with other records
- Report transformation success rate

### 5. Business Logic Errors

**Description**: Violations of business rules specific to the domain.

**Examples**:
- Order date after ship date
- Negative quantities or prices
- Invalid state transitions
- Mandatory relationship violations
- Business key conflicts

**Severity**: WARNING or ERROR (based on rule severity)

**Handling Strategy**:
- Flag records for business review
- Log rule violations with context
- Apply business exception workflow
- Continue processing
- Generate business exception report

## Error Capture Mechanisms

### 1. Try-Catch Blocks

**Purpose**: Capture and handle exceptions in code execution.

**Implementation Pattern**:
```python
try:
    # Data loading or transformation
    df = load_source_data(source_path)
    df_transformed = apply_transformations(df)
except FileNotFoundError as e:
    log_error("FILE_NOT_FOUND", source_path, str(e))
    notify_operations("Critical: Source file missing", str(e))
    raise
except SchemaException as e:
    log_error("SCHEMA_MISMATCH", source_path, str(e))
    quarantine_file(source_path, "schema_error")
    return None
except Exception as e:
    log_error("UNEXPECTED_ERROR", source_path, str(e))
    log_stack_trace(e)
    raise
```

**Best Practices**:
- Catch specific exceptions before generic ones
- Always log context information
- Re-raise critical errors
- Avoid silent failures

### 2. Data Validation Functions

**Purpose**: Validate data quality before processing.

**Implementation Pattern**:
```python
def validate_record(record):
    errors = []
    
    # Check required fields
    if not record.get('customer_id'):
        errors.append({
            'error_type': 'NULL_REQUIRED_FIELD',
            'field': 'customer_id',
            'severity': 'ERROR'
        })
    
    # Check data formats
    if record.get('email') and not is_valid_email(record['email']):
        errors.append({
            'error_type': 'INVALID_FORMAT',
            'field': 'email',
            'value': record['email'],
            'severity': 'WARNING'
        })
    
    # Check business rules
    if record.get('quantity', 0) < 0:
        errors.append({
            'error_type': 'INVALID_VALUE',
            'field': 'quantity',
            'value': record['quantity'],
            'rule': 'quantity_must_be_positive',
            'severity': 'ERROR'
        })
    
    return errors
```

**Best Practices**:
- Return structured error objects
- Include field name, value, and rule
- Set appropriate severity levels
- Keep validation rules configurable

### 3. Schema Enforcement

**Purpose**: Detect and handle schema changes early.

**Implementation Pattern**:
```python
expected_schema = StructType([
    StructField("CustomerID", StringType(), False),
    StructField("CompanyName", StringType(), False),
    StructField("Email", StringType(), True),
    # ... other fields
])

try:
    df = spark.read.schema(expected_schema).csv(source_path)
except AnalysisException as e:
    if "does not match expected schema" in str(e):
        log_schema_error(source_path, expected_schema, e)
        handle_schema_evolution(source_path, e)
    else:
        raise
```

**Best Practices**:
- Define explicit schemas
- Use schema evolution policies
- Log schema differences
- Version schema definitions

### 4. Guardrails and Circuit Breakers

**Purpose**: Prevent cascading failures and protect downstream systems.

**Implementation Pattern**:
```python
# Record count validation
expected_min_records = 100
expected_max_records = 1000000

record_count = df.count()

if record_count < expected_min_records:
    log_error("INSUFFICIENT_RECORDS", source_path, 
              f"Only {record_count} records, expected at least {expected_min_records}")
    raise ValueError("Record count below threshold")

if record_count > expected_max_records:
    log_error("EXCESSIVE_RECORDS", source_path,
              f"{record_count} records exceeds maximum {expected_max_records}")
    raise ValueError("Record count exceeds threshold")

# Error rate circuit breaker
error_rate = calculate_error_rate(df)
if error_rate > 0.10:  # More than 10% errors
    log_error("HIGH_ERROR_RATE", source_path,
              f"Error rate {error_rate:.2%} exceeds threshold")
    halt_pipeline("Error rate too high")
```

**Best Practices**:
- Set reasonable thresholds
- Make thresholds configurable
- Alert on threshold violations
- Fail fast for critical issues

## Error Logging Strategies

### 1. Structured Logging Schema

**Log Entry Structure**:
```python
{
    'log_id': 'uuid',
    'timestamp': 'ISO8601 datetime',
    'layer': 'bronze',
    'script_name': 'script identifier',
    'source_system': 'source system name',
    'source_file': 'file path or identifier',
    'error_type': 'error classification',
    'error_code': 'standardized error code',
    'severity': 'CRITICAL|ERROR|WARNING|INFO',
    'message': 'human-readable description',
    'details': {
        'record_id': 'identifier of failing record',
        'field_name': 'field that caused error',
        'field_value': 'problematic value',
        'validation_rule': 'rule that failed',
        'stack_trace': 'technical stack trace'
    },
    'context': {
        'job_id': 'pipeline run identifier',
        'batch_id': 'batch identifier',
        'row_number': 'position in file',
        'partition': 'data partition'
    }
}
```

### 2. Logging Levels

**CRITICAL**: System failures requiring immediate attention
- Example: Cluster failure, storage unavailable

**ERROR**: Data processing failures preventing successful completion
- Example: Schema mismatch, file corruption, high error rate

**WARNING**: Issues that should be reviewed but don't prevent processing
- Example: Data quality issues, optional field validation failures

**INFO**: Normal operational messages
- Example: Processing started, file loaded successfully

### 3. Log Storage

**Error Log Table**:
```sql
CREATE TABLE bronze.error_log (
    error_log_id STRING,
    log_timestamp TIMESTAMP,
    layer STRING,
    script_name STRING,
    source_system STRING,
    source_file STRING,
    error_type STRING,
    error_code STRING,
    severity STRING,
    error_message STRING,
    error_details STRING,  -- JSON
    context STRING,  -- JSON
    resolved BOOLEAN DEFAULT FALSE,
    resolved_timestamp TIMESTAMP,
    resolved_by STRING,
    resolution_notes STRING,
    load_date DATE
)
PARTITIONED BY (load_date, layer)
```

**Best Practices**:
- Store logs in Delta Lake for queryability
- Partition by date and layer
- Index frequently queried fields
- Retain logs per retention policy
- Archive old logs to cold storage

### 4. Logging Implementation

```python
import logging
import json
from datetime import datetime
from uuid import uuid4

class StructuredLogger:
    def __init__(self, layer, script_name, source_system):
        self.layer = layer
        self.script_name = script_name
        self.source_system = source_system
        self.logger = logging.getLogger(__name__)
    
    def log_error(self, error_type, source_file, message, 
                  severity='ERROR', details=None, context=None):
        log_entry = {
            'log_id': str(uuid4()),
            'timestamp': datetime.utcnow().isoformat(),
            'layer': self.layer,
            'script_name': self.script_name,
            'source_system': self.source_system,
            'source_file': source_file,
            'error_type': error_type,
            'severity': severity,
            'message': message,
            'details': details or {},
            'context': context or {}
        }
        
        # Log to standard logger
        self.logger.error(json.dumps(log_entry))
        
        # Persist to error log table
        self.persist_to_table(log_entry)
        
        return log_entry
    
    def persist_to_table(self, log_entry):
        # Implementation to write to Delta table
        pass
```

## Error Resolution Procedures

### 1. Quarantine Pattern

**Purpose**: Isolate problematic data for investigation and reprocessing.

**Implementation**:
```python
QUARANTINE_PATH = "bronze/quarantine/{source_system}/{error_type}/{date}"

def quarantine_file(source_path, error_type, error_details):
    """Move file to quarantine area with metadata"""
    quarantine_location = f"{QUARANTINE_PATH.format(
        source_system=source_system,
        error_type=error_type,
        date=datetime.now().strftime('%Y/%m/%d')
    )}/{os.path.basename(source_path)}"
    
    # Copy file to quarantine
    dbutils.fs.cp(source_path, quarantine_location)
    
    # Create metadata file
    metadata = {
        'original_path': source_path,
        'quarantine_timestamp': datetime.utcnow().isoformat(),
        'error_type': error_type,
        'error_details': error_details,
        'status': 'quarantined'
    }
    
    metadata_path = f"{quarantine_location}.metadata.json"
    dbutils.fs.put(metadata_path, json.dumps(metadata, indent=2))
    
    log_info(f"File quarantined: {source_path} -> {quarantine_location}")

def quarantine_records(df, error_column='validation_errors'):
    """Separate valid and invalid records"""
    # Split dataframe
    valid_df = df.filter(col(error_column).isNull())
    invalid_df = df.filter(col(error_column).isNotNull())
    
    # Save invalid records to quarantine
    if invalid_df.count() > 0:
        quarantine_path = f"{QUARANTINE_PATH.format(
            source_system=source_system,
            error_type='validation_failure',
            date=datetime.now().strftime('%Y/%m/%d')
        )}/records.parquet"
        
        invalid_df.write.mode("append").parquet(quarantine_path)
        
        log_warning(f"Quarantined {invalid_df.count()} invalid records")
    
    return valid_df, invalid_df
```

### 2. Dead Letter Queue Pattern

**Purpose**: Store messages that cannot be processed for manual intervention.

**Implementation**:
```python
def send_to_dead_letter_queue(record, error_info):
    """Store unprocessable records with full context"""
    dlq_entry = {
        'dlq_id': str(uuid4()),
        'timestamp': datetime.utcnow().isoformat(),
        'source_system': source_system,
        'original_record': record,
        'error_type': error_info['error_type'],
        'error_message': error_info['message'],
        'retry_count': 0,
        'max_retries': 3,
        'status': 'pending',
        'load_date': datetime.now().date()
    }
    
    # Append to DLQ table
    dlq_df = spark.createDataFrame([dlq_entry])
    dlq_df.write.mode("append").saveAsTable("bronze.dead_letter_queue")
```

### 3. Retry Mechanism

**Purpose**: Automatically retry transient failures with backoff.

**Implementation**:
```python
import time
from functools import wraps

def retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2):
    """Decorator for retrying operations with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (ConnectionError, TimeoutError) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        log_warning(f"Attempt {attempt + 1} failed, "
                                  f"retrying in {delay}s: {str(e)}")
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        log_error("MAX_RETRIES_EXCEEDED", 
                                str(args), 
                                f"Failed after {max_retries} attempts")
                except Exception as e:
                    # Don't retry non-transient errors
                    log_error("NON_RETRYABLE_ERROR", str(args), str(e))
                    raise
            
            raise last_exception
        return wrapper
    return decorator

@retry_with_backoff(max_retries=3, initial_delay=2)
def load_data_from_source(source_path):
    return spark.read.format("csv").load(source_path)
```

### 4. Manual Resolution Workflow

**Process**:

1. **Identify**: Query error log for unresolved errors
```sql
SELECT * FROM bronze.error_log
WHERE resolved = FALSE
  AND severity IN ('ERROR', 'CRITICAL')
ORDER BY log_timestamp DESC
```

2. **Investigate**: Examine quarantined data and error details
```python
def investigate_error(error_log_id):
    error = spark.table("bronze.error_log") \
        .filter(f"error_log_id = '{error_log_id}'") \
        .first()
    
    print(f"Error Type: {error.error_type}")
    print(f"Source File: {error.source_file}")
    print(f"Message: {error.error_message}")
    print(f"Details: {error.error_details}")
    
    # Load quarantined data if available
    if error.error_type in ['SCHEMA_MISMATCH', 'VALIDATION_FAILURE']:
        quarantine_path = find_quarantine_path(error)
        quarantined_data = spark.read.parquet(quarantine_path)
        display(quarantined_data)
```

3. **Resolve**: Fix root cause (update schema, fix data, adjust rules)

4. **Reprocess**: Resubmit corrected data
```python
def reprocess_quarantined_file(quarantine_path, error_log_id):
    # Load and process quarantined file
    df = spark.read.format("auto").load(quarantine_path)
    processed_df = apply_transformations(df)
    
    # Save to bronze layer
    processed_df.write.mode("append").saveAsTable("bronze.target_table")
    
    # Mark error as resolved
    mark_error_resolved(error_log_id, "Reprocessed after schema fix")
```

5. **Document**: Update error resolution notes
```python
def mark_error_resolved(error_log_id, resolution_notes):
    spark.sql(f"""
        UPDATE bronze.error_log
        SET resolved = TRUE,
            resolved_timestamp = current_timestamp(),
            resolved_by = current_user(),
            resolution_notes = '{resolution_notes}'
        WHERE error_log_id = '{error_log_id}'
    """)
```

## Error Prevention Best Practices

### 1. Input Validation

- Validate file existence before processing
- Check file size and format
- Verify file permissions
- Validate date ranges and partitions

### 2. Defensive Programming

- Use explicit schemas
- Set default values for optional fields
- Add NULL checks before operations
- Validate assumptions with assertions

### 3. Resource Management

- Set memory limits
- Configure timeout values
- Implement connection pooling
- Monitor resource utilization

### 4. Testing

- Unit test error handling paths
- Test with malformed data
- Simulate system failures
- Validate error logging
- Test recovery procedures

### 5. Monitoring

- Track error rates over time
- Alert on error threshold breaches
- Monitor quarantine queue size
- Review error logs regularly
- Generate error trend reports

## Integration with Data Quality Framework

This error handling strategy integrates with the [Data Quality Framework](data-quality-framework.md) by:

1. **Feeding Quality Metrics**: Error logs contribute to quality scorecards
2. **Triggering Quality Rules**: Validation errors invoke quality rules
3. **Exception Handling**: Routing quality violations through error workflow
4. **Quality Reporting**: Including error statistics in quality reports
5. **Continuous Improvement**: Using error patterns to improve quality rules

## Metrics and Reporting

### Key Metrics

- **Error Rate**: Percentage of records failing validation
- **Quarantine Size**: Number of files/records in quarantine
- **Resolution Time**: Average time to resolve errors
- **Retry Success Rate**: Percentage of retries that succeed
- **Error Type Distribution**: Breakdown by error classification

### Sample Dashboard

```
Source Data Generation Error Dashboard
======================================

Overall Health:
- Total Errors (24h):     45
- Critical Errors:        2
- Error Rate:            0.5%
- Avg Resolution Time:   4.2 hours

Error Breakdown:
- Schema Errors:         12 (27%)
- Validation Errors:     28 (62%)
- Technical Errors:      5 (11%)

Top Error Sources:
1. ERP_SYSTEM/customers: 18 errors
2. ORDER_SYSTEM/orders:  15 errors
3. INVENTORY/products:   12 errors

Quarantine Status:
- Files Quarantined:     8
- Records Quarantined:   1,247
- Pending Resolution:    3
```

## Example Implementation

See [Source Data Generation Examples](../notebooks/source_data/) for reference implementations demonstrating these error handling patterns.

## Related Documentation

- [Data Quality Framework](data-quality-framework.md)
- [Bronze Layer Data Ingestion](../features/bronze/data_ingestion.feature)
- [Source Data Specification](source-data-spec.md)
- [Implementation Roadmap](implementation-roadmap.md)

## Appendix: Error Codes Reference

| Error Code | Description | Severity | Handling |
|------------|-------------|----------|----------|
| SRC-001 | File not found | ERROR | Alert operations, halt job |
| SRC-002 | Schema mismatch | ERROR | Quarantine file, notify steward |
| SRC-003 | NULL required field | ERROR | Quarantine record, log violation |
| SRC-004 | Invalid format | WARNING | Flag for review, continue |
| SRC-005 | Value out of range | WARNING | Apply default or flag |
| SRC-006 | Referential integrity | ERROR | Quarantine record, log violation |
| SRC-007 | Duplicate record | WARNING | Log duplicate, apply dedup rule |
| SRC-008 | Transformation failure | ERROR | Isolate record, log context |
| SRC-009 | High error rate | CRITICAL | Halt pipeline, alert operations |
| SRC-010 | Resource exhaustion | CRITICAL | Retry with backoff, scale resources |

---

**Version**: 1.0  
**Last Updated**: 2026-02-05  
**Owner**: Data Engineering Team
