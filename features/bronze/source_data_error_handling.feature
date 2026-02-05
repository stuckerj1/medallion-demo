Feature: Source Data Generation Error Handling
  As a data engineer
  I want robust error handling in source data generation scripts
  So that data quality issues are captured, logged, and can be resolved without data loss

  Background:
    Given the source data generation environment is set up
    And error logging is enabled
    And the quarantine area is configured

  # Technical Errors
  Scenario: Handle file not found error
    Given a source data generation script expects file "customers_20240115.csv"
    When the file does not exist at the expected path
    Then an error should be logged with:
      | field         | value                         |
      | error_type    | FILE_NOT_FOUND                |
      | error_code    | SRC-001                       |
      | severity      | ERROR                         |
      | message       | Source file not found         |
    And the error should include the expected file path
    And an alert should be sent to operations
    And the script should terminate gracefully

  Scenario: Handle network connectivity failure
    Given a source data generation script attempts to read from remote storage
    When a network connectivity error occurs
    Then the script should retry with exponential backoff
    And retry attempts should be logged
    And if max retries exceeded, an error should be logged with:
      | field         | value                              |
      | error_type    | NETWORK_ERROR                      |
      | error_code    | SRC-010                            |
      | severity      | CRITICAL                           |
    And an alert should be sent to operations

  Scenario: Handle memory exhaustion during processing
    Given a source data generation script is processing a large dataset
    When memory resources are exhausted
    Then an error should be logged with:
      | field         | value                         |
      | error_type    | RESOURCE_EXHAUSTION           |
      | error_code    | SRC-010                       |
      | severity      | CRITICAL                      |
    And the error context should include memory metrics
    And an alert should be sent for resource scaling

  # Schema Errors
  Scenario: Detect and handle schema mismatch
    Given a source data generation script expects schema:
      | column_name   | data_type | nullable |
      | CustomerID    | string    | false    |
      | CompanyName   | string    | false    |
      | Email         | string    | true     |
      | CreatedDate   | date      | false    |
    When the source file has schema:
      | column_name   | data_type | nullable |
      | CustomerID    | string    | false    |
      | CompanyName   | string    | false    |
      | Phone         | string    | true     |
      | CreatedDate   | date      | false    |
    Then an error should be logged with:
      | field         | value                         |
      | error_type    | SCHEMA_MISMATCH               |
      | error_code    | SRC-002                       |
      | severity      | ERROR                         |
    And the error details should include:
      | detail_field       | value      |
      | expected_columns   | CustomerID, CompanyName, Email, CreatedDate |
      | actual_columns     | CustomerID, CompanyName, Phone, CreatedDate |
      | missing_columns    | Email      |
      | unexpected_columns | Phone      |
    And the file should be moved to quarantine
    And a notification should be sent to data stewards
    And processing should continue with other files

  Scenario: Handle unexpected additional columns
    Given a source data generation script expects specific columns
    When the source file contains additional unexpected columns
    Then a warning should be logged with:
      | field         | value                         |
      | error_type    | UNEXPECTED_COLUMNS            |
      | severity      | WARNING                       |
    And the unexpected columns should be listed in error details
    And processing should continue with expected columns
    And data stewards should be notified for schema evolution review

  Scenario: Handle data type mismatch
    Given a source data generation script expects "OrderDate" as date type
    When the source file contains "OrderDate" as string type
    And the string values can be parsed to dates
    Then a warning should be logged about type conversion
    And the data should be converted to the expected type
    And processing should continue successfully

  Scenario: Handle incompatible data type mismatch
    Given a source data generation script expects "Quantity" as integer type
    When the source file contains "Quantity" with non-numeric values
    Then an error should be logged with:
      | field         | value                         |
      | error_type    | TYPE_CONVERSION_FAILURE       |
      | error_code    | SRC-008                       |
      | severity      | ERROR                         |
    And the problematic records should be quarantined
    And the error should include sample invalid values
    And processing should continue with valid records

  # Data Quality Errors
  Scenario: Handle NULL values in required fields
    Given a source data generation script requires "CustomerID" to be non-NULL
    When processing records with NULL "CustomerID" values:
      | CustomerID | CompanyName      | Email            |
      | null       | Acme Corp        | acme@example.com |
      | CUST-002   | Beta Inc         | beta@example.com |
      | null       | Gamma LLC        | gamma@example.com|
    Then errors should be logged for each NULL value with:
      | field         | value                         |
      | error_type    | NULL_REQUIRED_FIELD           |
      | error_code    | SRC-003                       |
      | severity      | ERROR                         |
      | field_name    | CustomerID                    |
    And the records with NULL CustomerID should be quarantined
    And valid records should continue processing
    And error summary should show 2 records quarantined

  Scenario: Handle invalid email format
    Given a source data generation script validates email format
    When processing records with invalid emails:
      | CustomerID | Email               | validation_result |
      | CUST-001   | valid@example.com   | valid             |
      | CUST-002   | invalid.email       | invalid           |
      | CUST-003   | another@domain.com  | valid             |
      | CUST-004   | @nodomain.com       | invalid           |
    Then warnings should be logged for invalid emails with:
      | field         | value                         |
      | error_type    | INVALID_FORMAT                |
      | error_code    | SRC-004                       |
      | severity      | WARNING                       |
      | field_name    | Email                         |
    And invalid records should be flagged for review
    And processing should continue with all records
    And data quality report should show 2 format violations

  Scenario: Handle values outside expected range
    Given a source data generation script validates "Quantity" range (0-10000)
    When processing records with out-of-range values:
      | OrderID  | ProductID | Quantity | UnitPrice |
      | ORD-001  | PROD-001  | 50       | 25.00     |
      | ORD-002  | PROD-002  | -5       | 30.00     |
      | ORD-003  | PROD-003  | 15000    | 100.00    |
    Then warnings should be logged for out-of-range values with:
      | field         | value                         |
      | error_type    | VALUE_OUT_OF_RANGE            |
      | error_code    | SRC-005                       |
      | severity      | WARNING                       |
      | field_name    | Quantity                      |
    And the error details should include actual values and expected range
    And records should be flagged but not quarantined
    And data stewards should review for potential data issues

  Scenario: Detect duplicate records
    Given a source data generation script checks for duplicates on "CustomerID"
    When processing data with duplicate business keys:
      | CustomerID | CompanyName      | Email            |
      | CUST-001   | Acme Corp        | acme@example.com |
      | CUST-002   | Beta Inc         | beta@example.com |
      | CUST-001   | Acme Corporation | acme2@example.com|
    Then a warning should be logged with:
      | field         | value                         |
      | error_type    | DUPLICATE_RECORD              |
      | error_code    | SRC-007                       |
      | severity      | WARNING                       |
      | field_name    | CustomerID                    |
      | field_value   | CUST-001                      |
    And the deduplication rule should be applied
    And the duplicate handling should be logged
    And data quality metrics should record duplicate count

  # Transformation Errors
  Scenario: Handle transformation failure in derived columns
    Given a source data generation script calculates "TotalAmount" as "Quantity * UnitPrice"
    When processing records where calculation fails:
      | OrderID  | ProductID | Quantity | UnitPrice | calculation_result |
      | ORD-001  | PROD-001  | 10       | 25.00     | success            |
      | ORD-002  | PROD-002  | null     | 30.00     | failure            |
      | ORD-003  | PROD-003  | 5        | null      | failure            |
    Then errors should be logged for transformation failures with:
      | field         | value                         |
      | error_type    | TRANSFORMATION_FAILURE        |
      | error_code    | SRC-008                       |
      | severity      | ERROR                         |
    And the error context should include input values and operation
    And failed records should be quarantined
    And successful transformations should continue
    And transformation success rate should be reported

  Scenario: Handle date parsing errors
    Given a source data generation script parses "OrderDate" in format "YYYY-MM-DD"
    When processing records with invalid date formats:
      | OrderID  | OrderDate     | parse_result |
      | ORD-001  | 2024-01-15    | success      |
      | ORD-002  | 15/01/2024    | failure      |
      | ORD-003  | 2024-13-45    | failure      |
      | ORD-004  | invalid-date  | failure      |
    Then errors should be logged for each parsing failure
    And the error should include the invalid value and expected format
    And records with invalid dates should be quarantined
    And valid records should be processed

  Scenario: Handle division by zero in calculations
    Given a source data generation script calculates "AveragePrice" as "TotalAmount / Quantity"
    When processing a record where "Quantity" is zero
    Then an error should be logged with:
      | field         | value                         |
      | error_type    | TRANSFORMATION_FAILURE        |
      | error_code    | SRC-008                       |
      | severity      | ERROR                         |
      | message       | Division by zero in AveragePrice calculation |
    And the error details should include the input values
    And the record should be quarantined
    And a default value or NULL should not be silently applied

  # Business Logic Errors
  Scenario: Validate business rule - order date before ship date
    Given a source data generation script validates "OrderDate <= ShippedDate"
    When processing records where order date is after shipped date:
      | OrderID  | OrderDate  | ShippedDate | is_valid |
      | ORD-001  | 2024-01-15 | 2024-01-20  | true     |
      | ORD-002  | 2024-01-20 | 2024-01-15  | false    |
    Then a business logic error should be logged with:
      | field         | value                                |
      | error_type    | BUSINESS_RULE_VIOLATION              |
      | severity      | ERROR                                |
      | rule_name     | order_date_before_ship_date          |
    And the violating record should be flagged for business review
    And business exception workflow should be triggered

  Scenario: Validate referential integrity
    Given a source data generation script validates that "SupplierID" exists in suppliers
    When processing products with non-existent supplier references:
      | ProductID | ProductName | SupplierID | exists_in_suppliers |
      | PROD-001  | Widget      | SUP-001    | true                |
      | PROD-002  | Gadget      | SUP-999    | false               |
    Then an error should be logged with:
      | field         | value                                |
      | error_type    | REFERENTIAL_INTEGRITY_VIOLATION      |
      | error_code    | SRC-006                              |
      | severity      | ERROR                                |
      | field_name    | SupplierID                           |
      | field_value   | SUP-999                              |
    And the record should be quarantined
    And data stewards should be notified of orphaned records

  # Circuit Breakers and Guardrails
  Scenario: Trigger circuit breaker on high error rate
    Given a source data generation script has error threshold of 10%
    When processing a batch where 15% of records fail validation
    Then an error should be logged with:
      | field         | value                         |
      | error_type    | HIGH_ERROR_RATE               |
      | error_code    | SRC-009                       |
      | severity      | CRITICAL                      |
    And the error rate should be included in the message
    And the pipeline should be halted
    And operations team should be alerted immediately
    And investigation should be triggered

  Scenario: Validate record count threshold
    Given a source data generation script expects minimum 100 records
    When the source file contains only 45 records
    Then an error should be logged with:
      | field         | value                         |
      | error_type    | INSUFFICIENT_RECORDS          |
      | severity      | ERROR                         |
    And the actual and expected counts should be in error details
    And the pipeline should be halted
    And data stewards should be notified

  Scenario: Detect unexpected high record volume
    Given a source data generation script expects maximum 100000 records
    When the source file contains 500000 records
    Then a warning should be logged with:
      | field         | value                         |
      | error_type    | EXCESSIVE_RECORDS             |
      | severity      | WARNING                       |
    And the actual and expected counts should be logged
    And data stewards should be notified for investigation
    And processing should continue unless configured otherwise

  # Error Recovery and Reprocessing
  Scenario: Successfully reprocess quarantined file after schema fix
    Given a file was quarantined due to schema mismatch
    And the schema issue has been resolved
    When the quarantined file is reprocessed
    Then the file should be loaded successfully
    And the original error should be marked as resolved
    And resolution notes should be recorded
    And the file should be removed from quarantine

  Scenario: Reprocess quarantined records after data correction
    Given records were quarantined due to NULL required fields
    And the source has provided corrected data
    When the corrected records are resubmitted
    Then the records should pass validation
    And the records should be loaded to bronze layer
    And the error log entries should be marked as resolved
    And the resolution timestamp and user should be recorded

  Scenario: Track retry attempts in dead letter queue
    Given a record failed processing due to transient error
    And the record was sent to dead letter queue
    When automatic retry is triggered
    And the retry succeeds
    Then the record should be processed successfully
    And the DLQ entry should be marked as resolved
    And retry count and success should be logged

  Scenario: Escalate after max retries exceeded
    Given a record in dead letter queue has failed 3 times
    When another retry attempt is made
    Then no further retry should be attempted
    And the record should be marked for manual intervention
    And data stewards should be notified
    And the full retry history should be available

  # Monitoring and Reporting
  Scenario: Generate error summary report
    Given source data generation has completed with errors
    When an error summary is requested
    Then the report should include:
      | metric                  | value |
      | total_records_processed | 10000 |
      | successful_records      | 9850  |
      | failed_records          | 150   |
      | error_rate              | 1.5%  |
      | files_quarantined       | 2     |
      | records_quarantined     | 150   |
    And errors should be grouped by type
    And top error sources should be identified
    And trend comparison should be provided

  Scenario: Alert on error threshold breach
    Given error rate monitoring is configured with 5% threshold
    When error rate exceeds the threshold
    Then an alert should be triggered
    And the alert should include:
      | field            | value                    |
      | alert_type       | ERROR_THRESHOLD_BREACH   |
      | current_rate     | 7.2%                     |
      | threshold        | 5.0%                     |
      | affected_source  | ERP_SYSTEM/customers     |
    And operations team should be notified
    And error dashboard should be updated

  Scenario: Track error resolution metrics
    Given multiple errors have been logged and resolved over time
    When error resolution metrics are requested
    Then the report should include:
      | metric                     | value      |
      | average_resolution_time    | 4.2 hours  |
      | errors_resolved_24h        | 45         |
      | errors_pending             | 12         |
      | resolution_rate            | 78.9%      |
    And resolution time trends should be shown
    And common resolution patterns should be identified

  # Integration with Data Quality Framework
  Scenario: Route validation errors to data quality workflow
    Given data quality rules are configured
    When source data generation detects validation errors
    Then errors should be routed to quality exception workflow
    And quality scorecard should be updated
    And error details should feed into quality metrics
    And data quality dashboard should reflect errors

  Scenario: Generate data quality scorecard from errors
    Given source data generation has completed
    When quality scorecard is generated
    Then it should include error-based metrics:
      | dimension    | score | status | issues                    |
      | Completeness | 98.5% | PASS   | 15 NULL required fields   |
      | Accuracy     | 97.2% | WARN   | 28 invalid formats        |
      | Validity     | 99.1% | PASS   | 9 out-of-range values     |
    And overall quality score should be calculated
    And quality trends should be tracked over time
