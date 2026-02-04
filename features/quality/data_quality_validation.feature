Feature: Data Quality Validation
  As a data engineer
  I want to validate data quality at each layer
  So that I can ensure data accuracy, completeness, and consistency

  Background:
    Given the medallion architecture is set up
    And data quality rules are configured

  Scenario: Validate bronze layer file completeness
    Given a source file "customers.csv" is ready for ingestion
    When I perform bronze layer quality checks
    Then the following validations should pass:
      | check_name          | status | details                    |
      | file_exists         | PASS   | File found                 |
      | file_readable       | PASS   | File can be opened         |
      | file_not_empty      | PASS   | File size > 0 bytes        |
      | expected_columns    | PASS   | All required columns present |
      | record_count        | PASS   | 1000 records found         |

  Scenario: Detect and quarantine malformed files
    Given a malformed CSV file with parsing errors
    When I attempt to ingest into bronze layer
    Then the file should be moved to quarantine
    And an exception should be logged:
      | field            | value                        |
      | exception_type   | MALFORMED_FILE               |
      | severity         | ERROR                        |
      | layer            | bronze                       |
      | action_taken     | Moved to quarantine          |

  Scenario: Validate business keys in silver layer
    Given customer data from bronze layer:
      | customer_id | first_name | last_name |
      | CUST-001    | John       | Doe       |
      | NULL        | Jane       | Smith     |
      | CUST-003    | Bob        | Johnson   |
    When I load Hub Customer with quality validation
    Then the quality check results should be:
      | record          | check_name          | status | action     |
      | CUST-001        | business_key_valid  | PASS   | LOADED     |
      | NULL            | business_key_valid  | FAIL   | REJECTED   |
      | CUST-003        | business_key_valid  | PASS   | LOADED     |
    And only valid records should be loaded into the Hub

  Scenario: Validate email format in silver layer
    Given customer data with email addresses:
      | customer_id | email                |
      | CUST-001    | john@example.com     |
      | CUST-002    | invalid-email        |
      | CUST-003    | jane@company.co.uk   |
    When I load sat_customer_details with email validation
    Then the validation results should be:
      | customer_id | email_format_valid | status | action     |
      | CUST-001    | true               | PASS   | LOADED     |
      | CUST-002    | false              | FAIL   | REJECTED   |
      | CUST-003    | true               | PASS   | LOADED     |
    And invalid emails should be logged for correction

  Scenario: Check referential integrity for Links
    Given link_customer_order data:
      | customer_id | order_id |
      | CUST-001    | ORD-001  |
      | CUST-999    | ORD-002  |
    And hub_customer contains: CUST-001
    And hub_customer does not contain: CUST-999
    And hub_order contains: ORD-001, ORD-002
    When I load link_customer_order with referential integrity checks
    Then the results should be:
      | customer_id | order_id | referential_integrity | status | action     |
      | CUST-001    | ORD-001  | VALID                 | PASS   | LOADED     |
      | CUST-999    | ORD-002  | INVALID               | FAIL   | REJECTED   |
    And orphaned links should not be created

  Scenario: Validate data completeness in silver layer
    Given customer data with required fields:
      | customer_id | first_name | last_name | email              | phone    |
      | CUST-001    | John       | Doe       | john@example.com   | 555-0001 |
      | CUST-002    | Jane       | NULL      | jane@example.com   | 555-0002 |
      | CUST-003    | Bob        | Johnson   | bob@example.com    | NULL     |
    And the completeness rules specify:
      | field      | required |
      | first_name | true     |
      | last_name  | true     |
      | email      | true     |
      | phone      | false    |
    When I validate data completeness
    Then the results should be:
      | customer_id | completeness_check | status | missing_fields |
      | CUST-001    | 100%               | PASS   | None           |
      | CUST-002    | INCOMPLETE         | FAIL   | last_name      |
      | CUST-003    | 100%               | PASS   | None           |

  Scenario: Calculate data quality score for an entity
    Given 100 customer records are loaded
    And quality validation results are:
      | metric              | count |
      | total_records       | 100   |
      | valid_business_keys | 100   |
      | valid_emails        | 95    |
      | complete_addresses  | 90    |
      | valid_phone_numbers | 85    |
    When I calculate the data quality score
    Then the quality scorecard should show:
      | dimension    | score | status |
      | Uniqueness   | 100%  | PASS   |
      | Validity     | 95%   | PASS   |
      | Completeness | 90%   | PASS   |
      | Overall      | 95%   | PASS   |

  Scenario: Track quality metrics over time
    Given historical quality scores:
      | date       | entity   | overall_score |
      | 2024-01-01 | customer | 92%           |
      | 2024-01-08 | customer | 94%           |
      | 2024-01-15 | customer | 96%           |
    When I analyze the quality trend
    Then the trend should show "IMPROVING"
    And the improvement rate should be 4% over 2 weeks

  Scenario: Validate fact table referential integrity
    Given fact_sales data:
      | sale_id | customer_key | product_key | date_key |
      | 1       | 1            | 1           | 20240115 |
      | 2       | 999          | 2           | 20240116 |
    And dim_customer contains customer_key: 1
    And dim_customer does not contain customer_key: 999
    And dim_product contains product_key: 1, 2
    And dim_date contains date_key: 20240115, 20240116
    When I validate fact_sales referential integrity
    Then the results should be:
      | sale_id | referential_integrity | invalid_keys | action                  |
      | 1       | VALID                 | None         | LOADED                  |
      | 2       | INVALID               | customer_key | MAP_TO_UNKNOWN_CUSTOMER |

  Scenario: Validate aggregation accuracy
    Given fact_sales detail records:
      | sale_id | customer_key | date_key | sales_amount |
      | 1       | 1            | 20240115 | 100.00       |
      | 2       | 1            | 20240115 | 150.00       |
      | 3       | 2            | 20240116 | 200.00       |
    When I create fact_sales_daily aggregate
    Then the aggregate should contain:
      | customer_key | date_key | total_sales | record_count |
      | 1            | 20240115 | 250.00      | 2            |
      | 2            | 20240116 | 200.00      | 1            |
    And I validate aggregation accuracy
    Then all aggregate values should match detail sums

  Scenario: Monitor data freshness
    Given the expected data load time is "every 1 hour"
    And the last successful load was at "2024-01-15T10:00:00"
    And the current time is "2024-01-15T12:30:00"
    When I check data freshness
    Then the freshness check should return:
      | metric              | value               | status  | details                    |
      | last_load_time      | 2024-01-15T10:00:00 | WARNING | Data is 2.5 hours old      |
      | expected_interval   | 1 hour              | -       | -                          |
      | time_since_load     | 2.5 hours           | WARNING | Exceeds SLA by 1.5 hours   |
    And an alert should be sent for SLA breach

  Scenario: Exception workflow for quality failures
    Given customer records fail quality validation:
      | customer_id | failed_rule          | severity |
      | CUST-002    | invalid_email_format | WARNING  |
      | CUST-005    | null_business_key    | ERROR    |
    When quality exceptions are created
    Then exception_log should contain:
      | exception_id | customer_id | rule_id              | severity | status | assigned_to      |
      | EX-001       | CUST-002    | EMAIL_FORMAT_001     | WARNING  | OPEN   | data_steward_1   |
      | EX-002       | CUST-005    | BUSINESS_KEY_001     | ERROR    | OPEN   | data_engineer_1  |
    And notifications should be sent to assigned owners

  Scenario: Quality gate prevents bad data promotion
    Given silver layer data has quality score of 85%
    And the minimum quality threshold for gold layer is 95%
    When I attempt to promote data to gold layer
    Then the promotion should be blocked
    And a quality gate failure should be logged:
      | field            | value                                      |
      | layer            | gold                                       |
      | action           | PROMOTION_BLOCKED                          |
      | reason           | Quality score below threshold              |
      | current_score    | 85%                                        |
      | required_score   | 95%                                        |
      | blocking_issues  | 15 invalid records in customer satellite   |
