Feature: Bronze Layer Data Ingestion
  As a data engineer
  I want to ingest raw data into the bronze layer
  So that I can preserve the original source data for lineage and reprocessing

  Background:
    Given the medallion architecture is set up
    And the bronze layer storage is available

  Scenario: Ingest raw CSV data from source system
    Given a CSV file "customers.csv" from source system "ERP_SYSTEM"
    And the file contains the following data:
      | customer_id | first_name | last_name | email              | created_date |
      | CUST-001    | John       | Doe       | john@example.com   | 2024-01-15   |
      | CUST-002    | Jane       | Smith     | jane@example.com   | 2024-01-16   |
    When I ingest the file into the bronze layer
    Then the data should be stored in "bronze/ERP_SYSTEM/customers/2024/01/15/10/data.parquet"
    And the ingestion metadata should include:
      | field              | value       |
      | source_system      | ERP_SYSTEM  |
      | ingestion_timestamp| 2024-01-15T10:30:00 |
      | record_count       | 2           |
      | file_name          | customers.csv |

  Scenario: Ingest raw JSON data from source system
    Given a JSON file "orders.json" from source system "ORDER_SYSTEM"
    And the file contains:
      """
      [
        {"order_id": "ORD-001", "customer_id": "CUST-001", "amount": 150.00, "order_date": "2024-01-15"},
        {"order_id": "ORD-002", "customer_id": "CUST-002", "amount": 275.50, "order_date": "2024-01-16"}
      ]
      """
    When I ingest the file into the bronze layer
    Then the data should be stored in "bronze/ORDER_SYSTEM/orders/2024/01/15/10/data.parquet"
    And the original JSON structure should be preserved
    And the ingestion metadata should be recorded

  Scenario: Preserve data lineage in bronze layer
    Given a data file "products.csv" from source system "INVENTORY_SYSTEM"
    When I ingest the file into the bronze layer
    Then the stored data should include lineage metadata:
      | field               | value            |
      | source_file_path    | /data/products.csv |
      | source_system       | INVENTORY_SYSTEM |
      | ingestion_timestamp | 2024-01-15T10:30:00 |
      | ingestion_job_id    | job-12345        |

  Scenario: Handle duplicate file ingestion
    Given a file "customers.csv" has already been ingested at "2024-01-15T10:00:00"
    When I attempt to ingest the same file again at "2024-01-15T10:30:00"
    Then both versions should be stored in the bronze layer
    And each version should have a unique timestamp
    And the data should remain immutable

  Scenario: Ingest data with partitioning by date
    Given multiple data files from source system "SALES_SYSTEM"
    When I ingest files for dates:
      | file_date  |
      | 2024-01-15 |
      | 2024-01-16 |
      | 2024-01-17 |
    Then the data should be partitioned by date:
      | partition_path                              |
      | bronze/SALES_SYSTEM/sales/2024/01/15/       |
      | bronze/SALES_SYSTEM/sales/2024/01/16/       |
      | bronze/SALES_SYSTEM/sales/2024/01/17/       |

  Scenario: Capture ingestion errors without data loss
    Given a malformed CSV file "bad_data.csv"
    When I attempt to ingest the file into the bronze layer
    Then the file should still be stored in a quarantine area
    And an error log should be created with:
      | field         | value                    |
      | error_type    | MALFORMED_CSV            |
      | file_name     | bad_data.csv             |
      | error_message | Unable to parse row 5    |
      | timestamp     | 2024-01-15T10:30:00      |

  Scenario: Support full reprocessing from bronze
    Given data has been ingested into bronze layer for date range "2024-01-01 to 2024-01-31"
    When I request a full reprocessing for that date range
    Then all original data should be available
    And the data should be in its original format
    And no data transformations should have been applied

  Scenario: Store multiple source systems independently
    Given data from multiple source systems:
      | source_system    | table_name |
      | ERP_SYSTEM       | customers  |
      | CRM_SYSTEM       | customers  |
      | ORDER_SYSTEM     | orders     |
    When I ingest data from all systems
    Then each source should be stored in separate paths:
      | path                                    |
      | bronze/ERP_SYSTEM/customers/            |
      | bronze/CRM_SYSTEM/customers/            |
      | bronze/ORDER_SYSTEM/orders/             |
    And there should be no mixing of data between sources

  Scenario: Record ingestion metrics
    Given a data file with 10,000 records
    When I ingest the file into the bronze layer
    Then the ingestion metrics should be recorded:
      | metric              | value   |
      | records_ingested    | 10000   |
      | file_size_mb        | 2.5     |
      | ingestion_duration_sec | 15   |
      | status              | SUCCESS |
