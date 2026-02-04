Feature: Silver Layer Hub Management
  As a data engineer
  I want to manage Data Vault 2.0 Hubs in the silver layer
  So that I can maintain core business entities with proper business keys

  Background:
    Given the medallion architecture is set up
    And the bronze layer contains source data
    And the silver layer storage is available

  Scenario: Create a new Hub for Customer entity
    Given raw customer data in the bronze layer:
      | customer_id | first_name | last_name | email              |
      | CUST-001    | John       | Doe       | john@example.com   |
      | CUST-002    | Jane       | Smith     | jane@example.com   |
    When I create Hub Customer from the bronze data
    Then the hub_customer table should contain:
      | hub_customer_hashkey | business_key | load_date           | record_source |
      | <hash_of_CUST-001>   | CUST-001     | 2024-01-15T10:30:00 | ERP_SYSTEM    |
      | <hash_of_CUST-002>   | CUST-002     | 2024-01-15T10:30:00 | ERP_SYSTEM    |
    And each hash key should be generated using SHA-256
    And the business_key should be the unique identifier

  Scenario: Avoid duplicate Hub entries
    Given hub_customer already contains:
      | hub_customer_hashkey | business_key | load_date           | record_source |
      | <hash_of_CUST-001>   | CUST-001     | 2024-01-15T10:00:00 | ERP_SYSTEM    |
    And new source data contains:
      | customer_id | first_name | last_name |
      | CUST-001    | John       | Doe       |
      | CUST-003    | Bob        | Johnson   |
    When I load the Hub Customer
    Then only the new business key should be inserted:
      | hub_customer_hashkey | business_key | load_date           | record_source |
      | <hash_of_CUST-003>   | CUST-003     | 2024-01-15T10:30:00 | ERP_SYSTEM    |
    And CUST-001 should not be duplicated

  Scenario: Generate consistent hash keys
    Given a business key "CUST-12345"
    When I generate a hash key for the business key
    Then the hash key should be deterministic
    And generating the hash again should produce the same result
    And the hash should use SHA-256 algorithm

  Scenario: Handle multiple source systems for same entity
    Given customer data from ERP_SYSTEM:
      | customer_id |
      | CUST-001    |
    And customer data from CRM_SYSTEM:
      | customer_id |
      | CUST-002    |
    When I load Hub Customer from both sources
    Then hub_customer should contain:
      | hub_customer_hashkey | business_key | record_source |
      | <hash_of_CUST-001>   | CUST-001     | ERP_SYSTEM    |
      | <hash_of_CUST-002>   | CUST-002     | CRM_SYSTEM    |
    And each record should track its source system

  Scenario: Create Hub for Product entity
    Given raw product data in the bronze layer:
      | product_id | product_name | category   |
      | PROD-001   | Widget A     | Electronics|
      | PROD-002   | Widget B     | Electronics|
    When I create Hub Product from the bronze data
    Then the hub_product table should contain:
      | hub_product_hashkey | business_key | load_date           | record_source |
      | <hash_of_PROD-001>  | PROD-001     | 2024-01-15T10:30:00 | INVENTORY_SYSTEM |
      | <hash_of_PROD-002>  | PROD-002     | 2024-01-15T10:30:00 | INVENTORY_SYSTEM |

  Scenario: Create Hub for Order entity
    Given raw order data in the bronze layer:
      | order_id | order_date |
      | ORD-001  | 2024-01-15 |
      | ORD-002  | 2024-01-16 |
    When I create Hub Order from the bronze data
    Then the hub_order table should contain:
      | hub_order_hashkey   | business_key | load_date           | record_source |
      | <hash_of_ORD-001>   | ORD-001      | 2024-01-15T10:30:00 | ORDER_SYSTEM  |
      | <hash_of_ORD-002>   | ORD-002      | 2024-01-16T10:30:00 | ORDER_SYSTEM  |

  Scenario: Hub loading is idempotent
    Given hub_customer contains business keys:
      | business_key |
      | CUST-001     |
      | CUST-002     |
    When I reload the same source data multiple times
    Then hub_customer should still contain only:
      | business_key |
      | CUST-001     |
      | CUST-002     |
    And no duplicate entries should be created

  Scenario: Handle null business keys
    Given raw customer data with null business key:
      | customer_id | first_name | last_name |
      | NULL        | John       | Doe       |
      | CUST-002    | Jane       | Smith     |
    When I attempt to load Hub Customer
    Then the record with NULL business key should be rejected
    And an error should be logged:
      | error_type    | NULL_BUSINESS_KEY        |
      | record_data   | first_name=John          |
    And CUST-002 should be loaded successfully
