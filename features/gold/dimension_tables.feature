Feature: Gold Layer Dimension Tables
  As a data engineer
  I want to create conformed dimension tables in the gold layer
  So that I can provide business-ready data for analytics and reporting

  Background:
    Given the medallion architecture is set up
    And the silver layer contains Hubs, Links, and Satellites with current data

  Scenario: Create Customer Dimension from silver layer
    Given hub_customer and sat_customer_details contain:
      | hub_customer_hashkey | business_key | first_name | last_name | email              | is_current |
      | ABC123               | CUST-001     | John       | Doe       | john@example.com   | true       |
      | DEF456               | CUST-002     | Jane       | Smith     | jane@example.com   | true       |
    When I create dim_customer in the gold layer
    Then dim_customer should contain:
      | customer_key | customer_id | first_name | last_name | email              | effective_date      | current_flag |
      | 1            | CUST-001    | John       | Doe       | john@example.com   | 2024-01-15T10:30:00 | Y            |
      | 2            | CUST-002    | Jane       | Smith     | jane@example.com   | 2024-01-15T10:30:00 | Y            |
    And customer_key should be a surrogate key
    And only current records from satellites should be included

  Scenario: Create Product Dimension with hierarchies
    Given hub_product and sat_product_details contain:
      | hub_product_hashkey | business_key | product_name | category    | subcategory | brand   | is_current |
      | STU901              | PROD-001     | Widget A     | Electronics | Gadgets     | BrandX  | true       |
      | VWX234              | PROD-002     | Widget B     | Electronics | Gadgets     | BrandY  | true       |
    When I create dim_product in the gold layer
    Then dim_product should contain product hierarchies:
      | product_key | product_id | product_name | category    | subcategory | brand  | category_key | subcategory_key | brand_key |
      | 1           | PROD-001   | Widget A     | Electronics | Gadgets     | BrandX | 100          | 110             | 200       |
      | 2           | PROD-002   | Widget B     | Electronics | Gadgets     | BrandY | 100          | 110             | 201       |
    And dimension hierarchies should support drill-down analysis

  Scenario: Create Date Dimension
    Given a date range from "2024-01-01" to "2024-12-31"
    When I create dim_date in the gold layer
    Then dim_date should contain one row per day:
      | date_key | full_date  | year | quarter | month | day | day_of_week | is_weekend | is_holiday |
      | 20240101 | 2024-01-01 | 2024 | 1       | 1     | 1   | Monday      | N          | Y          |
      | 20240102 | 2024-01-02 | 2024 | 1       | 1     | 2   | Tuesday     | N          | N          |
    And the dimension should include calendar attributes
    And fiscal calendar attributes should be included

  Scenario: Create Geography Dimension
    Given customer and order location data from silver layer
    When I create dim_geography in the gold layer
    Then dim_geography should contain:
      | geography_key | country | state      | city        | postal_code | region    |
      | 1             | USA     | California | Los Angeles | 90001       | West      |
      | 2             | USA     | New York   | New York    | 10001       | Northeast |
    And the dimension should support geographic hierarchies:
      | level_1 | level_2 | level_3 | level_4     |
      | Country | Region  | State   | City        |

  Scenario: Handle Type 2 SCD in dimensions
    Given sat_customer_details has historical changes:
      | hub_customer_hashkey | load_date           | load_end_date       | is_current | email              |
      | ABC123               | 2024-01-15T10:30:00 | 2024-02-01T14:00:00 | false      | john@old.com       |
      | ABC123               | 2024-02-01T14:00:00 | NULL                | true       | john@new.com       |
    When I create dim_customer with Type 2 SCD
    Then dim_customer should contain both versions:
      | customer_key | customer_id | email            | effective_date      | expiration_date     | current_flag |
      | 1            | CUST-001    | john@old.com     | 2024-01-15T10:30:00 | 2024-02-01T14:00:00 | N            |
      | 2            | CUST-001    | john@new.com     | 2024-02-01T14:00:00 | 9999-12-31T23:59:59 | Y            |
    And the current version should have expiration_date as "9999-12-31"

  Scenario: Denormalize dimension with multiple satellites
    Given customer data from multiple satellites:
      | satellite                | attributes                        |
      | sat_customer_details     | first_name, last_name, email      |
      | sat_customer_address     | street, city, state, postal_code  |
      | sat_customer_preferences | marketing_consent, language       |
    When I create dim_customer
    Then all attributes should be denormalized into one row:
      | customer_id | first_name | last_name | email | street | city | state | postal_code | marketing_consent | language |
      | CUST-001    | John       | Doe       | ...   | 123 St | LA   | CA    | 90001       | true              | EN       |

  Scenario: Create slowly changing dimension with surrogate keys
    Given customer "CUST-001" has address change history
    When I query dim_customer for all versions
    Then each version should have unique surrogate key:
      | customer_key | customer_id | effective_date      | current_flag |
      | 1            | CUST-001    | 2024-01-15T10:30:00 | N            |
      | 2            | CUST-001    | 2024-02-01T14:00:00 | Y            |
    And facts should reference customer_key, not customer_id

  Scenario: Add derived attributes to dimension
    Given customer data with birth_date
    When I create dim_customer
    Then the dimension should include derived attributes:
      | customer_id | birth_date | age | age_group   |
      | CUST-001    | 1990-01-15 | 34  | 30-39       |
      | CUST-002    | 1985-06-20 | 38  | 30-39       |
    And derived attributes should be calculated during load

  Scenario: Handle unknown and not applicable dimension members
    Given order data with missing customer information
    When I create dim_customer
    Then special dimension members should exist:
      | customer_key | customer_id | first_name | last_name | description        |
      | -1           | UNKNOWN     | Unknown    | Unknown   | Unknown customer   |
      | 0            | N/A         | N/A        | N/A       | Not applicable     |
    And fact tables can reference these for missing data

  Scenario: Optimize dimension for query performance
    Given dim_customer is created
    When I analyze the dimension structure
    Then the dimension should be denormalized for performance
    And indexes should be created on:
      | column       |
      | customer_key |
      | customer_id  |
    And the dimension should support star schema queries efficiently
