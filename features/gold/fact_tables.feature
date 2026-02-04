Feature: Gold Layer Fact Tables
  As a data engineer
  I want to create fact tables in the gold layer
  So that I can provide aggregated, business-ready metrics for analytics

  Background:
    Given the medallion architecture is set up
    And the gold layer dimensions are created:
      | dimension     |
      | dim_customer  |
      | dim_product   |
      | dim_date      |
      | dim_geography |

  Scenario: Create Sales Fact Table
    Given silver layer contains:
      | hub_order | link_customer_order | link_order_product | sat_order_details |
    And gold layer dimensions map to silver hubs:
      | dimension    | hub          | business_key |
      | dim_customer | hub_customer | CUST-001     |
      | dim_product  | hub_product  | PROD-001     |
    When I create fact_sales from silver layer
    Then fact_sales should contain:
      | sale_key | customer_key | product_key | date_key | geography_key | order_id | quantity | unit_price | discount | sales_amount | cost_amount | profit_amount |
      | 1        | 1            | 1           | 20240115 | 1            | ORD-001  | 2        | 50.00      | 5.00     | 95.00        | 60.00       | 35.00         |
      | 2        | 2            | 2           | 20240116 | 2            | ORD-002  | 1        | 100.00     | 0.00     | 100.00       | 70.00       | 30.00         |
    And all dimension keys should be surrogate keys
    And measures should be additive

  Scenario: Calculate derived measures in fact table
    Given raw order data with quantity and unit_price
    When I create fact_sales
    Then derived measures should be calculated:
      | order_id | quantity | unit_price | discount | sales_amount         | discount_amount      | net_amount           |
      | ORD-001  | 2        | 50.00      | 5.00     | 100.00 (2*50)        | 5.00                 | 95.00                |
      | ORD-002  | 3        | 75.00      | 10.00    | 225.00 (3*75)        | 22.50 (225*0.10)     | 202.50               |
    And calculations should follow business rules

  Scenario: Create Inventory Fact Table (periodic snapshot)
    Given inventory levels are tracked daily
    When I create fact_inventory as a periodic snapshot
    Then fact_inventory should contain one row per product per day:
      | snapshot_date | product_key | warehouse_key | quantity_on_hand | quantity_allocated | quantity_available | reorder_point |
      | 2024-01-15    | 1           | 1             | 100              | 20                 | 80                 | 50            |
      | 2024-01-16    | 1           | 1             | 95               | 25                 | 70                 | 50            |
    And snapshots should be taken at consistent intervals

  Scenario: Create Customer Activity Fact Table (accumulating snapshot)
    Given customer order lifecycle from silver layer
    When I create fact_customer_activity as accumulating snapshot
    Then the fact should track milestones:
      | activity_key | customer_key | order_date_key | ship_date_key | delivery_date_key | order_to_ship_days | ship_to_delivery_days | order_status |
      | 1            | 1            | 20240115       | 20240117      | 20240120          | 2                  | 3                     | DELIVERED    |
      | 2            | 2            | 20240116       | NULL          | NULL              | NULL               | NULL                  | PENDING      |
    And NULL date keys should indicate milestones not yet reached

  Scenario: Handle fact table granularity
    Given order line items at different levels:
      | order_id | product_id | quantity |
      | ORD-001  | PROD-001   | 2        |
      | ORD-001  | PROD-002   | 1        |
    When I create fact_sales at line item grain
    Then each line item should be a separate fact record:
      | sale_key | order_id | product_key | quantity |
      | 1        | ORD-001  | 1           | 2        |
      | 2        | ORD-001  | 2           | 1        |
    And the grain should be clearly documented

  Scenario: Create fact table with multiple date dimensions (role-playing)
    Given orders have multiple dates:
      | order_id | order_date | ship_date  | delivery_date |
      | ORD-001  | 2024-01-15 | 2024-01-17 | 2024-01-20    |
    When I create fact_sales
    Then multiple date dimensions should be referenced:
      | sale_key | order_date_key | ship_date_key | delivery_date_key |
      | 1        | 20240115       | 20240117      | 20240120          |
    And each role should be clearly named

  Scenario: Handle degenerate dimensions in fact table
    Given transaction data with transaction number
    When I create fact_sales
    Then the transaction number should be stored as degenerate dimension:
      | sale_key | customer_key | product_key | transaction_number | sales_amount |
      | 1        | 1            | 1           | TXN-12345          | 95.00        |
    And no separate dimension table should be needed for transaction_number

  Scenario: Aggregate fact table for performance
    Given fact_sales contains daily grain data
    When I create fact_sales_monthly as aggregate fact
    Then the monthly aggregate should contain:
      | month_key | customer_key | product_key | total_quantity | total_sales | avg_unit_price | transaction_count |
      | 202401    | 1            | 1           | 50             | 2500.00     | 50.00          | 25                |
    And aggregates should improve query performance for summary reports

  Scenario: Handle late-arriving facts
    Given fact_sales contains orders up to 2024-01-31
    And a late order arrives for 2024-01-15
    When I load the late-arriving fact
    Then the fact should be inserted with the correct date_key:
      | sale_key | order_id | date_key | sales_amount |
      | 999      | ORD-999  | 20240115 | 150.00       |
    And historical aggregates should be recalculated if necessary

  Scenario: Handle fact updates (fixing errors)
    Given fact_sales contains:
      | sale_key | order_id | sales_amount |
      | 1        | ORD-001  | 95.00        |
    And an error is discovered in the sales_amount
    When I correct the fact record
    Then the fact should be updated:
      | sale_key | order_id | sales_amount | updated_date |
      | 1        | ORD-001  | 105.00       | 2024-02-01   |
    And an audit trail should be maintained

  Scenario: Handle fact deletes (order cancellations)
    Given fact_sales contains:
      | sale_key | order_id | sales_amount |
      | 1        | ORD-001  | 95.00        |
    And order ORD-001 is cancelled
    When I process the cancellation
    Then the fact record should be logically deleted:
      | sale_key | order_id | sales_amount | is_deleted | deleted_date |
      | 1        | ORD-001  | 95.00        | true       | 2024-02-01   |
    And queries should exclude deleted facts by default

  Scenario: Create factless fact table for events
    Given customer browsing events without transactions
    When I create fact_customer_browsing
    Then the factless fact should track events:
      | event_key | customer_key | product_key | date_key | time_key | event_type |
      | 1         | 1            | 1           | 20240115 | 143000   | VIEW       |
      | 2         | 1            | 2           | 20240115 | 143500   | VIEW       |
    And counts can be derived from the presence of rows

  Scenario: Validate fact table referential integrity
    Given fact_sales references dimensions:
      | customer_key | product_key | date_key |
      | 1            | 1           | 20240115 |
    When I validate referential integrity
    Then all dimension keys should exist in their dimension tables
    And orphaned facts should be identified and handled:
      | sale_key | customer_key | status    | action         |
      | 999      | 999          | ORPHANED  | Map to UNKNOWN |
