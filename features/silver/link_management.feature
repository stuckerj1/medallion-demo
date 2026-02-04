Feature: Silver Layer Link Management
  As a data engineer
  I want to manage Data Vault 2.0 Links in the silver layer
  So that I can represent relationships between business entities

  Background:
    Given the medallion architecture is set up
    And hub_customer contains:
      | hub_customer_hashkey | business_key |
      | ABC123               | CUST-001     |
      | DEF456               | CUST-002     |
    And hub_order contains:
      | hub_order_hashkey    | business_key |
      | MNO345               | ORD-001      |
      | PQR678               | ORD-002      |

  Scenario: Create Link between Customer and Order
    Given order data from source:
      | order_id | customer_id | order_date |
      | ORD-001  | CUST-001    | 2024-01-15 |
      | ORD-002  | CUST-002    | 2024-01-16 |
    When I create link_customer_order
    Then the link table should contain:
      | link_hashkey        | hub_customer_hashkey | hub_order_hashkey | load_date           | record_source |
      | <hash_link_001>     | ABC123               | MNO345            | 2024-01-15T10:30:00 | ORDER_SYSTEM  |
      | <hash_link_002>     | DEF456               | PQR678            | 2024-01-16T10:30:00 | ORDER_SYSTEM  |
    And each link_hashkey should be derived from the Hub hash keys

  Scenario: Generate Link hash key from Hub keys
    Given hub_customer_hashkey "ABC123" for CUST-001
    And hub_order_hashkey "MNO345" for ORD-001
    When I generate link_hashkey for the relationship
    Then the link_hashkey should be SHA-256 hash of "ABC123||MNO345"
    And the hash generation should be deterministic
    And regenerating should produce the same link_hashkey

  Scenario: Avoid duplicate Link entries
    Given link_customer_order already contains:
      | link_hashkey        | hub_customer_hashkey | hub_order_hashkey | load_date           |
      | <hash_link_001>     | ABC123               | MNO345            | 2024-01-15T10:00:00 |
    And new order data contains the same relationship:
      | order_id | customer_id |
      | ORD-001  | CUST-001    |
    When I load link_customer_order
    Then no duplicate should be created
    And the link count should remain 1

  Scenario: Create multi-way Link (3-way)
    Given hub_order contains:
      | hub_order_hashkey | business_key |
      | MNO345            | ORD-001      |
    And hub_product contains:
      | hub_product_hashkey | business_key |
      | STU901              | PROD-001     |
      | VWX234              | PROD-002     |
    And hub_warehouse contains:
      | hub_warehouse_hashkey | business_key |
      | YZA567                | WH-001       |
    And order fulfillment data:
      | order_id | product_id | warehouse_id |
      | ORD-001  | PROD-001   | WH-001       |
    When I create link_order_product_warehouse
    Then the link should contain:
      | link_hashkey    | hub_order_hashkey | hub_product_hashkey | hub_warehouse_hashkey | load_date           |
      | <hash_3way>     | MNO345            | STU901              | YZA567                | 2024-01-15T10:30:00 |

  Scenario: Link with Satellite for transactional attributes
    Given link_customer_order exists:
      | link_hashkey        | hub_customer_hashkey | hub_order_hashkey |
      | <hash_link_001>     | ABC123               | MNO345            |
    And order has transactional attributes:
      | order_id | customer_id | order_amount | order_status |
      | ORD-001  | CUST-001    | 150.00       | PENDING      |
    When I create sat_customer_order_transaction on the Link
    Then sat_customer_order_transaction should contain:
      | link_hashkey        | load_date           | load_end_date | is_current | order_amount | order_status |
      | <hash_link_001>     | 2024-01-15T10:30:00 | NULL          | true       | 150.00       | PENDING      |
    And status changes should be tracked with Type 2 SCD

  Scenario: Create Link between Order and Product (many-to-many)
    Given hub_product contains:
      | hub_product_hashkey | business_key |
      | STU901              | PROD-001     |
      | VWX234              | PROD-002     |
    And order line items:
      | order_id | product_id | quantity |
      | ORD-001  | PROD-001   | 2        |
      | ORD-001  | PROD-002   | 1        |
      | ORD-002  | PROD-001   | 3        |
    When I create link_order_product
    Then the link should contain:
      | link_hashkey        | hub_order_hashkey | hub_product_hashkey | load_date           |
      | <hash_OP_001_001>   | MNO345            | STU901              | 2024-01-15T10:30:00 |
      | <hash_OP_001_002>   | MNO345            | VWX234              | 2024-01-15T10:30:00 |
      | <hash_OP_002_001>   | PQR678            | STU901              | 2024-01-16T10:30:00 |

  Scenario: Track Link metadata
    Given order relationship data:
      | order_id | customer_id |
      | ORD-001  | CUST-001    |
    When I load link_customer_order
    Then each link record should include:
      | field         | value                   |
      | load_date     | 2024-01-15T10:30:00     |
      | record_source | ORDER_SYSTEM            |
    And the metadata should track the source of the relationship

  Scenario: Link loading is idempotent
    Given link_customer_order contains:
      | link_hashkey        | hub_customer_hashkey | hub_order_hashkey |
      | <hash_link_001>     | ABC123               | MNO345            |
    When I reload the same relationship data multiple times
    Then link_customer_order should still contain only one record
    And no duplicate links should be created

  Scenario: Handle orphaned Links (referential integrity)
    Given order data references non-existent customer:
      | order_id | customer_id |
      | ORD-003  | CUST-999    |
    And hub_customer does not contain "CUST-999"
    When I attempt to create link_customer_order
    Then the link should not be created
    And an error should be logged:
      | error_type    | ORPHANED_LINK           |
      | details       | Missing Hub: CUST-999   |

  Scenario: Same-as Link (connecting duplicate entities from different sources)
    Given hub_customer contains:
      | hub_customer_hashkey | business_key | record_source |
      | ABC123               | CUST-001     | ERP_SYSTEM    |
      | DEF456               | CRM-12345    | CRM_SYSTEM    |
    And we identify that CUST-001 and CRM-12345 represent the same customer
    When I create link_customer_same_as
    Then the link should connect the duplicate Hub entries:
      | link_hashkey        | hub_customer_hashkey_1 | hub_customer_hashkey_2 | load_date           |
      | <hash_same_as>      | ABC123                 | DEF456                 | 2024-01-15T10:30:00 |
    And this enables entity resolution across sources
