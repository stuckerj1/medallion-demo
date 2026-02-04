Feature: Silver Layer Satellite Management with Type 2 SCD
  As a data engineer
  I want to manage Data Vault 2.0 Satellites with Type 2 SCD
  So that I can track historical changes to descriptive attributes

  Background:
    Given the medallion architecture is set up
    And hub_customer contains:
      | hub_customer_hashkey | business_key |
      | ABC123               | CUST-001     |
      | DEF456               | CUST-002     |

  Scenario: Create initial Satellite records
    Given new customer details from source:
      | customer_id | first_name | last_name | email              | phone        |
      | CUST-001    | John       | Doe       | john@example.com   | 555-0001     |
      | CUST-002    | Jane       | Smith     | jane@example.com   | 555-0002     |
    When I load sat_customer_details
    Then the satellite should contain:
      | hub_customer_hashkey | load_date           | load_end_date | hash_diff    | is_current | first_name | last_name | email              | phone    |
      | ABC123               | 2024-01-15T10:30:00 | NULL          | <hash_john>  | true       | John       | Doe       | john@example.com   | 555-0001 |
      | DEF456               | 2024-01-15T10:30:00 | NULL          | <hash_jane>  | true       | Jane       | Smith     | jane@example.com   | 555-0002 |

  Scenario: Detect and track attribute changes (Type 2 SCD)
    Given sat_customer_details contains current record:
      | hub_customer_hashkey | load_date           | load_end_date | hash_diff    | is_current | first_name | last_name | email              |
      | ABC123               | 2024-01-15T10:30:00 | NULL          | <hash_old>   | true       | John       | Doe       | john@old.com       |
    And new customer data arrives:
      | customer_id | first_name | last_name | email              |
      | CUST-001    | John       | Doe       | john@new.com       |
    When I load sat_customer_details with Type 2 SCD
    Then the satellite should contain:
      | hub_customer_hashkey | load_date           | load_end_date       | hash_diff    | is_current | first_name | last_name | email              |
      | ABC123               | 2024-01-15T10:30:00 | 2024-02-01T14:00:00 | <hash_old>   | false      | John       | Doe       | john@old.com       |
      | ABC123               | 2024-02-01T14:00:00 | NULL                | <hash_new>   | true       | John       | Doe       | john@new.com       |
    And the old record should be end-dated
    And the new record should be marked as current

  Scenario: No change detection - do not insert duplicate
    Given sat_customer_details contains current record:
      | hub_customer_hashkey | load_date           | load_end_date | hash_diff    | is_current | first_name | last_name | email              |
      | ABC123               | 2024-01-15T10:30:00 | NULL          | <hash_john>  | true       | John       | Doe       | john@example.com   |
    And new customer data arrives with same attributes:
      | customer_id | first_name | last_name | email              |
      | CUST-001    | John       | Doe       | john@example.com   |
    When I load sat_customer_details with change detection
    Then no new record should be inserted
    And the existing record should remain unchanged:
      | hub_customer_hashkey | load_date           | load_end_date | hash_diff    | is_current |
      | ABC123               | 2024-01-15T10:30:00 | NULL          | <hash_john>  | true       |

  Scenario: Track multiple changes over time
    Given sat_customer_details contains historical records:
      | hub_customer_hashkey | load_date           | load_end_date       | hash_diff    | is_current | email              |
      | ABC123               | 2024-01-15T10:30:00 | 2024-02-01T14:00:00 | <hash_v1>    | false      | john@version1.com  |
      | ABC123               | 2024-02-01T14:00:00 | NULL                | <hash_v2>    | true       | john@version2.com  |
    And new customer data arrives:
      | customer_id | email              |
      | CUST-001    | john@version3.com  |
    When I load sat_customer_details with Type 2 SCD
    Then the satellite should contain three versions:
      | hub_customer_hashkey | load_date           | load_end_date       | hash_diff    | is_current | email              |
      | ABC123               | 2024-01-15T10:30:00 | 2024-02-01T14:00:00 | <hash_v1>    | false      | john@version1.com  |
      | ABC123               | 2024-02-01T14:00:00 | 2024-03-01T09:00:00 | <hash_v2>    | false      | john@version2.com  |
      | ABC123               | 2024-03-01T09:00:00 | NULL                | <hash_v3>    | true       | john@version3.com  |
    And only the latest version should be current

  Scenario: Multiple satellites for different change rates
    Given customer data with different change frequencies:
      | customer_id | first_name | last_name | email            | marketing_consent |
      | CUST-001    | John       | Doe       | john@example.com | true              |
    When I create separate satellites:
      | satellite_name          | attributes                    | change_frequency |
      | sat_customer_details    | first_name, last_name, email  | low              |
      | sat_customer_preferences| marketing_consent             | high             |
    Then sat_customer_details should track name and email changes
    And sat_customer_preferences should independently track consent changes
    And each satellite should maintain its own Type 2 SCD history

  Scenario: Calculate hash_diff for change detection
    Given customer attributes:
      | first_name | last_name | email              |
      | John       | Doe       | john@example.com   |
    When I calculate the hash_diff
    Then the hash should be generated from concatenated values: "JOHN||DOE||JOHN@EXAMPLE.COM"
    And the hash should use SHA-256 algorithm
    And the hash should be deterministic

  Scenario: Handle NULL values in hash_diff calculation
    Given customer attributes with NULL:
      | first_name | last_name | email              |
      | John       | NULL      | john@example.com   |
    When I calculate the hash_diff
    Then NULL values should be converted to "NULL" string
    And the hash should be generated from: "JOHN||NULL||JOHN@EXAMPLE.COM"

  Scenario: Query current records only
    Given sat_customer_details contains multiple versions:
      | hub_customer_hashkey | load_date           | load_end_date       | is_current | first_name |
      | ABC123               | 2024-01-15T10:30:00 | 2024-02-01T14:00:00 | false      | John       |
      | ABC123               | 2024-02-01T14:00:00 | NULL                | true       | John       |
      | DEF456               | 2024-01-15T10:30:00 | NULL                | true       | Jane       |
    When I query for current records only
    Then the result should be:
      | hub_customer_hashkey | is_current | first_name |
      | ABC123               | true       | John       |
      | DEF456               | true       | Jane       |

  Scenario: Point-in-time query for historical analysis
    Given sat_customer_details contains:
      | hub_customer_hashkey | load_date           | load_end_date       | email             |
      | ABC123               | 2024-01-15T10:30:00 | 2024-02-01T14:00:00 | old@example.com   |
      | ABC123               | 2024-02-01T14:00:00 | NULL                | new@example.com   |
    When I query for records as of "2024-01-20T00:00:00"
    Then the result should be:
      | hub_customer_hashkey | email             |
      | ABC123               | old@example.com   |
    And the new record should not be included

  Scenario: Type 1 SCD for non-historical attributes
    Given sat_customer_corrections for data fixes:
      | hub_customer_hashkey | corrected_birth_date |
      | ABC123               | 1990-01-15           |
    When new correction arrives:
      | hub_customer_hashkey | corrected_birth_date |
      | ABC123               | 1990-01-16           |
    Then the satellite should overwrite the value (Type 1):
      | hub_customer_hashkey | corrected_birth_date |
      | ABC123               | 1990-01-16           |
    And no history should be maintained
