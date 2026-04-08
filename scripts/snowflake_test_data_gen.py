"""
Generate test data for SCD2 incremental load testing.
Inserts ~100 rows into Snowflake table OD_GDWEXTRACT_201201_202408.

Test cases:
- INSERT: ~35 new account_keys (OD001001-OD001035) - completely new records
- UPDATE: ~35 existing accounts (OD000001-OD000035) with new date 2022-01-01 and changed values
- IGNORE: ~30 exact duplicates of existing 2021-12-01 data (should be ignored by SCD2)
"""

import snowflake.connector
import random
from datetime import datetime

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    "account": "fsultcl-ht17125",
    "user": "SRINUBAYYAVARAPU",
    "password": "Srinubayyavarapu5657",
    "warehouse": "COMPUTE_WH",
    "database": "ASB_ANALYTICS",
    "schema": "GDW"
}

TABLE_NAME = "OD_GDWEXTRACT_201201_202408"

def generate_test_data():
    """Generate test data for INSERT, UPDATE, and IGNORE scenarios."""

    random.seed(42)  # For reproducibility

    test_rows = []

    # --- SCENARIO 1: INSERT - New accounts (35 rows) ---
    # These are completely new account_keys that don't exist in the table
    print("Generating INSERT test cases (new accounts)...")
    for i in range(1001, 1036):
        account_key = f"OD{i:06d}"
        test_rows.append({
            "ACCOUNT_KEY": account_key,
            "OBSERVATION_DATE": "2022-01-01",
            "ARREARS_DAYS": random.choice([0, 0, 0, 30, 60, 90]),
            "OUTSTANDING_BALANCE": round(random.uniform(10000, 500000), 2),
            "MONTHLY_PAYMENT": round(random.uniform(500, 8000), 2),
            "PAYMENT_STATUS": random.choice(["CURRENT", "CURRENT", "CURRENT", "ARREARS_30D", "ARREARS_60D"]),
            "EXPOSURE_AMOUNT": round(random.uniform(50000, 1500000), 2),
            "scenario": "INSERT"
        })

    # --- SCENARIO 2: UPDATE - Existing accounts with new date and changed values (35 rows) ---
    # These accounts exist but we're adding a NEW observation_date with DIFFERENT values
    print("Generating UPDATE test cases (existing accounts, new date, changed values)...")
    for i in range(1, 36):
        account_key = f"OD{i:06d}"
        # Changed values compared to their 2021-12-01 data
        test_rows.append({
            "ACCOUNT_KEY": account_key,
            "OBSERVATION_DATE": "2022-01-01",
            "ARREARS_DAYS": random.choice([0, 30, 60, 90, 120]),
            "OUTSTANDING_BALANCE": round(random.uniform(100000, 900000), 2),
            "MONTHLY_PAYMENT": round(random.uniform(2000, 7000), 2),
            "PAYMENT_STATUS": random.choice(["CURRENT", "ARREARS_30D", "ARREARS_60D", "ARREARS_90D"]),
            "EXPOSURE_AMOUNT": round(random.uniform(200000, 1400000), 2),
            "scenario": "UPDATE"
        })

    # --- SCENARIO 3: IGNORE - Exact duplicates of existing data (30 rows) ---
    # We need to fetch existing data for accounts 36-65 for date 2021-12-01 and re-insert it
    # This will simulate duplicate/unchanged data that should be ignored
    print("IGNORE test cases will be fetched from existing data...")

    return test_rows


def get_existing_data_for_duplicates(conn):
    """Fetch existing rows to create exact duplicates for IGNORE scenario."""
    cursor = conn.cursor()

    # Get 30 rows from 2021-12-01 for accounts OD000036 to OD000065
    query = """
        SELECT ACCOUNT_KEY, OBSERVATION_DATE, ARREARS_DAYS, OUTSTANDING_BALANCE,
               MONTHLY_PAYMENT, PAYMENT_STATUS, EXPOSURE_AMOUNT
        FROM OD_GDWEXTRACT_201201_202408
        WHERE OBSERVATION_DATE = '2021-12-01'
        AND ACCOUNT_KEY BETWEEN 'OD000036' AND 'OD000065'
        ORDER BY ACCOUNT_KEY
        LIMIT 30
    """

    cursor.execute(query)
    existing_rows = cursor.fetchall()
    cursor.close()

    duplicates = []
    for row in existing_rows:
        duplicates.append({
            "ACCOUNT_KEY": row[0],
            "OBSERVATION_DATE": str(row[1]),
            "ARREARS_DAYS": int(row[2]) if row[2] else 0,
            "OUTSTANDING_BALANCE": float(row[3]) if row[3] else 0.0,
            "MONTHLY_PAYMENT": float(row[4]) if row[4] else 0.0,
            "PAYMENT_STATUS": row[5],
            "EXPOSURE_AMOUNT": float(row[6]) if row[6] else 0.0,
            "scenario": "IGNORE"
        })

    return duplicates


def insert_test_data(conn, rows):
    """Insert test data into Snowflake table."""
    cursor = conn.cursor()

    insert_sql = f"""
        INSERT INTO {TABLE_NAME}
        (ACCOUNT_KEY, OBSERVATION_DATE, ARREARS_DAYS, OUTSTANDING_BALANCE,
         MONTHLY_PAYMENT, PAYMENT_STATUS, EXPOSURE_AMOUNT)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    insert_count = 0
    for row in rows:
        try:
            cursor.execute(insert_sql, (
                row["ACCOUNT_KEY"],
                row["OBSERVATION_DATE"],
                row["ARREARS_DAYS"],
                row["OUTSTANDING_BALANCE"],
                row["MONTHLY_PAYMENT"],
                row["PAYMENT_STATUS"],
                row["EXPOSURE_AMOUNT"]
            ))
            insert_count += 1
        except Exception as e:
            print(f"  Error inserting {row['ACCOUNT_KEY']}: {e}")

    conn.commit()
    cursor.close()

    return insert_count


def main():
    print("=" * 60)
    print("Snowflake Test Data Generator for SCD2 Testing")
    print("=" * 60)

    # Connect to Snowflake
    print("\nConnecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("Connected successfully!")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    # Get initial count
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    initial_count = cursor.fetchone()[0]
    print(f"\nInitial row count: {initial_count:,}")
    cursor.close()

    # Generate INSERT and UPDATE test data
    test_rows = generate_test_data()

    # Fetch existing data for IGNORE scenario (exact duplicates)
    duplicate_rows = get_existing_data_for_duplicates(conn)
    print(f"Fetched {len(duplicate_rows)} existing rows for IGNORE scenario")

    # Combine all test data
    all_test_rows = test_rows + duplicate_rows

    # Summary before insert
    insert_cases = [r for r in all_test_rows if r["scenario"] == "INSERT"]
    update_cases = [r for r in all_test_rows if r["scenario"] == "UPDATE"]
    ignore_cases = [r for r in all_test_rows if r["scenario"] == "IGNORE"]

    print(f"\nTest data summary:")
    print(f"  - INSERT cases (new accounts): {len(insert_cases)}")
    print(f"  - UPDATE cases (existing accounts, new values): {len(update_cases)}")
    print(f"  - IGNORE cases (exact duplicates): {len(ignore_cases)}")
    print(f"  - Total rows to insert: {len(all_test_rows)}")

    # Insert all test data
    print(f"\nInserting {len(all_test_rows)} test rows...")
    inserted = insert_test_data(conn, all_test_rows)
    print(f"Successfully inserted {inserted} rows")

    # Verify final count
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    final_count = cursor.fetchone()[0]
    print(f"\nFinal row count: {final_count:,}")
    print(f"Rows added: {final_count - initial_count}")
    cursor.close()

    # Close connection
    conn.close()
    print("\n" + "=" * 60)
    print("Test data generation complete!")
    print("=" * 60)

    print("\nExpected SCD2 behavior when running Bronze->Silver:")
    print("  - INSERT cases: Will create NEW current records")
    print("  - UPDATE cases: Will close old records, insert new current records")
    print("  - IGNORE cases: Will be ignored (no changes detected)")


if __name__ == "__main__":
    main()
