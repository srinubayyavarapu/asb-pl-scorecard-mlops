"""
==========================================================
Synthetic Credit Risk Data Generator for ASB Bank Testing
==========================================================

Generates realistic credit risk data and loads it into Snowflake.
This creates test data for ALL 17 tables in master_table_inventory.csv.

Usage:
    python scripts/generate_synthetic_data.py \
        --account <snowflake_account> \
        --user <username> \
        --password <password>

    Example:
    python scripts/generate_synthetic_data.py \
        --account abc12345.australiaeast.azure \
        --user SRINU \
        --password MyPassword123

Prerequisites:
    pip install snowflake-connector-python pandas numpy faker
"""

import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random
import sys

# ── Configuration ──
NUM_HOME_LOAN_ACCOUNTS = 5000    # Keep small for trial (real = 212 GB)
NUM_CREDIT_CARD_ACCOUNTS = 3000
NUM_PERSONAL_LOAN_ACCOUNTS = 2000
NUM_OVERDRAFT_ACCOUNTS = 1000
NUM_SME_ACCOUNTS = 500
NUM_MONTHS = 36                   # 3 years of monthly history
RANDOM_SEED = 42

np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)


# ══════════════════════════════════════════════
# Data Generation Functions
# ══════════════════════════════════════════════

def generate_account_keys(prefix, n):
    """Generate unique account keys like HL000001, CC000001, etc."""
    return [f"{prefix}{str(i).zfill(6)}" for i in range(1, n + 1)]


def generate_dates(start_date, n_months):
    """Generate list of monthly observation dates."""
    dates = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    for _ in range(n_months):
        dates.append(current)
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return dates


def random_choice_weighted(options, weights, n):
    """Weighted random choice for realistic distributions."""
    return np.random.choice(options, size=n, p=weights)


# ──────────────────────────────────────────────
# Table 1: HLACCTBASE_FINAL (Home Loan Account Base)
# ──────────────────────────────────────────────
def generate_hlacctbase_final(n=NUM_HOME_LOAN_ACCOUNTS):
    print(f"  Generating HLACCTBASE_FINAL ({n:,} accounts)...")

    accounts = generate_account_keys("HL", n)

    data = {
        "ACCOUNT_KEY": accounts,
        "CUSTOMER_ID": [f"CUST{str(random.randint(100000, 999999))}" for _ in range(n)],
        "PRODUCT_TYPE": ["HOME_LOAN"] * n,
        "LOAN_AMOUNT": np.random.uniform(100000, 1500000, n).round(2),
        "INTEREST_RATE": np.random.uniform(3.5, 8.5, n).round(2),
        "LOAN_TERM_MONTHS": random_choice_weighted([180, 240, 300, 360], [0.1, 0.2, 0.3, 0.4], n),
        "LVR": np.random.uniform(30, 95, n).round(2),  # Loan-to-Value Ratio
        "PROPERTY_VALUE": np.random.uniform(200000, 3000000, n).round(2),
        "PROPERTY_TYPE": random_choice_weighted(
            ["RESIDENTIAL", "INVESTMENT", "COMMERCIAL", "RURAL"],
            [0.60, 0.25, 0.10, 0.05], n
        ),
        "REGION": random_choice_weighted(
            ["AUCKLAND", "WELLINGTON", "CHRISTCHURCH", "HAMILTON", "TAURANGA", "OTHER"],
            [0.35, 0.20, 0.15, 0.10, 0.08, 0.12], n
        ),
        "EMPLOYMENT_TYPE": random_choice_weighted(
            ["SALARY", "SELF_EMPLOYED", "CONTRACT", "RETIRED", "OTHER"],
            [0.55, 0.20, 0.12, 0.08, 0.05], n
        ),
        "ANNUAL_INCOME": np.random.uniform(40000, 250000, n).round(2),
        "CREDIT_SCORE": np.random.randint(300, 850, n),
        "MONTHS_ON_BOOK": np.random.randint(1, 240, n),
        "ACCOUNT_STATUS": random_choice_weighted(
            ["ACTIVE", "CLOSED", "DORMANT", "STAFF"],
            [0.80, 0.10, 0.07, 0.03], n
        ),
        "ORIGINATION_DATE": [
            (datetime(2015, 1, 1) + timedelta(days=random.randint(0, 3000))).strftime("%Y-%m-%d")
            for _ in range(n)
        ],
        "OBSERVATION_DATE": [
            (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1200))).strftime("%Y-%m-%d")
            for _ in range(n)
        ],
    }

    return pd.DataFrame(data)


# ──────────────────────────────────────────────
# Table 2-4: GDW Extracts (Monthly snapshots)
# ──────────────────────────────────────────────
def generate_gdw_extract(accounts, prefix, n_months=NUM_MONTHS):
    print(f"  Generating {prefix}_GDWEXTRACT ({len(accounts):,} accounts × {n_months} months)...")

    dates = generate_dates("2021-07-01", n_months)
    rows = []

    for account in accounts:
        # Each account has a base arrears tendency
        base_risk = np.random.beta(2, 10)  # Most accounts are low risk

        for date in dates:
            # Arrears days: mostly 0, occasionally increases
            arrears = 0
            if random.random() < base_risk:
                arrears = random.choice([0, 0, 0, 30, 30, 60, 60, 90, 120, 150, 180])

            rows.append({
                "ACCOUNT_KEY": account,
                "OBSERVATION_DATE": date.strftime("%Y-%m-%d"),
                "ARREARS_DAYS": arrears,
                "OUTSTANDING_BALANCE": round(random.uniform(50000, 1000000) * (1 - random.uniform(0, 0.3)), 2),
                "MONTHLY_PAYMENT": round(random.uniform(500, 8000), 2),
                "PAYMENT_STATUS": "CURRENT" if arrears == 0 else f"ARREARS_{arrears}D",
                "EXPOSURE_AMOUNT": round(random.uniform(100000, 1500000), 2),
            })

    return pd.DataFrame(rows)


# ──────────────────────────────────────────────
# Table 5: HL_BASELEVEL (Home Loan Base Level)
# ──────────────────────────────────────────────
def generate_hl_baselevel(accounts):
    print(f"  Generating HL_BASELEVEL ({len(accounts):,} accounts)...")

    n = len(accounts)
    return pd.DataFrame({
        "ACCOUNT_KEY": accounts,
        "CUSTOMER_SEGMENT": random_choice_weighted(
            ["PRIME", "NEAR_PRIME", "SUB_PRIME"],
            [0.65, 0.25, 0.10], n
        ),
        "DEBT_TO_INCOME": np.random.uniform(15, 60, n).round(2),
        "NUM_DEPENDANTS": np.random.choice([0, 1, 2, 3, 4], n, p=[0.25, 0.30, 0.25, 0.15, 0.05]),
        "AGE": np.random.randint(22, 75, n),
        "MARITAL_STATUS": random_choice_weighted(
            ["MARRIED", "SINGLE", "DIVORCED", "DEFACTO"],
            [0.45, 0.30, 0.15, 0.10], n
        ),
        "TIME_AT_ADDRESS_MONTHS": np.random.randint(1, 240, n),
        "TIME_AT_EMPLOYER_MONTHS": np.random.randint(1, 300, n),
        "NUM_CREDIT_ENQUIRIES": np.random.randint(0, 15, n),
    })


# ──────────────────────────────────────────────
# Table 6: HL_PPTY_RES_TYPE (Property/Residence)
# ──────────────────────────────────────────────
def generate_hl_ppty_res_type(accounts):
    print(f"  Generating HL_PPTY_RES_TYPE ({len(accounts):,} accounts)...")

    n = len(accounts)
    return pd.DataFrame({
        "ACCOUNT_KEY": accounts,
        "PROPERTY_CLASS": random_choice_weighted(
            ["STANDALONE", "APARTMENT", "TOWNHOUSE", "UNIT", "RURAL"],
            [0.40, 0.25, 0.20, 0.10, 0.05], n
        ),
        "NUM_BEDROOMS": np.random.choice([1, 2, 3, 4, 5], n, p=[0.05, 0.20, 0.40, 0.25, 0.10]),
        "LAND_AREA_SQM": np.random.uniform(100, 5000, n).round(0),
        "YEAR_BUILT": np.random.randint(1960, 2024, n),
        "VALUATION_DATE": [
            (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1200))).strftime("%Y-%m-%d")
            for _ in range(n)
        ],
        "VALUATION_AMOUNT": np.random.uniform(200000, 3000000, n).round(2),
    })


# ──────────────────────────────────────────────
# Table 7-8: Default Flags
# ──────────────────────────────────────────────
def generate_default_flags(all_accounts):
    print(f"  Generating ADD_DEFAULT_FLAG ({len(all_accounts):,} accounts)...")

    n = len(all_accounts)
    # ~5% default rate (realistic for credit risk)
    default_flags = np.random.choice([0, 1], n, p=[0.95, 0.05])

    return pd.DataFrame({
        "ACCOUNT_KEY": all_accounts,
        "DEFAULT_FLAG": default_flags,
        "DEFAULT_DATE": [
            (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
            if flag == 1 else None
            for flag in default_flags
        ],
        "MAX_ARREARS_DAYS": [
            random.choice([60, 90, 120, 150, 180]) if flag == 1 else random.randint(0, 30)
            for flag in default_flags
        ],
        "UPDATED_AT": [
            (datetime(2024, 8, 1) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S")
            for _ in range(n)
        ],
    })


def generate_all_default_baselevel(all_accounts):
    print(f"  Generating ALL_DEFAULT_BASELEVEL ({len(all_accounts):,} accounts)...")

    n = len(all_accounts)
    return pd.DataFrame({
        "ACCOUNT_KEY": all_accounts,
        "PRODUCT_TYPE": [
            a[:2].replace("HL", "HOME_LOAN").replace("CC", "CREDIT_CARD")
            .replace("PL", "PERSONAL_LOAN").replace("OD", "OVERDRAFT").replace("SM", "SME")
            for a in all_accounts
        ],
        "EVER_DEFAULT": np.random.choice([0, 1], n, p=[0.92, 0.08]),
        "FIRST_DEFAULT_DATE": [
            (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime("%Y-%m-%d")
            if random.random() < 0.08 else None
            for _ in range(n)
        ],
        "TIMES_DEFAULTED": np.random.choice([0, 0, 0, 0, 1, 1, 2, 3], n),
        "UPDATED_AT": [
            (datetime(2024, 8, 1) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S")
            for _ in range(n)
        ],
    })


# ──────────────────────────────────────────────
# Table 9-10: Credit Card Scorecard & DataFrame
# ──────────────────────────────────────────────
def generate_cc_scorecard(accounts):
    print(f"  Generating CC_Scorecard_withP1 ({len(accounts):,} accounts)...")

    n = len(accounts)
    return pd.DataFrame({
        "ACCOUNT_KEY": accounts,
        "CUSTOMER_ID": [f"CUST{random.randint(100000, 999999)}" for _ in range(n)],
        "CREDIT_LIMIT": np.random.uniform(2000, 50000, n).round(2),
        "CURRENT_BALANCE": np.random.uniform(0, 40000, n).round(2),
        "UTILIZATION_PCT": np.random.uniform(0, 100, n).round(2),
        "MIN_PAYMENT_DUE": np.random.uniform(25, 2000, n).round(2),
        "MONTHS_ON_BOOK": np.random.randint(1, 180, n),
        "ANNUAL_INCOME": np.random.uniform(30000, 200000, n).round(2),
        "P1_SCORE": np.random.randint(300, 850, n),
        "OBSERVATION_DATE": [
            (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1200))).strftime("%Y-%m-%d")
            for _ in range(n)
        ],
    })


def generate_cc_dataframe(accounts):
    print(f"  Generating CC_DATAFRAME ({len(accounts):,} accounts)...")

    n = len(accounts)
    return pd.DataFrame({
        "ACCOUNT_KEY": accounts,
        "CASH_ADVANCE_PCT": np.random.uniform(0, 50, n).round(2),
        "NUM_TRANSACTIONS_3M": np.random.randint(0, 200, n),
        "AVG_TRANSACTION_AMT": np.random.uniform(10, 500, n).round(2),
        "OVERLIMIT_COUNT_12M": np.random.choice([0, 0, 0, 1, 2, 3], n),
        "LATE_PAYMENT_COUNT_12M": np.random.choice([0, 0, 0, 0, 1, 1, 2, 3], n),
        "REWARD_TIER": random_choice_weighted(
            ["STANDARD", "GOLD", "PLATINUM", "BLACK"],
            [0.50, 0.25, 0.15, 0.10], n
        ),
    })


# ──────────────────────────────────────────────
# Table 11-12: CC Facility Snapshot & Account Keys
# ──────────────────────────────────────────────
def generate_cc_facility_snapshot(accounts):
    print(f"  Generating CC_facilitySnapshot ({len(accounts):,} accounts)...")

    n = len(accounts)
    return pd.DataFrame({
        "ACCOUNT_KEY": accounts,
        "FACILITY_TYPE": random_choice_weighted(["REVOLVING", "TERM", "OVERDRAFT"], [0.70, 0.20, 0.10], n),
        "FACILITY_LIMIT": np.random.uniform(5000, 100000, n).round(2),
        "DRAWN_AMOUNT": np.random.uniform(0, 80000, n).round(2),
        "FACILITY_STATUS": random_choice_weighted(["ACTIVE", "FROZEN", "CLOSED"], [0.85, 0.05, 0.10], n),
        "SNAPSHOT_DATE": ["2024-08-01"] * n,
    })


def generate_cc_acctnumkey(accounts):
    print(f"  Generating CC_ACCTNUMKEY ({len(accounts):,} accounts)...")

    return pd.DataFrame({
        "ACCOUNT_KEY": accounts,
        "ACCOUNT_NUMBER": [f"44{random.randint(10000000000, 99999999999)}" for _ in range(len(accounts))],
        "CARD_TYPE": random_choice_weighted(["VISA", "MASTERCARD", "AMEX"], [0.45, 0.40, 0.15], len(accounts)),
    })


# ──────────────────────────────────────────────
# Table 13-14: Personal Loans
# ──────────────────────────────────────────────
def generate_pl_scorecard(accounts):
    print(f"  Generating PL_Scorecard_withP1 ({len(accounts):,} accounts)...")

    n = len(accounts)
    return pd.DataFrame({
        "ACCOUNT_KEY": accounts,
        "CUSTOMER_ID": [f"CUST{random.randint(100000, 999999)}" for _ in range(n)],
        "LOAN_AMOUNT": np.random.uniform(2000, 50000, n).round(2),
        "INTEREST_RATE": np.random.uniform(8, 22, n).round(2),
        "LOAN_TERM_MONTHS": random_choice_weighted([12, 24, 36, 48, 60], [0.10, 0.25, 0.30, 0.20, 0.15], n),
        "LOAN_PURPOSE": random_choice_weighted(
            ["DEBT_CONSOLIDATION", "HOME_IMPROVEMENT", "VEHICLE", "HOLIDAY", "OTHER"],
            [0.30, 0.25, 0.20, 0.15, 0.10], n
        ),
        "ANNUAL_INCOME": np.random.uniform(30000, 150000, n).round(2),
        "P1_SCORE": np.random.randint(300, 850, n),
        "OBSERVATION_DATE": [
            (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1200))).strftime("%Y-%m-%d")
            for _ in range(n)
        ],
    })


def generate_pl_dataframe(accounts):
    print(f"  Generating PL_DATAFRAME ({len(accounts):,} accounts)...")

    n = len(accounts)
    return pd.DataFrame({
        "ACCOUNT_KEY": accounts,
        "REPAYMENT_FREQUENCY": random_choice_weighted(["MONTHLY", "FORTNIGHTLY", "WEEKLY"], [0.60, 0.25, 0.15], n),
        "AUTO_DEBIT": random_choice_weighted(["Y", "N"], [0.70, 0.30], n),
        "MISSED_PAYMENTS_6M": np.random.choice([0, 0, 0, 0, 1, 1, 2, 3], n),
        "REMAINING_TERM_MONTHS": np.random.randint(1, 60, n),
    })


# ──────────────────────────────────────────────
# Table 15-16: Model Parameters (small reference tables)
# ──────────────────────────────────────────────
def generate_model_params(product):
    print(f"  Generating {product}_Scorecard_Model...")

    features = [f"feature_{i}" for i in range(1, 16)]
    return pd.DataFrame({
        "FEATURE_NAME": features,
        "COEFFICIENT": np.random.uniform(-2, 2, len(features)).round(6),
        "STD_ERROR": np.random.uniform(0.01, 0.5, len(features)).round(6),
        "P_VALUE": np.random.uniform(0, 0.10, len(features)).round(4),
        "INTERCEPT": [round(random.uniform(-3, 3), 6)] + [None] * (len(features) - 1),
        "MODEL_VERSION": ["SAS_v1.0"] * len(features),
        "PRODUCT": [product.upper()] * len(features),
    })


# ══════════════════════════════════════════════
# Load to Snowflake
# ══════════════════════════════════════════════

def load_to_snowflake(df, schema, table_name, conn):
    """Load a pandas DataFrame to a Snowflake table."""
    from snowflake.connector.pandas_tools import write_pandas

    print(f"    Loading {schema}.{table_name} ({len(df):,} rows)...")

    # Ensure column names are uppercase (Snowflake convention)
    df.columns = [c.upper() for c in df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name,
        database="ASB_ANALYTICS",
        schema=schema,
        auto_create_table=True,
        overwrite=True,
    )

    if success:
        print(f"    [OK] {schema}.{table_name} -- {nrows:,} rows loaded")
    else:
        print(f"    [FAIL] {schema}.{table_name}")

    return success


# ══════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic credit risk data for Snowflake")
    parser.add_argument("--account", required=True, help="Snowflake account (e.g., abc12345.australiaeast.azure)")
    parser.add_argument("--user", required=True, help="Snowflake username")
    parser.add_argument("--password", required=True, help="Snowflake password")
    parser.add_argument("--local-only", action="store_true", help="Generate CSVs locally without loading to Snowflake")

    args = parser.parse_args()

    print("=" * 60)
    print("ASB Bank — Synthetic Credit Risk Data Generator")
    print("=" * 60)

    # ── Generate all account keys ──
    hl_accounts = generate_account_keys("HL", NUM_HOME_LOAN_ACCOUNTS)
    cc_accounts = generate_account_keys("CC", NUM_CREDIT_CARD_ACCOUNTS)
    pl_accounts = generate_account_keys("PL", NUM_PERSONAL_LOAN_ACCOUNTS)
    od_accounts = generate_account_keys("OD", NUM_OVERDRAFT_ACCOUNTS)
    sme_accounts = generate_account_keys("SM", NUM_SME_ACCOUNTS)
    all_accounts = hl_accounts + cc_accounts + pl_accounts + od_accounts + sme_accounts

    # ── Generate all tables ──
    print("\n[1/5] Generating CREDIT_RISK tables...")
    tables_credit_risk = {
        "HLACCTBASE_FINAL": generate_hlacctbase_final(),
        "CC_Scorecard_withP1": generate_cc_scorecard(cc_accounts),
        "CC_DATAFRAME": generate_cc_dataframe(cc_accounts),
        "CC_facilitySnapshot_temp2": generate_cc_facility_snapshot(cc_accounts),
        "PL_Scorecard_withP1": generate_pl_scorecard(pl_accounts),
        "PL_DATAFRAME": generate_pl_dataframe(pl_accounts),
        "CC_Scorecard_Model": generate_model_params("CC"),
        "PL_Scorecard_Model": generate_model_params("PL"),
        "ADD_DEFAULT_FLAG_202408": generate_default_flags(all_accounts),
        "ALL_DEFAULT_BASELEVEL_202408": generate_all_default_baselevel(all_accounts),
    }

    print("\n[2/5] Generating GDW tables...")
    tables_gdw = {
        "CC_ACCTNUMKEY_199607_202408": generate_cc_acctnumkey(cc_accounts),
        "CC_GDWEXTRACT_199607_202408": generate_gdw_extract(cc_accounts, "CC", n_months=12),
        "HL_BASELEVEL_199607_202408": generate_hl_baselevel(hl_accounts),
        "HL_GDWEXTRACT_199607_202408": generate_gdw_extract(hl_accounts, "HL", n_months=12),
        "HL_PPTY_RES_TYPE_199607_202408": generate_hl_ppty_res_type(hl_accounts),
        "OD_GDWEXTRACT_201201_202408": generate_gdw_extract(od_accounts, "OD", n_months=6),
        "SME_GDWEXTRACT_201201_202408": generate_gdw_extract(sme_accounts, "SM", n_months=6),
    }

    # ── Save locally as CSV (optional) ──
    if args.local_only:
        print("\n[3/5] Saving CSVs locally...")
        import os
        csv_dir = os.path.join(os.path.dirname(__file__), "..", "test_data")
        os.makedirs(csv_dir, exist_ok=True)
        for name, df in {**tables_credit_risk, **tables_gdw}.items():
            path = os.path.join(csv_dir, f"{name}.csv")
            df.to_csv(path, index=False)
            print(f"  [OK] {path} ({len(df):,} rows)")
        print("\nDone! CSVs saved to test_data/")
        return

    # ── Connect to Snowflake ──
    print("\n[3/5] Connecting to Snowflake...")
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=args.account,
        user=args.user,
        password=args.password,
        database="ASB_ANALYTICS",
        warehouse="COMPUTE_WH",
    )

    print("  [OK] Connected to Snowflake")

    # ── Load CREDIT_RISK tables ──
    print("\n[4/5] Loading CREDIT_RISK tables...")
    for table_name, df in tables_credit_risk.items():
        load_to_snowflake(df, "CREDIT_RISK", table_name, conn)

    # ── Load GDW tables ──
    print("\n[5/5] Loading GDW tables...")
    for table_name, df in tables_gdw.items():
        load_to_snowflake(df, "GDW", table_name, conn)

    conn.close()

    # ── Summary ──
    total_tables = len(tables_credit_risk) + len(tables_gdw)
    total_rows = sum(len(df) for df in tables_credit_risk.values()) + sum(len(df) for df in tables_gdw.values())

    print("\n" + "=" * 60)
    print(f"COMPLETE!")
    print(f"  Tables created: {total_tables}")
    print(f"  Total rows:     {total_rows:,}")
    print(f"  Database:       ASB_ANALYTICS")
    print(f"  Schemas:        CREDIT_RISK ({len(tables_credit_risk)} tables)")
    print(f"                  GDW ({len(tables_gdw)} tables)")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Verify in Snowflake: SELECT COUNT(*) FROM ASB_ANALYTICS.CREDIT_RISK.HLACCTBASE_FINAL;")
    print("  2. Configure Databricks secrets for Snowflake connection")
    print("  3. Run: notebooks/etl/01_ingest_snowflake.py")


if __name__ == "__main__":
    main()
