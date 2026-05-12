"""
================================================================
Seed Snowflake demo databases for ASB PL Application Scorecard
================================================================
Creates the 9 client-side source tables across 2 Snowflake DBs:

  DP_CreditApplication.mart     (5 tables — origination)
    DIM_SMApplicationRequest
    DIM_SMOnyxApplication
    DIM_SMApplicationRequestSummary
    FACT_SMApplicationRequest
    FACT_SMBridgeapplicationfacility

  DP_CreditManagement.mart      (4 tables — performance)
    DIM_product
    DIM_facility
    DIM_snapshotdate
    FACT_CreditFacility

Volume (single demo-sized dataset, no per-env split):
  ~10,000 applications  ->  ~8,300 facilities  ->  ~200,000 monthly observations

Run once locally:
  python scripts/seed_snowflake_demo.py
================================================================
"""

import numpy as np
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta
import random

# ──────────────────────────────────────────────────────────────
# Snowflake connection (trial)
# ──────────────────────────────────────────────────────────────
SF = {
    "account":   "CQZORVY-YZ26298",
    "user":      "SRINUBAYYAVARAPU3657",
    "password":  "Srinubayyavarapu3657",
    "warehouse": "COMPUTE_WH",
    "role":      "ACCOUNTADMIN",
}

DB_APP  = "DP_CreditApplication"
DB_MGMT = "DP_CreditManagement"
SCHEMA  = "MART"

N_APPLICATIONS = 10_000
APPROVAL_RATE  = 0.83
PERF_MONTHS    = 24
SEED           = 42

# ──────────────────────────────────────────────────────────────
# Categorical universes (anchor real banking distributions)
# ──────────────────────────────────────────────────────────────
OCCUPATION = ["Professional", "Manager", "Clerical", "Trades", "Sales",
              "Labourer", "Self-Employed", "Retired", "Student", "Other"]
INCOME_SOURCE = ["Salary", "SelfEmployed", "Pension", "Investment", "Benefit", "Mixed"]
HOUSING       = ["OwnerOccupier", "Mortgaged", "RentingPrivate", "RentingPublic", "BoardingFamily"]
LOAN_PURPOSE  = ["DebtConsolidation", "HomeImprovement", "Vehicle", "Holiday", "Medical", "Other"]
REGION        = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Tauranga", "Other"]
MARITAL       = ["Single", "Married", "DeFacto", "Divorced", "Widowed"]
CHANNEL       = ["BRANCH", "ONLINE", "BROKER", "MOBILE"]
APP_TYPE      = ["NEW", "TOPUP", "RENEWAL"]

# Risk multipliers (used to make performance realistic)
OCC_RISK = {"Labourer": 1.4, "Self-Employed": 1.3, "Student": 1.5, "Sales": 1.2,
            "Trades": 1.0, "Clerical": 0.9, "Manager": 0.7, "Professional": 0.6,
            "Retired": 0.8, "Other": 1.1}
INC_RISK = {"Benefit": 1.8, "SelfEmployed": 1.4, "Mixed": 1.2, "Salary": 0.8,
            "Investment": 0.7, "Pension": 0.9}
HOU_RISK = {"RentingPublic": 1.6, "BoardingFamily": 1.3, "RentingPrivate": 1.2,
            "Mortgaged": 0.9, "OwnerOccupier": 0.7}


# ══════════════════════════════════════════════════════════════
# Helper: month-end
# ══════════════════════════════════════════════════════════════
def month_end(d):
    return (d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)


# ══════════════════════════════════════════════════════════════
# ORIGINATION (DP_CreditApplication.mart) — 5 tables
# ══════════════════════════════════════════════════════════════
def generate_origination(n):
    """Build the 5 origination tables from one shared application backbone.

    Returns dict of {table_name: dataframe}.
    """
    print(f"\n[Origination] Generating backbone for {n:,} applications...")

    base_date = datetime(2022, 1, 1)
    app_dates = [base_date + timedelta(days=int(np.random.uniform(0, 730))) for _ in range(n)]

    # Bureau score drives approval probability
    bureau = np.clip(np.random.normal(680, 90, n), 300, 850).astype(int)
    approve_prob = 1 / (1 + np.exp(-(bureau - 600) / 40))
    approved = np.random.uniform(0, 1, n) < approve_prob

    # Force overall approval rate to land near APPROVAL_RATE
    target_approved = int(n * APPROVAL_RATE)
    if approved.sum() != target_approved:
        order = np.argsort(-approve_prob)
        approved = np.zeros(n, dtype=bool)
        approved[order[:target_approved]] = True

    # Surrogate keys + business keys
    app_request_ids  = [f"AR{str(i).zfill(8)}" for i in range(1, n + 1)]
    application_keys = [f"AK{str(i).zfill(8)}" for i in range(1, n + 1)]
    onyx_keys        = [f"OK{str(i).zfill(8)}" for i in range(1, n + 1)]
    summary_keys     = [f"SK{str(i).zfill(8)}" for i in range(1, n + 1)]
    customer_ids     = [f"CUST{random.randint(100000, 999999)}" for _ in range(n)]

    # Demographics / financials (drives DIM_SMApplicationRequestSummary)
    occupation   = np.random.choice(OCCUPATION, n,
                     p=[0.18, 0.12, 0.15, 0.12, 0.10, 0.10, 0.10, 0.06, 0.03, 0.04])
    income_src   = np.random.choice(INCOME_SOURCE, n,
                     p=[0.55, 0.18, 0.10, 0.05, 0.07, 0.05])
    housing      = np.random.choice(HOUSING, n,
                     p=[0.30, 0.30, 0.25, 0.10, 0.05])
    annual_inc   = np.round(np.random.lognormal(11.0, 0.45, n), 2).clip(15_000, 400_000)
    age          = np.random.randint(18, 75, n)
    dependants   = np.random.choice([0, 1, 2, 3, 4, 5], n,
                     p=[0.30, 0.25, 0.20, 0.15, 0.07, 0.03])
    marital      = np.random.choice(MARITAL, n,
                     p=[0.32, 0.42, 0.12, 0.10, 0.04])
    region       = np.random.choice(REGION, n,
                     p=[0.35, 0.20, 0.15, 0.10, 0.08, 0.12])
    addr_months  = np.random.randint(0, 240, n)
    emp_months   = np.random.randint(0, 360, n)
    enquiries    = np.random.choice(range(0, 12), n,
                     p=[0.30, 0.25, 0.18, 0.10, 0.06, 0.04, 0.03, 0.02,
                        0.01, 0.005, 0.003, 0.002])
    existing_debt = np.round(np.random.uniform(0, 150_000, n), 2)
    dti_ratio    = np.round(existing_debt / np.maximum(annual_inc, 1), 3).clip(0, 5)

    requested_amt   = np.round(np.random.uniform(2_000, 50_000, n), 2)
    requested_term  = np.random.choice([12, 24, 36, 48, 60], n,
                        p=[0.10, 0.25, 0.30, 0.20, 0.15])
    purpose         = np.random.choice(LOAN_PURPOSE, n,
                        p=[0.30, 0.20, 0.20, 0.15, 0.05, 0.10])
    channel         = np.random.choice(CHANNEL, n, p=[0.40, 0.30, 0.20, 0.10])
    app_type        = np.random.choice(APP_TYPE, n, p=[0.75, 0.15, 0.10])
    joint_flag      = np.random.choice(["Y", "N"], n, p=[0.20, 0.80])
    branch_code     = [f"BR{random.randint(1, 50):03d}" for _ in range(n)]
    officer_id      = [f"OFF{random.randint(1000, 9999)}" for _ in range(n)]

    decision_dates = [d + timedelta(days=int(np.random.uniform(0, 5))) for d in app_dates]
    decision_reason = np.where(
        approved,
        np.random.choice(["AUTO_APPROVED", "MANUAL_APPROVED"], n, p=[0.80, 0.20]),
        np.random.choice(["LOW_SCORE", "DTI_HIGH", "INSUFFICIENT_INCOME", "ADVERSE_BUREAU"], n,
                         p=[0.40, 0.25, 0.20, 0.15]),
    )

    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    submission_ts = [d.strftime("%Y-%m-%d %H:%M:%S") for d in app_dates]

    # ── 1. DIM_SMApplicationRequest ───────────────────────────────
    dim_app_req = pd.DataFrame({
        "APPLICATION_KEY":        application_keys,
        "APPLICATION_REFERENCE":  [f"APPREF-{i:08d}" for i in range(1, n + 1)],
        "LOAN_PURPOSE":           purpose,
        "REQUESTED_TERM_MONTHS":  requested_term,
        "REQUESTED_FREQUENCY":    np.random.choice(["MONTHLY", "FORTNIGHTLY", "WEEKLY"], n,
                                                   p=[0.65, 0.25, 0.10]),
        "APPLICATION_TYPE":       app_type,
        "JOINT_APP_FLAG":         joint_flag,
        "PRODUCT_CODE":           "PL",
        "_UPDATED_AT":            now_ts,
    })

    # ── 2. DIM_SMOnyxApplication ──────────────────────────────────
    dim_onyx = pd.DataFrame({
        "ONYX_KEY":               onyx_keys,
        "ONYX_APP_REFERENCE":     [f"ONYX-{i:08d}" for i in range(1, n + 1)],
        "CHANNEL":                channel,
        "BRANCH_CODE":            branch_code,
        "OFFICER_ID":             officer_id,
        "SUBMISSION_TS":          submission_ts,
        "CREDIT_BUREAU_SCORE":    bureau,
        "DTI_RATIO":              dti_ratio,
        "ONYX_STATUS":            np.where(approved, "APPROVED", "DECLINED"),
        "_UPDATED_AT":            now_ts,
    })

    # ── 3. DIM_SMApplicationRequestSummary ────────────────────────
    dim_summary = pd.DataFrame({
        "SUMMARY_KEY":            summary_keys,
        "CUSTOMER_ID":            customer_ids,
        "AGE":                    age,
        "ANNUAL_INCOME":          annual_inc,
        "OCCUPATION":             occupation,
        "INCOME_SOURCE":          income_src,
        "HOUSING_STATUS":         housing,
        "REGION":                 region,
        "MARITAL":                marital,
        "DEPENDANTS":             dependants,
        "EXISTING_DEBT":          existing_debt,
        "NUM_ENQUIRIES_6M":       enquiries,
        "TIME_AT_ADDRESS_MO":     addr_months,
        "TIME_AT_EMPLOYER_MO":    emp_months,
        "_UPDATED_AT":            now_ts,
    })

    # ── 4. FACT_SMApplicationRequest ──────────────────────────────
    fact_app_req = pd.DataFrame({
        "APPLICATION_REQUEST_ID": app_request_ids,
        "APPLICATION_KEY":        application_keys,
        "ONYX_KEY":               onyx_keys,
        "SUMMARY_KEY":            summary_keys,
        "CUSTOMER_ID":            customer_ids,
        "APPLICATION_DATE":       [d.strftime("%Y-%m-%d") for d in app_dates],
        "DECISION_DATE":          [d.strftime("%Y-%m-%d") for d in decision_dates],
        "REQUESTED_AMOUNT":       requested_amt,
        "DECISION":               np.where(approved, "APPROVED", "DECLINED"),
        "DECISION_REASON":        decision_reason,
        "PRODUCT_CODE":           "PL",
        "_UPDATED_AT":            now_ts,
    })

    print(f"  DIM_SMApplicationRequest:        {len(dim_app_req):,}")
    print(f"  DIM_SMOnyxApplication:           {len(dim_onyx):,}")
    print(f"  DIM_SMApplicationRequestSummary: {len(dim_summary):,}")
    print(f"  FACT_SMApplicationRequest:       {len(fact_app_req):,}")

    # Return raw arrays for downstream (need approved mask + lookups)
    backbone = {
        "app_request_ids":  app_request_ids,
        "application_keys": application_keys,
        "customer_ids":     customer_ids,
        "app_dates":        app_dates,
        "approved":         approved,
        "bureau":           bureau,
        "occupation":       occupation,
        "income_src":       income_src,
        "housing":          housing,
        "emp_months":       emp_months,
        "enquiries":        enquiries,
        "requested_amt":    requested_amt,
        "requested_term":   requested_term,
    }

    return {
        "DIM_SMApplicationRequest":        dim_app_req,
        "DIM_SMOnyxApplication":           dim_onyx,
        "DIM_SMApplicationRequestSummary": dim_summary,
        "FACT_SMApplicationRequest":       fact_app_req,
    }, backbone


def generate_bridge_and_facility_seed(backbone):
    """Build bridge (approved-only) and seed facility info for management domain.

    Returns:
      bridge_df  -> FACT_SMBridgeapplicationfacility (origination DB)
      fac_info   -> dict with facility_keys, customer_ids, funded_dates, amounts,
                    rates, terms, plus risk lookups (for performance generation)
    """
    approved_mask = backbone["approved"]
    n_approved = int(approved_mask.sum())
    print(f"\n[Bridge] Generating FACT_SMBridgeapplicationfacility ({n_approved:,} approved)...")

    app_request_ids_arr = np.array(backbone["app_request_ids"])
    customer_ids_arr    = np.array(backbone["customer_ids"])
    app_dates_arr       = np.array(backbone["app_dates"])
    requested_amt_arr   = np.array(backbone["requested_amt"])
    requested_term_arr  = np.array(backbone["requested_term"])

    approved_app_request_ids = app_request_ids_arr[approved_mask]
    approved_customer_ids    = customer_ids_arr[approved_mask]
    approved_app_dates       = app_dates_arr[approved_mask]
    approved_amounts         = requested_amt_arr[approved_mask]
    approved_terms           = requested_term_arr[approved_mask]

    facility_keys = [f"FK{str(i).zfill(8)}" for i in range(1, n_approved + 1)]
    bridge_ids    = [f"BG{str(i).zfill(8)}" for i in range(1, n_approved + 1)]

    funded_dates = [d + timedelta(days=int(np.random.uniform(1, 8))) for d in approved_app_dates]
    funded_amts  = np.round(approved_amounts * np.random.uniform(0.90, 1.00, n_approved), 2)
    rates        = np.round(np.random.uniform(7.5, 22.0, n_approved), 2)

    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    bridge_df = pd.DataFrame({
        "BRIDGE_ID":              bridge_ids,
        "APPLICATION_REQUEST_ID": approved_app_request_ids,
        "FACILITY_KEY":           facility_keys,
        "CREATED_DATE":           [d.strftime("%Y-%m-%d") for d in funded_dates],
        "RELATIONSHIP_TYPE":      "PRIMARY",
        "_UPDATED_AT":            now_ts,
    })

    print(f"  FACT_SMBridgeapplicationfacility: {len(bridge_df):,}")

    fac_info = {
        "facility_keys":    facility_keys,
        "customer_ids":     approved_customer_ids,
        "app_request_ids":  approved_app_request_ids,
        "funded_dates":     funded_dates,
        "funded_amounts":   funded_amts,
        "rates":            rates,
        "terms":            approved_terms,
        # carry forward risk inputs aligned to approved subset
        "bureau":     np.array(backbone["bureau"])[approved_mask],
        "occupation": np.array(backbone["occupation"])[approved_mask],
        "income_src": np.array(backbone["income_src"])[approved_mask],
        "housing":    np.array(backbone["housing"])[approved_mask],
        "emp_months": np.array(backbone["emp_months"])[approved_mask],
        "enquiries":  np.array(backbone["enquiries"])[approved_mask],
    }

    return bridge_df, fac_info


# ══════════════════════════════════════════════════════════════
# MANAGEMENT (DP_CreditManagement.mart) — 4 tables
# ══════════════════════════════════════════════════════════════
def generate_dim_product():
    """Static product reference. PL is the focus; others present for realism."""
    print(f"\n[Management] Generating DIM_product...")
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        # PRODUCT_KEY,  CODE,  NAME,                FAMILY,         MIN,  MAX,   MIN_T, MAX_T
        ("PK00000001", "PL",  "Personal Loan",     "UNSECURED",    2000, 50000, 12, 60),
        ("PK00000002", "CC",  "Credit Card",       "REVOLVING",    500,  30000, 0,  0),
        ("PK00000003", "HL",  "Home Loan",         "MORTGAGE",     50000, 1500000, 60, 360),
        ("PK00000004", "OD",  "Overdraft",         "REVOLVING",    500,  20000, 0,  0),
        ("PK00000005", "AL",  "Auto Loan",         "SECURED",      5000, 80000, 12, 84),
    ]
    return pd.DataFrame(rows, columns=[
        "PRODUCT_KEY", "PRODUCT_CODE", "PRODUCT_NAME", "PRODUCT_FAMILY",
        "MIN_AMOUNT", "MAX_AMOUNT", "MIN_TERM_MONTHS", "MAX_TERM_MONTHS",
    ]).assign(_UPDATED_AT=now_ts)


def generate_dim_facility(fac_info):
    """One row per funded facility. All point to PL product (PK00000001)."""
    n = len(fac_info["facility_keys"])
    print(f"[Management] Generating DIM_facility ({n:,} facilities)...")
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Most active, some closed/written-off — drives realistic facility_status distribution
    statuses = np.random.choice(
        ["ACTIVE", "CLOSED", "WRITTEN_OFF", "HARDSHIP"],
        n, p=[0.83, 0.10, 0.04, 0.03],
    )

    close_dates = [
        (d + timedelta(days=int(np.random.uniform(180, 720)))).strftime("%Y-%m-%d")
        if s in ("CLOSED", "WRITTEN_OFF") else None
        for d, s in zip(fac_info["funded_dates"], statuses)
    ]

    return pd.DataFrame({
        "FACILITY_KEY":        fac_info["facility_keys"],
        "FACILITY_REFERENCE":  [f"FAC-{i:08d}" for i in range(1, n + 1)],
        "CUSTOMER_ID":         fac_info["customer_ids"],
        "PRODUCT_KEY":         "PK00000001",
        "OPEN_DATE":           [d.strftime("%Y-%m-%d") for d in fac_info["funded_dates"]],
        "CLOSE_DATE":          close_dates,
        "INITIAL_LIMIT":       fac_info["funded_amounts"],
        "INTEREST_RATE":       fac_info["rates"],
        "TERM_MONTHS":         fac_info["terms"],
        "REPAYMENT_FREQUENCY": np.random.choice(["MONTHLY", "FORTNIGHTLY", "WEEKLY"], n,
                                                p=[0.65, 0.25, 0.10]),
        "AUTO_DEBIT_FLAG":     np.random.choice(["Y", "N"], n, p=[0.78, 0.22]),
        "FACILITY_STATUS":     statuses,
        "_UPDATED_AT":         now_ts,
    })


def generate_dim_snapshotdate(start_year=2022, end_year=2026):
    """One row per month-end across the observation window."""
    print(f"[Management] Generating DIM_snapshotdate ({start_year}–{end_year})...")
    rows = []
    key_counter = 1
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            d = month_end(datetime(year, month, 1))
            quarter = (month - 1) // 3 + 1
            rows.append({
                "SNAPSHOT_KEY":     f"DK{str(key_counter).zfill(6)}",
                "SNAPSHOT_DATE":    d.strftime("%Y-%m-%d"),
                "YEAR":             year,
                "QUARTER":          quarter,
                "MONTH":            month,
                "YEAR_MONTH":       d.strftime("%Y-%m"),
                "IS_MONTH_END":     "Y",
                "IS_QUARTER_END":   "Y" if month in (3, 6, 9, 12) else "N",
            })
            key_counter += 1
    return pd.DataFrame(rows)


def generate_fact_credit_facility(fac_info, dim_snapshot_df, months=PERF_MONTHS):
    """24 monthly snapshots per facility. Path (good/indet/bad) driven by risk model."""
    n_fac = len(fac_info["facility_keys"])
    print(f"[Management] Generating FACT_CreditFacility ({n_fac * months:,} rows = {n_fac:,} × {months})...")

    # Snapshot key lookup keyed on date string
    snap_key_lookup = dict(zip(dim_snapshot_df["SNAPSHOT_DATE"], dim_snapshot_df["SNAPSHOT_KEY"]))

    rows = []
    for idx in range(n_fac):
        fkey   = fac_info["facility_keys"][idx]
        funded = fac_info["funded_dates"][idx]
        amount = fac_info["funded_amounts"][idx]
        rate   = fac_info["rates"][idx]
        term   = fac_info["terms"][idx]
        bureau = fac_info["bureau"][idx]
        occ    = fac_info["occupation"][idx]
        inc    = fac_info["income_src"][idx]
        hou    = fac_info["housing"][idx]
        emp    = fac_info["emp_months"][idx]
        enq    = fac_info["enquiries"][idx]

        # Base bad probability driven by bureau score
        if bureau < 580:
            base_bad = 0.30
        elif bureau < 680:
            base_bad = 0.05
        else:
            base_bad = 0.005

        bad_p = min(0.85,
                    base_bad
                    * OCC_RISK.get(occ, 1.0)
                    * INC_RISK.get(inc, 1.0)
                    * HOU_RISK.get(hou, 1.0)
                    * (1.4 if emp < 12 else 1.1 if emp < 36 else 0.9)
                    * (1.5 if enq >= 5 else 1.2 if enq >= 3 else 1.0))
        indet_p = bad_p * 0.7
        good_p  = max(0.0, 1.0 - bad_p - indet_p)

        path = np.random.choice(["good", "indet", "bad"], p=[good_p, indet_p, bad_p])

        # Amortising payment
        monthly_payment = round(
            (amount * (rate / 100 / 12)) / (1 - (1 + rate / 100 / 12) ** -term), 2
        )

        balance = amount

        for m in range(1, months + 1):
            obs = funded + timedelta(days=30 * m)
            obs_month_end = month_end(obs).strftime("%Y-%m-%d")
            snap_key = snap_key_lookup.get(obs_month_end)
            if snap_key is None:
                # Out of DIM_snapshotdate window — skip
                continue

            if path == "good":
                arrears = 0
                hardship = "N"
            elif path == "indet":
                arrears = int(np.random.choice([0, 30, 60], p=[0.85, 0.10, 0.05]))
                hardship = "N"
            else:
                if m < 6:
                    arrears = int(np.random.choice([0, 30], p=[0.7, 0.3]))
                elif m < 12:
                    arrears = int(np.random.choice([30, 60, 90], p=[0.4, 0.4, 0.2]))
                else:
                    arrears = int(np.random.choice([90, 120, 150, 180], p=[0.4, 0.3, 0.2, 0.1]))
                hardship = "Y" if np.random.random() < 0.15 else "N"

            payment_status = "CURRENT" if arrears == 0 else f"ARREARS_{arrears}D"
            if hardship == "Y":
                payment_status = "HARDSHIP"

            # Arrears bucket label
            if arrears == 0:
                bucket = "0_DPD"
            elif arrears <= 30:
                bucket = "1_30_DPD"
            elif arrears <= 60:
                bucket = "31_60_DPD"
            elif arrears <= 89:
                bucket = "61_89_DPD"
            else:
                bucket = "90_PLUS_DPD"

            paydown = monthly_payment if arrears == 0 else monthly_payment * 0.3
            balance = max(0, round(balance - paydown, 2))
            arrears_amt = round(monthly_payment * (arrears / 30.0), 2)

            rows.append({
                "FACILITY_PERFORMANCE_ID": f"FP{fkey[2:]}{str(m).zfill(2)}",
                "FACILITY_KEY":            fkey,
                "PRODUCT_KEY":             "PK00000001",
                "SNAPSHOT_KEY":            snap_key,
                "SNAPSHOT_DATE":           obs_month_end,
                "MONTHS_SINCE_FUNDING":    m,
                "OUTSTANDING_BALANCE":     balance,
                "ARREARS_AMOUNT":          arrears_amt,
                "ARREARS_DAYS":            arrears,
                "MONTHLY_PAYMENT":         monthly_payment,
                "PAYMENT_STATUS":          payment_status,
                "HARDSHIP_FLAG":           hardship,
                "DELINQUENCY_BUCKET":      bucket,
            })

    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════
# Snowflake operations
# ══════════════════════════════════════════════════════════════
def drop_legacy(conn):
    """Drop legacy DBs from earlier seeds. Drop both quoted (mixed-case)
    and unquoted (Snowflake-folded uppercase) forms — covers DBs left
    over from earlier seed attempts that used either style."""
    cur = conn.cursor()
    print("\n[Snowflake] Dropping legacy databases...")
    legacy = [
        "ASB_ANALYTICS",
        "DP_CREDITMANAGEMENT",
        "DP_CREDITMANAGEMENT_DEV",
        "DP_CREDITMANAGEMENT_STG",
        "DP_CREDITMANAGEMENT_PRD",
        DB_APP,                              # quoted: drops mixed-case if present
        DB_MGMT,
        DB_APP.upper(),                      # unquoted: drops uppercase if present
        DB_MGMT.upper(),
    ]
    for name in legacy:
        cur.execute(f'DROP DATABASE IF EXISTS "{name}"')
    cur.close()
    print(f"  Cleaned {len(legacy)} legacy db slots")


def create_database(conn, database):
    """Create unquoted — Snowflake folds to uppercase. write_pandas() also
    sends unquoted SQL by default, so the names match. Mixed-case logical
    names in master_table_inventory.csv reach this DB via case-insensitive
    JDBC lookup."""
    cur = conn.cursor()
    print(f"\n[Snowflake] Creating {database}.{SCHEMA} (will be stored as {database.upper()})...")
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{SCHEMA}")
    cur.close()


def load_table(conn, df, database, table_name):
    print(f"    Loading {database}.{SCHEMA}.{table_name} ({len(df):,} rows)...")
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]
    success, _, nrows, _ = write_pandas(
        conn, df, table_name,
        database=database, schema=SCHEMA,
        auto_create_table=True, overwrite=True,
        quote_identifiers=False,
    )
    if success:
        print(f"      OK -- {nrows:,} rows")
    else:
        raise RuntimeError(f"write_pandas failed for {database}.{table_name}")


def verify(conn, database, expected_tables):
    cur = conn.cursor()
    print(f"\n[Verify] {database}.{SCHEMA}:")
    cur.execute(f"""
        SELECT TABLE_NAME, ROW_COUNT
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{SCHEMA}'
        ORDER BY TABLE_NAME
    """)
    found = {r[0]: r[1] for r in cur.fetchall()}
    for t in expected_tables:
        n = found.get(t.upper(), None)
        marker = "OK" if n is not None else "MISSING"
        print(f"  {t:<35} {str(n or 0):>10}  [{marker}]")
    cur.close()


def verify_target_distribution(conn):
    """Sanity check the bad-rate signal lands realistically."""
    cur = conn.cursor()
    print(f"\n[Verify] Target distribution from FACT_CreditFacility 24-month aggregate:")
    cur.execute(f"""
        WITH max_arr AS (
          SELECT FACILITY_KEY,
                 MAX(ARREARS_DAYS) AS max_a,
                 MAX(HARDSHIP_FLAG) AS hs
          FROM {DB_MGMT}.{SCHEMA}.FACT_CreditFacility
          GROUP BY FACILITY_KEY
        )
        SELECT
          CASE
            WHEN max_a >= 90 OR hs = 'Y' THEN 'Bad'
            WHEN max_a = 0                THEN 'Good'
            ELSE                                'Indeterminate'
          END AS target_flag,
          COUNT(*) AS n
        FROM max_arr GROUP BY 1 ORDER BY 1
    """)
    for label, n in cur.fetchall():
        print(f"  {label:<15} {n:>8,}")
    cur.close()


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════
def main():
    print("=" * 64)
    print("ASB PL Scorecard — Snowflake demo seed")
    print(f"  {DB_APP}.MART  (5 tables — origination)")
    print(f"  {DB_MGMT}.MART (4 tables — performance)")
    print(f"  {N_APPLICATIONS:,} applications target")
    print("=" * 64)

    np.random.seed(SEED)
    random.seed(SEED)

    print("\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(**SF)
    print("  Connected.")

    # 1. Drop legacy + create fresh DBs
    drop_legacy(conn)
    create_database(conn, DB_APP)
    create_database(conn, DB_MGMT)

    # 2. Generate origination (5 tables) + backbone
    origination_tables, backbone = generate_origination(N_APPLICATIONS)

    # 3. Generate bridge + facility seed info
    bridge_df, fac_info = generate_bridge_and_facility_seed(backbone)
    origination_tables["FACT_SMBridgeapplicationfacility"] = bridge_df

    # 4. Generate management (4 tables)
    dim_product   = generate_dim_product()
    dim_facility  = generate_dim_facility(fac_info)
    dim_snapshot  = generate_dim_snapshotdate(start_year=2022, end_year=2026)
    fact_perf     = generate_fact_credit_facility(fac_info, dim_snapshot)

    management_tables = {
        "DIM_product":        dim_product,
        "DIM_facility":       dim_facility,
        "DIM_snapshotdate":   dim_snapshot,
        "FACT_CreditFacility": fact_perf,
    }

    # 5. Load to Snowflake
    print(f"\n[Load] Writing origination tables to {DB_APP}.{SCHEMA}...")
    for tbl, df in origination_tables.items():
        load_table(conn, df, DB_APP, tbl)

    print(f"\n[Load] Writing management tables to {DB_MGMT}.{SCHEMA}...")
    for tbl, df in management_tables.items():
        load_table(conn, df, DB_MGMT, tbl)

    # 6. Verify
    verify(conn, DB_APP, [
        "DIM_SMApplicationRequest",
        "DIM_SMOnyxApplication",
        "DIM_SMApplicationRequestSummary",
        "FACT_SMApplicationRequest",
        "FACT_SMBridgeapplicationfacility",
    ])
    verify(conn, DB_MGMT, [
        "DIM_product",
        "DIM_facility",
        "DIM_snapshotdate",
        "FACT_CreditFacility",
    ])
    verify_target_distribution(conn)

    conn.close()

    print("\n" + "=" * 64)
    print("SEED COMPLETE")
    print("=" * 64)
    print(f"  Origination DB:  {DB_APP}.{SCHEMA}")
    print(f"  Management DB:   {DB_MGMT}.{SCHEMA}")
    print(f"  Applications:    {N_APPLICATIONS:,}")
    print(f"  Approved:        ~{int(N_APPLICATIONS * APPROVAL_RATE):,}")
    print(f"  Performance:     ~{int(N_APPLICATIONS * APPROVAL_RATE * PERF_MONTHS):,} rows")
    print(f"  Next: update master_table_inventory.csv, then deploy bundle")


if __name__ == "__main__":
    main()
