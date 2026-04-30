"""
================================================================
Seed DP_CREDITMANAGEMENT_{DEV,STG,PRD}.MART — ASB PL App Scorecard
================================================================
Best-practice env isolation: separate Snowflake databases per env.

  DP_CREDITMANAGEMENT_DEV.MART     ~5,000  applications  (dev sample)
  DP_CREDITMANAGEMENT_STG.MART    ~30,000  applications  (stg full)
  DP_CREDITMANAGEMENT_PRD.MART   ~100,000  applications  (prd full-scale)

Each db carries the same 4 tables:
  APPLICATIONS         TTD (funded + rejected)
  FACILITIES           funded loans only
  CREDIT_PERFORMANCE  24 monthly obs per facility
  SAS_FINAL_SCORES    reference scores from current SAS model

Run once locally:
  python scripts/seed_dp_creditmanagement.py
================================================================
"""

import numpy as np
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta
import random

# ──────────────────────────────────────────────────────────────
# Snowflake connection
# ──────────────────────────────────────────────────────────────
SF = {
    "account":   "fsultcl-ht17125",
    "user":      "SRINUBAYYAVARAPU",
    "password":  "Srinubayyavarapu5657",
    "warehouse": "COMPUTE_WH",
    "role":      "ACCOUNTADMIN",
}

# Tiered volume per environment — best-practice env isolation
ENV_DATABASES = {
    "DEV": ("DP_CREDITMANAGEMENT_DEV",   5_000),
    "STG": ("DP_CREDITMANAGEMENT_STG",  30_000),
    "PRD": ("DP_CREDITMANAGEMENT_PRD", 100_000),
}
SCHEMA = "MART"

APPROVAL_RATE  = 0.833           # ~83% funded out of TTD
PERF_MONTHS    = 24              # 24-month performance window per facility
SEED           = 42

# Note: seed reset per env in main() so each env gets reproducible but distinct data

# ──────────────────────────────────────────────────────────────
# Categorical universes (PIE_* per walkthrough)
# ──────────────────────────────────────────────────────────────
PIE_OCCUPATION = [
    "Professional", "Manager", "Clerical", "Trades", "Sales",
    "Labourer", "Self-Employed", "Retired", "Student", "Other",
]
PIE_INCOMESOURCE = ["Salary", "SelfEmployed", "Pension", "Investment", "Benefit", "Mixed"]
PIE_ACCOMODATOR  = ["OwnerOccupier", "Mortgaged", "RentingPrivate", "RentingPublic", "BoardingFamily"]
LOAN_PURPOSE     = ["DebtConsolidation", "HomeImprovement", "Vehicle", "Holiday", "Medical", "Other"]
REGION           = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Tauranga", "Other"]
MARITAL          = ["Single", "Married", "DeFacto", "Divorced", "Widowed"]


# ══════════════════════════════════════════════════════════════
# 1. APPLICATIONS — TTD (funded + rejected), one row per application
# ══════════════════════════════════════════════════════════════
def generate_applications(n):
    print(f"  Generating APPLICATIONS ({n:,} rows — TTD)...")

    # Application dates spread across 24 months (2022-01 to 2023-12)
    # so the 24-month performance window for the latest funded apps lands in 2025
    base = datetime(2022, 1, 1)
    app_dates = [base + timedelta(days=int(np.random.uniform(0, 730))) for _ in range(n)]

    # Bureau credit score drives approval probability — realistic skew
    bureau_score = np.clip(np.random.normal(680, 90, n), 300, 850).astype(int)
    approve_prob = 1 / (1 + np.exp(-(bureau_score - 600) / 40))   # logistic
    approved = np.random.uniform(0, 1, n) < approve_prob

    # Force overall approval rate to land near APPROVAL_RATE
    target_approved = int(n * APPROVAL_RATE)
    if approved.sum() != target_approved:
        order = np.argsort(-approve_prob)        # rank by score
        approved = np.zeros(n, dtype=bool)
        approved[order[:target_approved]] = True

    df = pd.DataFrame({
        "APPLICATION_ID":    [f"APP{str(i).zfill(8)}" for i in range(1, n + 1)],
        "CUSTOMER_ID":       [f"CUST{random.randint(100000, 999999)}" for _ in range(n)],
        "APPLICATION_DATE":  [d.strftime("%Y-%m-%d") for d in app_dates],
        "DP3":               [
            (d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            for d in app_dates
        ],
        "PRODUCT_TYPE":      "PERSONAL_LOAN",
        "LOAN_AMOUNT_REQUESTED": np.round(np.random.uniform(2_000, 50_000, n), 2),
        "LOAN_PURPOSE":      np.random.choice(LOAN_PURPOSE, n,
                              p=[0.30, 0.20, 0.20, 0.15, 0.05, 0.10]),
        "APPLICATION_DECISION": np.where(approved, "APPROVED", "DECLINED"),

        # PIE_* character variables (walkthrough names)
        "PIE_OCCUPATION":    np.random.choice(PIE_OCCUPATION, n,
                              p=[0.18, 0.12, 0.15, 0.12, 0.10, 0.10, 0.10, 0.06, 0.03, 0.04]),
        "PIE_INCOMESOURCE":  np.random.choice(PIE_INCOMESOURCE, n,
                              p=[0.55, 0.18, 0.10, 0.05, 0.07, 0.05]),
        "PIE_ACCOMODATOR":   np.random.choice(PIE_ACCOMODATOR, n,
                              p=[0.30, 0.30, 0.25, 0.10, 0.05]),

        # Demographics + financials
        "ANNUAL_INCOME":     np.round(np.random.lognormal(11.0, 0.45, n), 2).clip(15_000, 400_000),
        "AGE":               np.random.randint(18, 75, n),
        "NUM_DEPENDANTS":    np.random.choice([0, 1, 2, 3, 4, 5], n,
                              p=[0.30, 0.25, 0.20, 0.15, 0.07, 0.03]),
        "MARITAL_STATUS":    np.random.choice(MARITAL, n,
                              p=[0.32, 0.42, 0.12, 0.10, 0.04]),
        "REGION":            np.random.choice(REGION, n,
                              p=[0.35, 0.20, 0.15, 0.10, 0.08, 0.12]),
        "TIME_AT_ADDRESS_MONTHS":  np.random.randint(0, 240, n),
        "TIME_AT_EMPLOYER_MONTHS": np.random.randint(0, 360, n),
        "NUM_CREDIT_ENQUIRIES":    np.random.choice(range(0, 12), n,
                                    p=[0.30, 0.25, 0.18, 0.10, 0.06, 0.04, 0.03, 0.02, 0.01, 0.005, 0.003, 0.002]),
        "EXISTING_DEBT":     np.round(np.random.uniform(0, 150_000, n), 2),
        "CREDIT_SCORE_BUREAU": bureau_score,

        "_UPDATED_AT": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * n,
    })
    return df


# ══════════════════════════════════════════════════════════════
# 2. FACILITIES — funded only, joins to APPLICATIONS on application_id
# ══════════════════════════════════════════════════════════════
def generate_facilities(applications_df):
    funded = applications_df[applications_df["APPLICATION_DECISION"] == "APPROVED"].copy()
    n = len(funded)
    print(f"  Generating FACILITIES ({n:,} rows — funded only)...")

    # Funded date = application_date + 1-7 days
    funded_dates = [
        datetime.strptime(d, "%Y-%m-%d") + timedelta(days=int(np.random.uniform(1, 8)))
        for d in funded["APPLICATION_DATE"].values
    ]

    # Funded amount = requested ± 10%
    requested = funded["LOAN_AMOUNT_REQUESTED"].values
    funded_amount = np.round(requested * np.random.uniform(0.90, 1.0, n), 2)

    df = pd.DataFrame({
        "FACILITY_ID":         [f"FAC{str(i).zfill(8)}" for i in range(1, n + 1)],
        "APPLICATION_ID":      funded["APPLICATION_ID"].values,
        "CUSTOMER_ID":         funded["CUSTOMER_ID"].values,
        "FUNDED_DATE":         [d.strftime("%Y-%m-%d") for d in funded_dates],
        "LOAN_AMOUNT_FUNDED":  funded_amount,
        "INTEREST_RATE":       np.round(np.random.uniform(7.5, 22.0, n), 2),
        "LOAN_TERM_MONTHS":    np.random.choice([12, 24, 36, 48, 60], n,
                                p=[0.10, 0.25, 0.30, 0.20, 0.15]),
        "REPAYMENT_FREQUENCY": np.random.choice(["MONTHLY", "FORTNIGHTLY", "WEEKLY"], n,
                                p=[0.65, 0.25, 0.10]),
        "AUTO_DEBIT_FLAG":     np.random.choice(["Y", "N"], n, p=[0.78, 0.22]),
        "FACILITY_STATUS":     np.random.choice(
                                ["ACTIVE", "CLOSED", "WRITTEN_OFF", "HARDSHIP"],
                                n, p=[0.83, 0.10, 0.04, 0.03]),
        "_UPDATED_AT":         [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * n,
    })
    return df


# ══════════════════════════════════════════════════════════════
# 3. CREDIT_PERFORMANCE — 24 monthly obs per facility
# Drives the Good/Bad/Indeterminate target downstream.
# ══════════════════════════════════════════════════════════════
def generate_credit_performance(facilities_df, applications_df, months=PERF_MONTHS):
    n_fac = len(facilities_df)
    print(f"  Generating CREDIT_PERFORMANCE ({n_fac * months:,} rows = {n_fac:,} × {months} months)...")

    # Index applications by id for bureau-score lookup → drives bad-rate realism
    bureau_lookup = dict(zip(applications_df["APPLICATION_ID"], applications_df["CREDIT_SCORE_BUREAU"]))

    rows = []

    # Pre-compute base bad rate per facility — lower bureau score → higher bad probability
    fac_records = facilities_df.to_dict("records")

    # Build lookups so other PIE_* / demographic features carry real signal,
    # not just bureau score. This mirrors what ASB sees in production data.
    occupation_lookup    = dict(zip(applications_df["APPLICATION_ID"], applications_df["PIE_OCCUPATION"]))
    incomesource_lookup  = dict(zip(applications_df["APPLICATION_ID"], applications_df["PIE_INCOMESOURCE"]))
    accomodator_lookup   = dict(zip(applications_df["APPLICATION_ID"], applications_df["PIE_ACCOMODATOR"]))
    employer_lookup      = dict(zip(applications_df["APPLICATION_ID"], applications_df["TIME_AT_EMPLOYER_MONTHS"]))
    enquiries_lookup     = dict(zip(applications_df["APPLICATION_ID"], applications_df["NUM_CREDIT_ENQUIRIES"]))

    # Risk multipliers per categorical level — realistic credit-risk patterns
    OCC_BAD = {"Labourer": 1.4, "Self-Employed": 1.3, "Student": 1.5, "Sales": 1.2,
               "Trades": 1.0, "Clerical": 0.9, "Manager": 0.7, "Professional": 0.6,
               "Retired": 0.8, "Other": 1.1}
    INC_BAD = {"Benefit": 1.8, "SelfEmployed": 1.4, "Mixed": 1.2, "Salary": 0.8,
               "Investment": 0.7, "Pension": 0.9}
    ACC_BAD = {"RentingPublic": 1.6, "BoardingFamily": 1.3, "RentingPrivate": 1.2,
               "Mortgaged": 0.9, "OwnerOccupier": 0.7}

    for fac in fac_records:
        fid     = fac["FACILITY_ID"]
        appid   = fac["APPLICATION_ID"]
        funded  = datetime.strptime(fac["FUNDED_DATE"], "%Y-%m-%d")
        amount  = fac["LOAN_AMOUNT_FUNDED"]
        rate    = fac["INTEREST_RATE"]
        term    = fac["LOAN_TERM_MONTHS"]
        bureau  = bureau_lookup.get(appid, 650)

        # Base bad probability driven by bureau score (dominant — realistic)
        # then modulated by PIE_* and stability features.
        if bureau < 580:
            base_bad = 0.30
        elif bureau < 680:
            base_bad = 0.05
        else:
            base_bad = 0.005

        # Multipliers from categoricals
        occ_mult = OCC_BAD.get(occupation_lookup.get(appid, "Other"), 1.0)
        inc_mult = INC_BAD.get(incomesource_lookup.get(appid, "Salary"), 1.0)
        acc_mult = ACC_BAD.get(accomodator_lookup.get(appid, "OwnerOccupier"), 1.0)

        # Stability multipliers — short employer tenure / many enquiries → riskier
        employer_months = employer_lookup.get(appid, 60)
        enquiries       = enquiries_lookup.get(appid, 1)
        emp_mult = 1.4 if employer_months < 12 else 1.1 if employer_months < 36 else 0.9
        enq_mult = 1.5 if enquiries >= 5 else 1.2 if enquiries >= 3 else 1.0

        bad_p = min(0.85, base_bad * occ_mult * inc_mult * acc_mult * emp_mult * enq_mult)
        # Indeterminate proportional to a fraction of bad (always smaller)
        indet_p = bad_p * 0.7
        good_p  = max(0.0, 1.0 - bad_p - indet_p)

        path = np.random.choice(["good", "indet", "bad"], p=[good_p, indet_p, bad_p])

        # Monthly amortising payment
        monthly_payment = round(
            (amount * (rate / 100 / 12)) / (1 - (1 + rate / 100 / 12) ** -term), 2
        )

        balance = amount

        for m in range(1, months + 1):
            obs = funded + timedelta(days=30 * m)
            dp3 = (obs.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

            # Determine arrears for this month based on path
            if path == "good":
                arrears = 0
                hardship = "N"
            elif path == "indet":
                # 30-89 day arrears in 1-3 random months
                arrears = np.random.choice([0, 30, 60], p=[0.85, 0.10, 0.05])
                hardship = "N"
            else:  # bad — arrears escalate to 90+ at some point in window
                if m < 6:
                    arrears = np.random.choice([0, 30], p=[0.7, 0.3])
                elif m < 12:
                    arrears = np.random.choice([30, 60, 90], p=[0.4, 0.4, 0.2])
                else:
                    arrears = np.random.choice([90, 120, 150, 180], p=[0.4, 0.3, 0.2, 0.1])
                hardship = "Y" if np.random.random() < 0.15 else "N"

            payment_status = "CURRENT" if arrears == 0 else f"ARREARS_{arrears}D"
            if hardship == "Y":
                payment_status = "HARDSHIP"

            # Balance pays down (slowly when in arrears)
            paydown = monthly_payment if arrears == 0 else monthly_payment * 0.3
            balance = max(0, round(balance - paydown, 2))

            rows.append({
                "PERFORMANCE_ID":       f"PERF{fid[3:]}{str(m).zfill(2)}",
                "FACILITY_ID":          fid,
                "OBSERVATION_DATE":     obs.strftime("%Y-%m-%d"),
                "DP3":                  dp3,
                "MONTHS_SINCE_FUNDING": m,
                "ARREARS_DAYS":         arrears,
                "OUTSTANDING_BALANCE":  balance,
                "MONTHLY_PAYMENT":      monthly_payment,
                "PAYMENT_STATUS":       payment_status,
                "HARDSHIP_FLAG":        hardship,
            })

    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════
# 4. SAS_FINAL_SCORES — reference scores from existing SAS model
# Used downstream for SAS-vs-DBx reconciliation (regulatory)
# ══════════════════════════════════════════════════════════════
def generate_sas_final_scores(facilities_df, applications_df):
    n = len(facilities_df)
    print(f"  Generating SAS_FINAL_SCORES ({n:,} rows — funded apps)...")

    # SAS Final p-good is correlated with bureau score but not identical
    bureau_lookup = dict(zip(applications_df["APPLICATION_ID"], applications_df["CREDIT_SCORE_BUREAU"]))
    bureau = facilities_df["APPLICATION_ID"].map(bureau_lookup).values

    # SAS p-good in [0.3, 0.99], correlated to bureau, with noise
    sas_p_good = np.clip(
        (bureau - 300) / 550 + np.random.normal(0, 0.05, n),
        0.30, 0.99,
    ).round(4)

    df = pd.DataFrame({
        "APPLICATION_ID":    facilities_df["APPLICATION_ID"].values,
        "FACILITY_ID":       facilities_df["FACILITY_ID"].values,
        "SAS_FINAL_P_GOOD":  sas_p_good,
        "SAS_PD_ESTIMATE":   (1 - sas_p_good).round(4),
        "SAS_DECISION":      np.where(sas_p_good >= 0.55, "APPROVE", "DECLINE"),
        "SAS_MODEL_VERSION": "SAS_PL_APP_v3.2",
        "SCORED_AT":         [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * n,
    })
    return df


# ══════════════════════════════════════════════════════════════
# Snowflake operations
# ══════════════════════════════════════════════════════════════
def drop_legacy(conn):
    """Drop the legacy single-DB and the previously-shared ASB_ANALYTICS."""
    cur = conn.cursor()
    print("\n[Snowflake] Dropping legacy databases...")
    for legacy in ("ASB_ANALYTICS", "DP_CREDITMANAGEMENT"):
        cur.execute(f"DROP DATABASE IF EXISTS {legacy}")
        print(f"  Dropped {legacy} (if existed)")
    cur.close()


def create_env_database(conn, database):
    cur = conn.cursor()
    print(f"\n[Snowflake] Creating {database}.{SCHEMA}...")
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{SCHEMA}")
    cur.execute(f"USE DATABASE {database}")
    cur.execute(f"USE SCHEMA {SCHEMA}")
    print(f"  {database}.{SCHEMA} ready")
    cur.close()


def load_table(conn, df, database, table_name):
    print(f"    Loading {database}.{SCHEMA}.{table_name} ({len(df):,} rows)...")
    df.columns = [c.upper() for c in df.columns]
    success, _, nrows, _ = write_pandas(
        conn, df, table_name,
        database=database, schema=SCHEMA,
        auto_create_table=True, overwrite=True,
    )
    if success:
        print(f"      OK -- {nrows:,} rows")
    else:
        raise RuntimeError(f"write_pandas failed for {database}.{table_name}")


def verify_database(conn, database):
    cur = conn.cursor()
    print(f"\n[Verify] {database}.{SCHEMA}:")
    cur.execute(f"""
        SELECT TABLE_NAME, ROW_COUNT
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{SCHEMA}'
        ORDER BY TABLE_NAME
    """)
    for tbl, n in cur.fetchall():
        print(f"  {tbl:<22} {n:>10,} rows")

    cur.execute(f"""
        WITH max_arr AS (
          SELECT FACILITY_ID, MAX(ARREARS_DAYS) AS max_a, MAX(HARDSHIP_FLAG) AS hs
          FROM {database}.{SCHEMA}.CREDIT_PERFORMANCE GROUP BY 1
        )
        SELECT
          CASE
            WHEN max_a >= 90 OR hs = 'Y' THEN 'Bad'
            WHEN max_a = 0 THEN 'Good'
            ELSE 'Indeterminate'
          END AS target_flag,
          COUNT(*) AS n
        FROM max_arr GROUP BY 1 ORDER BY 1
    """)
    print(f"  Target distribution:")
    for label, n in cur.fetchall():
        print(f"    {label:<14} {n:>8,}")
    cur.close()


def seed_environment(conn, env_name, database, n_apps, env_seed):
    """Generate + load all 4 tables for one environment."""
    print(f"\n{'='*64}")
    print(f"  ENVIRONMENT: {env_name}  ->  {database}  ({n_apps:,} applications)")
    print(f"{'='*64}")

    # Reset RNG seed so each env produces reproducible but distinct data
    np.random.seed(env_seed)
    random.seed(env_seed)

    print("\n  [1/3] Generating synthetic data...")
    apps   = generate_applications(n_apps)
    facs   = generate_facilities(apps)
    perf   = generate_credit_performance(facs, apps)
    scores = generate_sas_final_scores(facs, apps)
    print(f"    APPLICATIONS:        {len(apps):>10,}")
    print(f"    FACILITIES:          {len(facs):>10,}")
    print(f"    CREDIT_PERFORMANCE:  {len(perf):>10,}")
    print(f"    SAS_FINAL_SCORES:    {len(scores):>10,}")

    print("\n  [2/3] Creating database + schema...")
    create_env_database(conn, database)

    print("\n  [3/3] Loading tables...")
    load_table(conn, apps,   database, "APPLICATIONS")
    load_table(conn, facs,   database, "FACILITIES")
    load_table(conn, perf,   database, "CREDIT_PERFORMANCE")
    load_table(conn, scores, database, "SAS_FINAL_SCORES")


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════
def main():
    print("=" * 64)
    print("ASB Bank -- Seed DP_CREDITMANAGEMENT_{DEV,STG,PRD}.MART")
    print("Tiered env isolation per Databricks UC best practice")
    print("=" * 64)

    conn = snowflake.connector.connect(**SF)
    print("Connected to Snowflake")

    drop_legacy(conn)

    # Distinct seed per env -> reproducible-but-different rows across environments
    env_seeds = {"DEV": 42, "STG": 43, "PRD": 44}
    for env_name, (database, n_apps) in ENV_DATABASES.items():
        seed_environment(conn, env_name, database, n_apps, env_seeds[env_name])

    print(f"\n{'='*64}\nVERIFICATION\n{'='*64}")
    for env_name, (database, _) in ENV_DATABASES.items():
        verify_database(conn, database)

    conn.close()

    print("\n" + "=" * 64)
    print("SEED COMPLETE")
    print("=" * 64)
    for env_name, (database, n) in ENV_DATABASES.items():
        print(f"  {env_name:<5} -> {database:<30} (~{n:,} applications)")
    print(f"  Schema:    {SCHEMA}")
    print(f"  Tables:    APPLICATIONS, FACILITIES, CREDIT_PERFORMANCE, SAS_FINAL_SCORES")
    print(f"  Next:      Re-deploy bundles (vars now point at env-specific DBs)")


if __name__ == "__main__":
    main()
