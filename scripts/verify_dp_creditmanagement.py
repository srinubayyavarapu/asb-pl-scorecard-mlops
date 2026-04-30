"""Verify DP_CREDITMANAGEMENT.MART contents (post-seed sanity checks)."""
import snowflake.connector

SF = {
    "account":   "fsultcl-ht17125",
    "user":      "SRINUBAYYAVARAPU",
    "password":  "Srinubayyavarapu5657",
    "warehouse": "COMPUTE_WH",
    "role":      "ACCOUNTADMIN",
    "database":  "DP_CREDITMANAGEMENT",
    "schema":    "MART",
}

with snowflake.connector.connect(**SF) as conn:
    cur = conn.cursor()

    print("=" * 64)
    print("DP_CREDITMANAGEMENT.MART verification")
    print("=" * 64)

    print("\n[1] Table inventory:")
    cur.execute("""
        SELECT TABLE_NAME, ROW_COUNT, BYTES
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'MART' ORDER BY TABLE_NAME
    """)
    for tbl, n, b in cur.fetchall():
        print(f"  {tbl:<22} {n:>10,} rows   ({b/1024:>8,.0f} KB)")

    print("\n[2] APPLICATIONS - decision split (TTD population):")
    cur.execute("SELECT APPLICATION_DECISION, COUNT(*) FROM APPLICATIONS GROUP BY 1 ORDER BY 1")
    for d, n in cur.fetchall():
        print(f"  {d:<10} {n:>8,}")

    print("\n[3] APPLICATIONS - PIE_OCCUPATION distribution (top 5):")
    cur.execute("""
        SELECT PIE_OCCUPATION, COUNT(*) AS n
        FROM APPLICATIONS GROUP BY 1 ORDER BY n DESC LIMIT 5
    """)
    for p, n in cur.fetchall():
        print(f"  {p:<18} {n:>6,}")

    print("\n[4] FACILITIES - status distribution:")
    cur.execute("SELECT FACILITY_STATUS, COUNT(*) FROM FACILITIES GROUP BY 1 ORDER BY 2 DESC")
    for s, n in cur.fetchall():
        print(f"  {s:<14} {n:>8,}")

    print("\n[5] CREDIT_PERFORMANCE - shape:")
    cur.execute("""
        SELECT COUNT(DISTINCT FACILITY_ID), MIN(MONTHS_SINCE_FUNDING),
               MAX(MONTHS_SINCE_FUNDING), MIN(OBSERVATION_DATE), MAX(OBSERVATION_DATE)
        FROM CREDIT_PERFORMANCE
    """)
    f, lo, hi, dmin, dmax = cur.fetchone()
    print(f"  facilities={f:,}  months_since_funding=[{lo}..{hi}]  obs_date=[{dmin}..{dmax}]")

    print("\n[6] CREDIT_PERFORMANCE - payment status mix:")
    cur.execute("""
        SELECT PAYMENT_STATUS, COUNT(*) AS n
        FROM CREDIT_PERFORMANCE GROUP BY 1 ORDER BY n DESC
    """)
    for s, n in cur.fetchall():
        print(f"  {s:<16} {n:>10,}")

    print("\n[7] Target derivation preview (max arrears in 24-mo window per facility):")
    cur.execute("""
        WITH max_arr AS (
          SELECT FACILITY_ID,
                 MAX(ARREARS_DAYS) AS max_a,
                 MAX(HARDSHIP_FLAG) AS hs
          FROM CREDIT_PERFORMANCE GROUP BY 1
        )
        SELECT
          CASE
            WHEN max_a >= 90 OR hs = 'Y' THEN 'Bad'
            WHEN max_a = 0 THEN 'Good'
            ELSE 'Indeterminate'
          END AS target_flag,
          COUNT(*) AS n,
          ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM max_arr GROUP BY 1 ORDER BY 1
    """)
    for label, n, pct in cur.fetchall():
        print(f"  {label:<14} {n:>8,}  ({pct}%)")

    print("\n[8] SAS_FINAL_SCORES - score distribution:")
    cur.execute("""
        SELECT SAS_DECISION, COUNT(*),
               ROUND(MIN(SAS_FINAL_P_GOOD), 4),
               ROUND(AVG(SAS_FINAL_P_GOOD), 4),
               ROUND(MAX(SAS_FINAL_P_GOOD), 4)
        FROM SAS_FINAL_SCORES GROUP BY 1
    """)
    for d, n, lo, avg, hi in cur.fetchall():
        print(f"  {d:<10} {n:>6,}  p_good=[{lo}..{hi}]  avg={avg}")

    print("\n[9] Join sanity (APPLICATIONS -> FACILITIES -> CREDIT_PERFORMANCE):")
    cur.execute("""
        SELECT
          (SELECT COUNT(*) FROM APPLICATIONS WHERE APPLICATION_DECISION = 'APPROVED'),
          (SELECT COUNT(*) FROM FACILITIES),
          (SELECT COUNT(DISTINCT f.APPLICATION_ID)
             FROM FACILITIES f JOIN APPLICATIONS a ON f.APPLICATION_ID = a.APPLICATION_ID),
          (SELECT COUNT(DISTINCT cp.FACILITY_ID)
             FROM CREDIT_PERFORMANCE cp JOIN FACILITIES f ON cp.FACILITY_ID = f.FACILITY_ID)
    """)
    a, f, j1, j2 = cur.fetchone()
    print(f"  approved apps        = {a:,}")
    print(f"  facilities           = {f:,}")
    print(f"  app->fac join hits   = {j1:,}  (should equal facilities)")
    print(f"  fac->perf join hits  = {j2:,}  (should equal facilities)")

    print("\n" + "=" * 64)
    print("VERIFICATION COMPLETE")
    print("=" * 64)
