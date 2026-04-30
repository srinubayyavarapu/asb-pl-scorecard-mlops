"""Quick Snowflake connectivity probe — prints account state."""
import snowflake.connector

CFG = {
    "account":  "fsultcl-ht17125",
    "user":     "SRINUBAYYAVARAPU",
    "password": "Srinubayyavarapu5657",
}

with snowflake.connector.connect(**CFG) as conn:
    cur = conn.cursor()
    cur.execute(
        "SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_USER(), "
        "CURRENT_ROLE(), CURRENT_WAREHOUSE()"
    )
    acc, region, user, role, wh = cur.fetchone()
    print(f"Account:   {acc}")
    print(f"Region:    {region}")
    print(f"User:      {user}")
    print(f"Role:      {role}")
    print(f"Warehouse: {wh}")

    print("\nDatabases:")
    cur.execute("SHOW DATABASES")
    for r in cur.fetchall():
        print(f"  - {r[1]}")

    print("\nWarehouses:")
    cur.execute("SHOW WAREHOUSES")
    for r in cur.fetchall():
        print(f"  - {r[0]}  ({r[2]}, state={r[1]})")

    print("\nIf ASB_ANALYTICS exists, schemas:")
    try:
        cur.execute("SHOW SCHEMAS IN DATABASE ASB_ANALYTICS")
        for r in cur.fetchall():
            print(f"  - {r[1]}")
    except Exception as e:
        print(f"  (no ASB_ANALYTICS: {e})")

    print("\nIf DP_CREDITMANAGEMENT exists, schemas:")
    try:
        cur.execute("SHOW SCHEMAS IN DATABASE DP_CREDITMANAGEMENT")
        for r in cur.fetchall():
            print(f"  - {r[1]}")
    except Exception as e:
        print(f"  (no DP_CREDITMANAGEMENT: {e})")
