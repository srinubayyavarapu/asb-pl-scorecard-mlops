"""
Script to add incremental records to test SCD2 logic.
This adds ~200 records including:
- 120 updates to KNOWN existing employees (deterministic IDs for validation)
- 80 new employees (INSERT scenario)

SCD2 Validation:
  After running this script + the ETL pipeline (incremental mode),
  the Silver table should have:
  - Updated employees: 2 rows each (old closed, new current)
  - New employees: 1 row each (current)
  - Unchanged employees: 1 row each (untouched)

Specific IDs to verify after SCD2:
  EMP000001 - EMP000010: PROMOTION (job title + salary changed)
  EMP000011 - EMP000020: DEPARTMENT_TRANSFER (department + job title changed)
  EMP000021 - EMP000030: SALARY_ADJUSTMENT (salary changed)
  EMP000031 - EMP000040: STATUS_CHANGE (employment status changed)
  EMP000041 - EMP000050: ADDRESS_CHANGE (address fields changed)
  EMP000051 - EMP000060: MANAGER_CHANGE (manager_id changed)
  EMP000061 - EMP000120: RANDOM update scenario
  EMP004001 - EMP004080: NEW_HIRE (brand new records)
"""

import snowflake.connector
from faker import Faker
import random
from datetime import datetime, timedelta

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    "account": "fsultcl-ht17125",
    "user": "SRINUBAYYAVARAPU",
    "password": "Srinubayyavarapu5657",
    "warehouse": "COMPUTE_WH",
    "database": "ASB_ANALYTICS",
    "schema": "GDW"
}

# Table name
TABLE_NAME = "EMPLOYEE_MASTER"

# Initialize Faker with different seed for variation
fake = Faker()
Faker.seed(99)
random.seed(99)

# Reference data
DEPARTMENTS = [
    "Engineering", "Sales", "Marketing", "Finance", "Human Resources",
    "Operations", "Legal", "Customer Support", "Product", "Data Science",
    "IT Infrastructure", "Quality Assurance", "Research", "Administration"
]

JOB_TITLES = {
    "Engineering": ["Software Engineer", "Senior Software Engineer", "Staff Engineer", "Engineering Manager", "Principal Engineer"],
    "Sales": ["Sales Representative", "Account Executive", "Sales Manager", "Regional Sales Director", "VP of Sales"],
    "Marketing": ["Marketing Analyst", "Marketing Manager", "Content Specialist", "Brand Manager", "CMO"],
    "Finance": ["Financial Analyst", "Senior Accountant", "Finance Manager", "Controller", "CFO"],
    "Human Resources": ["HR Coordinator", "HR Manager", "Recruiter", "HR Business Partner", "CHRO"],
    "Operations": ["Operations Analyst", "Operations Manager", "Supply Chain Manager", "COO", "Process Engineer"],
    "Legal": ["Paralegal", "Legal Counsel", "Senior Attorney", "General Counsel", "Compliance Officer"],
    "Customer Support": ["Support Representative", "Support Lead", "Support Manager", "Customer Success Manager", "VP of Support"],
    "Product": ["Product Analyst", "Product Manager", "Senior Product Manager", "Director of Product", "CPO"],
    "Data Science": ["Data Analyst", "Data Scientist", "Senior Data Scientist", "ML Engineer", "Head of Data Science"],
    "IT Infrastructure": ["System Administrator", "Network Engineer", "DevOps Engineer", "IT Manager", "CTO"],
    "Quality Assurance": ["QA Analyst", "QA Engineer", "Senior QA Engineer", "QA Manager", "Director of QA"],
    "Research": ["Research Associate", "Research Scientist", "Senior Researcher", "Research Director", "Chief Scientist"],
    "Administration": ["Administrative Assistant", "Office Manager", "Executive Assistant", "Facilities Manager", "COO"]
}

SALARY_RANGES = {
    "Junior": (45000, 70000),
    "Mid": (70000, 100000),
    "Senior": (100000, 150000),
    "Manager": (120000, 180000),
    "Executive": (180000, 350000)
}

US_STATES = [
    "CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI"
]

# Deterministic update plan: specific IDs -> specific scenarios
# This makes SCD2 validation predictable
DETERMINISTIC_UPDATES = {
    "promotion":           list(range(1, 11)),    # EMP000001 - EMP000010
    "department_transfer": list(range(11, 21)),   # EMP000011 - EMP000020
    "salary_adjustment":   list(range(21, 31)),   # EMP000021 - EMP000030
    "status_change":       list(range(31, 41)),   # EMP000031 - EMP000040
    "address_change":      list(range(41, 51)),   # EMP000041 - EMP000050
    "manager_change":      list(range(51, 61)),   # EMP000051 - EMP000060
}

# Additional random updates for broader coverage
RANDOM_UPDATE_IDS = list(range(61, 121))  # EMP000061 - EMP000120

UPDATE_SCENARIOS = list(DETERMINISTIC_UPDATES.keys())


def get_salary_level(job_title: str) -> str:
    """Determine salary level based on job title."""
    title_lower = job_title.lower()
    if any(word in title_lower for word in ["chief", "cfo", "cto", "coo", "cmo", "cpo", "chro", "vp", "director"]):
        return "Executive"
    elif any(word in title_lower for word in ["manager", "head", "lead"]):
        return "Manager"
    elif any(word in title_lower for word in ["senior", "staff", "principal"]):
        return "Senior"
    elif any(word in title_lower for word in ["analyst", "representative", "coordinator", "associate"]):
        return "Junior"
    else:
        return "Mid"


def generate_new_employee(employee_id: int) -> dict:
    """Generate a new employee record (for INSERT scenario)."""
    department = random.choice(DEPARTMENTS)
    job_title = random.choice(JOB_TITLES[department])
    salary_level = get_salary_level(job_title)
    salary_range = SALARY_RANGES[salary_level]

    days_employed = random.randint(1, 30)
    hire_date = datetime.now() - timedelta(days=days_employed)
    last_modified = datetime.now()

    return {
        "employee_id": f"EMP{employee_id:06d}",
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number()[:20],
        "department": department,
        "job_title": job_title,
        "salary": round(random.uniform(*salary_range), 2),
        "employment_status": "Probation",
        "hire_date": hire_date.strftime("%Y-%m-%d"),
        "manager_id": f"EMP{random.randint(1, 500):06d}",
        "street_address": fake.street_address(),
        "city": fake.city(),
        "state": random.choice(US_STATES),
        "zip_code": fake.zipcode(),
        "country": "USA",
        "last_modified_date": last_modified.strftime("%Y-%m-%d %H:%M:%S"),
        "change_type": "NEW_HIRE"
    }


def apply_update_scenario(record: dict, scenario: str) -> dict:
    """Apply an update scenario to an existing record for SCD2 testing."""
    updated = record.copy()
    updated["last_modified_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if scenario == "promotion":
        current_dept = updated["department"]
        titles = JOB_TITLES.get(current_dept, ["Manager"])
        current_idx = titles.index(updated["job_title"]) if updated["job_title"] in titles else 0
        if current_idx < len(titles) - 1:
            updated["job_title"] = titles[current_idx + 1]
        else:
            updated["job_title"] = titles[-1]
        updated["salary"] = round(float(updated["salary"]) * random.uniform(1.10, 1.25), 2)
        updated["change_type"] = "PROMOTION"

    elif scenario == "department_transfer":
        new_dept = random.choice([d for d in DEPARTMENTS if d != updated["department"]])
        updated["department"] = new_dept
        updated["job_title"] = random.choice(JOB_TITLES[new_dept])
        updated["manager_id"] = f"EMP{random.randint(1, 500):06d}"
        updated["change_type"] = "TRANSFER"

    elif scenario == "salary_adjustment":
        updated["salary"] = round(float(updated["salary"]) * random.uniform(1.03, 1.08), 2)
        updated["change_type"] = "SALARY_ADJUSTMENT"

    elif scenario == "status_change":
        current_status = updated["employment_status"]
        if current_status == "Active":
            updated["employment_status"] = random.choice(["On Leave", "Terminated", "Resigned"])
        elif current_status == "On Leave":
            updated["employment_status"] = "Active"
        elif current_status == "Probation":
            updated["employment_status"] = random.choice(["Active", "Terminated"])
        else:
            updated["employment_status"] = "Active"
        updated["change_type"] = "STATUS_CHANGE"

    elif scenario == "address_change":
        updated["street_address"] = fake.street_address()
        updated["city"] = fake.city()
        updated["state"] = random.choice(US_STATES)
        updated["zip_code"] = fake.zipcode()
        updated["change_type"] = "ADDRESS_CHANGE"

    elif scenario == "manager_change":
        updated["manager_id"] = f"EMP{random.randint(1, 500):06d}"
        updated["change_type"] = "MANAGER_CHANGE"

    return updated


def get_employees_by_ids(cursor, emp_ids: list) -> list:
    """Fetch specific employees by their IDs."""
    id_list = ", ".join([f"'EMP{eid:06d}'" for eid in emp_ids])
    query = f"""
    SELECT
        EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
        DEPARTMENT, JOB_TITLE, SALARY, EMPLOYMENT_STATUS,
        TO_CHAR(HIRE_DATE, 'YYYY-MM-DD') as HIRE_DATE,
        MANAGER_ID, STREET_ADDRESS, CITY, STATE, ZIP_CODE, COUNTRY
    FROM {TABLE_NAME}
    WHERE EMPLOYEE_ID IN ({id_list})
    ORDER BY EMPLOYEE_ID
    """
    cursor.execute(query)
    columns = [desc[0].lower() for desc in cursor.description]

    records = []
    for row in cursor.fetchall():
        record = dict(zip(columns, row))
        records.append(record)
    return records


def upsert_records(cursor, records: list) -> None:
    """Update/Insert records into the table."""
    update_sql = f"""
    UPDATE {TABLE_NAME} SET
        FIRST_NAME = %(first_name)s,
        LAST_NAME = %(last_name)s,
        EMAIL = %(email)s,
        PHONE = %(phone)s,
        DEPARTMENT = %(department)s,
        JOB_TITLE = %(job_title)s,
        SALARY = %(salary)s,
        EMPLOYMENT_STATUS = %(employment_status)s,
        HIRE_DATE = %(hire_date)s,
        MANAGER_ID = %(manager_id)s,
        STREET_ADDRESS = %(street_address)s,
        CITY = %(city)s,
        STATE = %(state)s,
        ZIP_CODE = %(zip_code)s,
        COUNTRY = %(country)s,
        LAST_MODIFIED_DATE = %(last_modified_date)s
    WHERE EMPLOYEE_ID = %(employee_id)s
    """

    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
        DEPARTMENT, JOB_TITLE, SALARY, EMPLOYMENT_STATUS, HIRE_DATE,
        MANAGER_ID, STREET_ADDRESS, CITY, STATE, ZIP_CODE,
        COUNTRY, LAST_MODIFIED_DATE
    ) VALUES (
        %(employee_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s,
        %(department)s, %(job_title)s, %(salary)s, %(employment_status)s, %(hire_date)s,
        %(manager_id)s, %(street_address)s, %(city)s, %(state)s, %(zip_code)s,
        %(country)s, %(last_modified_date)s
    )
    """

    updates = [r for r in records if r.get("change_type") != "NEW_HIRE"]
    inserts = [r for r in records if r.get("change_type") == "NEW_HIRE"]

    for r in updates:
        r.pop("change_type", None)
    for r in inserts:
        r.pop("change_type", None)

    if updates:
        cursor.executemany(update_sql, updates)
        print(f"  Updated {len(updates)} existing employee records")

    if inserts:
        cursor.executemany(insert_sql, inserts)
        print(f"  Inserted {len(inserts)} new employee records")


def main():
    """Main function to add incremental data for SCD2 testing."""
    print("=" * 60)
    print("SNOWFLAKE INCREMENTAL DATA GENERATOR (SCD2 Testing)")
    print("=" * 60)

    print(f"\nConnecting to Snowflake...")

    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")

        # Current count
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        initial_count = cursor.fetchone()[0]
        print(f"Current record count: {initial_count}")

        all_changes = []
        update_summary = {}

        # ---- DETERMINISTIC UPDATES (specific IDs -> specific scenarios) ----
        print(f"\n--- Deterministic Updates (for SCD2 validation) ---")
        for scenario, emp_ids in DETERMINISTIC_UPDATES.items():
            employees = get_employees_by_ids(cursor, emp_ids)
            found = len(employees)
            for emp in employees:
                updated = apply_update_scenario(emp, scenario)
                all_changes.append(updated)
            update_summary[scenario] = found
            id_range = f"EMP{emp_ids[0]:06d} - EMP{emp_ids[-1]:06d}"
            print(f"  {scenario:25s}: {found:3d} records ({id_range})")

        # ---- RANDOM UPDATES (broader coverage) ----
        print(f"\n--- Random Updates (broader coverage) ---")
        random_employees = get_employees_by_ids(cursor, RANDOM_UPDATE_IDS)
        for emp in random_employees:
            scenario = random.choice(UPDATE_SCENARIOS)
            updated = apply_update_scenario(emp, scenario)
            all_changes.append(updated)
            update_summary[scenario] = update_summary.get(scenario, 0) + 1
        print(f"  Random updates: {len(random_employees)} records (EMP000061 - EMP000120)")

        total_updates = sum(1 for c in all_changes if c.get("change_type") != "NEW_HIRE")

        # ---- NEW EMPLOYEES ----
        print(f"\n--- New Employees ---")
        new_emp_start_id = initial_count + 1
        new_employees = [generate_new_employee(i) for i in range(new_emp_start_id, new_emp_start_id + 80)]
        all_changes.extend(new_employees)
        print(f"  New hires: {len(new_employees)} records (EMP{new_emp_start_id:06d} - EMP{new_emp_start_id + 79:06d})")

        # ---- APPLY ALL CHANGES ----
        print(f"\nApplying {len(all_changes)} total changes...")
        upsert_records(cursor, all_changes)
        conn.commit()

        # Verify
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        final_count = cursor.fetchone()[0]

        # Show sample of updated records for validation
        print(f"\n--- Sample: Promoted employees (verify in Silver after SCD2) ---")
        cursor.execute(f"""
            SELECT EMPLOYEE_ID, DEPARTMENT, JOB_TITLE, SALARY, LAST_MODIFIED_DATE
            FROM {TABLE_NAME}
            WHERE EMPLOYEE_ID IN ('EMP000001','EMP000002','EMP000003')
            ORDER BY EMPLOYEE_ID
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]} | {row[1]} | {row[2]} | ${row[3]:,.2f} | {row[4]}")

        print(f"\n--- Sample: Transferred employees ---")
        cursor.execute(f"""
            SELECT EMPLOYEE_ID, DEPARTMENT, JOB_TITLE, LAST_MODIFIED_DATE
            FROM {TABLE_NAME}
            WHERE EMPLOYEE_ID IN ('EMP000011','EMP000012','EMP000013')
            ORDER BY EMPLOYEE_ID
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]} | {row[1]} | {row[2]} | {row[3]}")

        print(f"\n{'='*60}")
        print(f"INCREMENTAL DATA GENERATION COMPLETE")
        print(f"{'='*60}")
        print(f"  Initial count:   {initial_count:,}")
        print(f"  Final count:     {final_count:,}")
        print(f"  Records updated: {total_updates}")
        print(f"  Records inserted:{len(new_employees)}")
        print(f"\nUpdate breakdown:")
        for scenario, count in sorted(update_summary.items()):
            print(f"    {scenario:25s}: {count}")
        print(f"\nSCD2 Validation Guide:")
        print(f"  After running ETL incremental pipeline:")
        print(f"  1. EMP000001-EMP000010: should have 2 rows each (old+new) - PROMOTION")
        print(f"  2. EMP000011-EMP000020: should have 2 rows each - DEPARTMENT_TRANSFER")
        print(f"  3. EMP000021-EMP000030: should have 2 rows each - SALARY_ADJUSTMENT")
        print(f"  4. EMP000031-EMP000040: should have 2 rows each - STATUS_CHANGE")
        print(f"  5. EMP000041-EMP000050: should have 2 rows each - ADDRESS_CHANGE")
        print(f"  6. EMP000051-EMP000060: should have 2 rows each - MANAGER_CHANGE")
        print(f"  7. EMP004001-EMP004080: should have 1 row each (new insert)")
        print(f"  8. EMP000200+: should have 1 row each (unchanged)")

    except Exception as e:
        print(f"\nError: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()
