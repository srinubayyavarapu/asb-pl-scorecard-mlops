"""
Script to add incremental records to test SCD2 logic.
This adds ~200 records including:
- New employees (INSERT scenario)
- Updated existing employees (UPDATE scenario for SCD2)
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

EMPLOYMENT_STATUS = ["Active", "Active", "Active", "On Leave", "Probation", "Terminated", "Resigned"]

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

# Update scenarios for SCD2 testing
UPDATE_SCENARIOS = [
    "promotion",           # Job title change + salary increase
    "department_transfer", # Department change + possibly new job title
    "salary_adjustment",   # Salary change only
    "status_change",       # Employment status change (leave, termination)
    "address_change",      # Address update
    "manager_change",      # Reporting line change
]


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

    # New hires - within last 30 days
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
        "employment_status": "Probation",  # New hires start on probation
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
        # Promote to a higher position in same department
        current_dept = updated["department"]
        titles = JOB_TITLES[current_dept]
        current_idx = titles.index(updated["job_title"]) if updated["job_title"] in titles else 0
        if current_idx < len(titles) - 1:
            updated["job_title"] = titles[current_idx + 1]
        # Salary increase 10-25%
        updated["salary"] = round(float(updated["salary"]) * random.uniform(1.10, 1.25), 2)
        updated["change_type"] = "PROMOTION"

    elif scenario == "department_transfer":
        # Move to a different department
        new_dept = random.choice([d for d in DEPARTMENTS if d != updated["department"]])
        updated["department"] = new_dept
        updated["job_title"] = random.choice(JOB_TITLES[new_dept])
        updated["manager_id"] = f"EMP{random.randint(1, 500):06d}"
        updated["change_type"] = "TRANSFER"

    elif scenario == "salary_adjustment":
        # Annual raise or market adjustment (3-8%)
        updated["salary"] = round(float(updated["salary"]) * random.uniform(1.03, 1.08), 2)
        updated["change_type"] = "SALARY_ADJUSTMENT"

    elif scenario == "status_change":
        # Change employment status
        current_status = updated["employment_status"]
        if current_status == "Active":
            updated["employment_status"] = random.choice(["On Leave", "Terminated", "Resigned"])
        elif current_status == "On Leave":
            updated["employment_status"] = "Active"
        elif current_status == "Probation":
            updated["employment_status"] = random.choice(["Active", "Terminated"])
        updated["change_type"] = "STATUS_CHANGE"

    elif scenario == "address_change":
        # Employee relocated
        updated["street_address"] = fake.street_address()
        updated["city"] = fake.city()
        updated["state"] = random.choice(US_STATES)
        updated["zip_code"] = fake.zipcode()
        updated["change_type"] = "ADDRESS_CHANGE"

    elif scenario == "manager_change":
        # Reporting line change (reorg)
        updated["manager_id"] = f"EMP{random.randint(1, 500):06d}"
        updated["change_type"] = "MANAGER_CHANGE"

    return updated


def get_existing_employees(cursor, sample_size: int = 120) -> list:
    """Fetch a sample of existing employees to update."""
    query = f"""
    SELECT
        EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
        DEPARTMENT, JOB_TITLE, SALARY, EMPLOYMENT_STATUS,
        TO_CHAR(HIRE_DATE, 'YYYY-MM-DD') as HIRE_DATE,
        MANAGER_ID, STREET_ADDRESS, CITY, STATE, ZIP_CODE, COUNTRY
    FROM {TABLE_NAME}
    WHERE EMPLOYMENT_STATUS NOT IN ('Terminated', 'Resigned')
    ORDER BY RANDOM()
    LIMIT {sample_size}
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

    # For SCD2 testing, we simply update the source table
    # The SCD2 logic in the target system will handle the historical tracking

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

    # Remove change_type field before executing SQL
    for r in updates:
        r.pop("change_type", None)
    for r in inserts:
        r.pop("change_type", None)

    if updates:
        cursor.executemany(update_sql, updates)
        print(f"Updated {len(updates)} existing employee records")

    if inserts:
        cursor.executemany(insert_sql, inserts)
        print(f"Inserted {len(inserts)} new employee records")


def main():
    """Main function to add incremental data for SCD2 testing."""
    print("=" * 60)
    print("SNOWFLAKE INCREMENTAL DATA GENERATOR (SCD2 Testing)")
    print("=" * 60)

    print(f"\nConnecting to Snowflake...")
    print(f"  Database: {SNOWFLAKE_CONFIG['database']}")
    print(f"  Schema: {SNOWFLAKE_CONFIG['schema']}")
    print(f"  Table: {TABLE_NAME}")

    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")

        # Get current record count
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        initial_count = cursor.fetchone()[0]
        print(f"\nCurrent record count: {initial_count}")

        # Generate incremental changes
        all_changes = []

        # 1. Fetch existing employees and apply updates (~120 updates)
        print(f"\nFetching existing employees for updates...")
        existing_employees = get_existing_employees(cursor, sample_size=120)
        print(f"  Selected {len(existing_employees)} employees for updates")

        # Apply various update scenarios
        update_summary = {}
        for emp in existing_employees:
            scenario = random.choice(UPDATE_SCENARIOS)
            updated_emp = apply_update_scenario(emp, scenario)
            all_changes.append(updated_emp)
            update_summary[scenario] = update_summary.get(scenario, 0) + 1

        print(f"\n  Update scenarios applied:")
        for scenario, count in sorted(update_summary.items()):
            print(f"    - {scenario}: {count}")

        # 2. Generate new employees (~80 new hires)
        print(f"\nGenerating new employee records...")
        new_emp_start_id = initial_count + 1
        new_employees = [generate_new_employee(i) for i in range(new_emp_start_id, new_emp_start_id + 80)]
        all_changes.extend(new_employees)
        print(f"  Generated {len(new_employees)} new employees (EMP{new_emp_start_id:06d} - EMP{new_emp_start_id + 79:06d})")

        # 3. Apply all changes
        print(f"\nApplying {len(all_changes)} total changes...")
        upsert_records(cursor, all_changes)

        # Commit transaction
        conn.commit()

        # Verify record count
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        final_count = cursor.fetchone()[0]
        print(f"\nFinal record count: {final_count}")
        print(f"Net new records: {final_count - initial_count}")

        # Show sample of recent changes
        print(f"\nSample of recently modified records:")
        cursor.execute(f"""
            SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, DEPARTMENT, JOB_TITLE,
                   SALARY, EMPLOYMENT_STATUS, LAST_MODIFIED_DATE
            FROM {TABLE_NAME}
            ORDER BY LAST_MODIFIED_DATE DESC
            LIMIT 10
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]} | {row[1]} {row[2]} | {row[3]} | {row[4]} | ${row[5]:,.2f} | {row[6]} | {row[7]}")

        print("\n" + "=" * 60)
        print("INCREMENTAL DATA GENERATION COMPLETE")
        print("=" * 60)
        print("\nSCD2 Test Scenarios Created:")
        print(f"  - {len(new_employees)} new records (INSERT scenario)")
        print(f"  - {len(existing_employees)} updated records (UPDATE scenario)")
        print("\nUpdate types will trigger SCD2 history tracking:")
        print("  - PROMOTION: New salary + title version")
        print("  - TRANSFER: New department version")
        print("  - SALARY_ADJUSTMENT: New salary version")
        print("  - STATUS_CHANGE: New status version")
        print("  - ADDRESS_CHANGE: New address version")
        print("  - MANAGER_CHANGE: New manager version")

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
