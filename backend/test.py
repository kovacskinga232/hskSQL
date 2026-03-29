import random
from faker import Faker
from database.connect import get_client
from database.table_functions import create_table, Table
from database.data_handler_functions import InsertRequest
from database.database_functions import create_database, Database
from database.foreign_key_functions import create_fk_index, ForeignKeyRequest

# random data generation
fake = Faker()

def setup_database():
    client = get_client()
    
    if "MiniPerformanceDB" in client.list_database_names():
        client.drop_database("MiniPerformanceDB")
        print("Dropped existing MiniPerformanceDB")
    
    db = Database(name="MiniPerformanceDB")
    create_database(db)
    
    dept_table = Table(
        database_name="MiniPerformanceDB",
        table_name="departments",
        columns={
            "id": "int",
            "name": "varchar",
            "location": "varchar",
            "budget": "float"
        },
        primary_key="id"
    )
    create_table(dept_table)
    
    teams_table = Table(
        database_name="MiniPerformanceDB",
        table_name="teams",
        columns={
            "id": "int",
            "dept": "int",
            "name": "varchar",
            "manager": "varchar"
        },
        primary_key="id"
    )
    create_table(teams_table)
    
    fk_request = ForeignKeyRequest(
        name="fk_teams_dept",
        database="MiniPerformanceDB",
        fk_table="teams",
        fk_column="dept",
        reference_table="departments",
        reference_column="id"
    )
    create_fk_index(fk_request)
    
    employees_table = Table(
        database_name="MiniPerformanceDB",
        table_name="employees",
        columns={
            "id": "int",
            "name": "varchar",
            "email": "varchar",
            "salary": "float",
            "hireDate": "varchar", 
            "team": "int"
        },
        primary_key="id"
    )
    create_table(employees_table)
    
    fk_request = ForeignKeyRequest(
        name="fk_employees_team",
        database="MiniPerformanceDB",
        fk_table="employees",
        fk_column="team",
        reference_table="teams",
        reference_column="id"
    )
    create_fk_index(fk_request)
    

def generate_test_data():
    departments = [
        {"id": 1, "name": "Engineering", "location": "New York", "budget": 1000000.00},
        {"id": 2, "name": "Marketing", "location": "Chicago", "budget": 750000.00},
        {"id": 3, "name": "Sales", "location": "Los Angeles", "budget": 900000.00},
        {"id": 4, "name": "HR", "location": "Austin", "budget": 500000.00},
        {"id": 5, "name": "Finance", "location": "Boston", "budget": 850000.00}
    ]
    
    dept_request = InsertRequest(
        database_name="MiniPerformanceDB",
        table_name="departments",
        records=departments
    )
    insert_data(dept_request, "id")
    
    teams = []
    team_managers = [fake.name() for _ in range(20)]  
    
    batch_size = 500  
    teams = []
    for i in range(1, 10001):
        dept_id = random.randint(1, 5)
        teams.append({
            "id": i,
            "dept": dept_id,
            "name": f"Team {chr(64 + (i % 26) + 1)}{i//26 if i//26 > 0 else ''}",  
            "manager": random.choice(team_managers)
        })
        if i % batch_size == 0 or i == 10000:
            team_request = InsertRequest(
                database_name="MiniPerformanceDB",
                table_name="teams",
                records=teams
            )
            insert_data(team_request, "id")
            teams = [] 
    
    
    
    employees = []
    
    for i in range(1, 10001):
        team_id = random.randint(1, 10000)
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        employees.append({
            "id": i,
            "name": f"{first_name} {last_name}",
            "email": f"{first_name.lower()}.{last_name.lower()}@company.com",
            "salary": round(random.uniform(40000, 120000), 2),
            "hireDate": fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d'),
            "team": team_id
        })
        
        if i % batch_size == 0 or i == 10000:
            emp_request = InsertRequest(
                database_name="MiniPerformanceDB",
                table_name="employees",
                records=employees
            )
            insert_data(emp_request, "id")
            employees = [] 

def insert_data(request: InsertRequest, primary_key_field: str):
    """Wrapper for your insert_data function with progress tracking"""
    from database.data_handler_functions import insert_data as original_insert
    
    result = original_insert(request, primary_key_field)
    if 'errors' in result:
        print(f"Encountered {len(result['errors'])} errors")

if __name__ == "__main__":
    print("Starting database creation and test data generation...")
    setup_database()
    generate_test_data()
    print("MiniPerformanceDB created and populated successfully!")