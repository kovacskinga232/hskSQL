from fastapi import HTTPException
from .database_functions import load_databases
from .existence_check_functions import table_exists, database_exists

def get_columns(database_name: str, table_name: str):
    data = load_databases()
    if not database_exists(database_name):
        #print(f"Database {database_name} not found")
        return {"columns": {}}
    if not table_exists(database_name, table_name):
        #print(f"Table {table_name} not found in database {database_name}")
        return {"columns": {}}
    columns = data[database_name]["tables"][table_name].get("columns", {})
    #output columns to console
    #print(f"Columns: {columns}")
    return {"columns": columns}

def get_column_index(database_name: str, table_name: str, column_name: str) -> int:
    columns_info = get_columns(database_name, table_name)
    column_names = list(columns_info["columns"].keys())
    try:
        return column_names.index(column_name)
    except ValueError:
        print(f"Column '{column_name}' not found in table '{table_name}'")
        return -1
