from fastapi import HTTPException
from .database_functions import load_databases

def table_exists(database_name: str, table_name: str):
    data = load_databases()
    return (database_name in data) and ("tables" in data[database_name]) and (table_name in data[database_name]["tables"])

def database_exists(database_name: str):
    data = load_databases()
    return database_name in data

def index_exists(database_name: str, table_name: str, index_name: str):
    try:
        data = load_databases()
        table = data.get(database_name, {}).get("tables", {}).get(table_name, {})
        indexes = table.get("indexes", {})
        return index_name in indexes
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
