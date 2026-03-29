from fastapi import HTTPException
from .existence_check_functions import database_exists, table_exists
from .database_functions import load_databases

def list_indexes(database_name: str, table_name: str):
    try:
        if not database_exists(database_name):
            raise HTTPException(status_code=404, detail="Database not found")
        
        data = load_databases()
        if not ( (database_name in data) and ("tables" in data[database_name]) and (table_name in data[database_name]["tables"])):
            raise HTTPException(status_code=404, detail="Table not found")
        
        indexes = data[database_name]["tables"][table_name].get("indexes", {})
        
        return {
            "indexes": indexes
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

def get_primary_key(database_name: str, table_name: str):
    data = load_databases()
    if not database_exists(database_name):
        raise HTTPException(status_code=404, detail="Database not found.")
    if not table_exists(database_name, table_name):
        raise HTTPException(status_code=404, detail="Table not found.")
    pk_name= data[database_name]["tables"][table_name].get("primary_key", None)
    return {"primary_key": pk_name}