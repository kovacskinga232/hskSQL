from fastapi import HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
from .database_functions import load_databases, save_databases
from .index_functions import create_index, drop_index
class ForeignKeyRequest(BaseModel):
    name: str
    database: str
    fk_table: str
    fk_column: str
    reference_table: str
    reference_column: str

class DropForeignKeyRequest(BaseModel):
    name: str
    database: str
    fk_table: str
    fk_column: str
    reference_table: str
    

def create_fk_index(request: ForeignKeyRequest):

    create_index(request.database, request.fk_table, request.fk_column, unique=False)
    data = load_databases()
    
    new_fk_reference = {
        "column": request.reference_column,
        "other_table": request.fk_table,
        "other_column": request.fk_column
    }
    data[request.database]["tables"][request.reference_table]["is_foreign_key"][request.name]=new_fk_reference
    
    new_fk_reference = {
        "column": request.fk_column,
        "reference_table": request.reference_table,
        "reference_column": request.reference_column
    }
    data[request.database]["tables"][request.fk_table]["foreign_keys"][request.name] = new_fk_reference
    save_databases(data)
    return {"message": "Foreign key constraint index created successfully."}

def drop_fk(request: DropForeignKeyRequest):
    data = load_databases()
    
    if request.database not in data:
        raise HTTPException(status_code=404, detail="Database not found")
    
    if request.fk_table not in data[request.database]["tables"]:
        raise HTTPException(status_code=404, detail="fk Table not found")
    
    if request.reference_table not in data[request.database]["tables"]:
        raise HTTPException(status_code=404, detail="Reference Table not found")
    
    if request.name not in data[request.database]["tables"][request.fk_table]["foreign_keys"]:
        raise HTTPException(status_code=404, detail="Foreign key constraint not found")
    
    del data[request.database]["tables"][request.reference_table]["is_foreign_key"][request.name]
    del data[request.database]["tables"][request.fk_table]["foreign_keys"][request.name]
    save_databases(data)
    index_name = "idx_" + request.fk_table + "_" + request.fk_column
    drop_index(request.database, request.fk_table, index_name)
    return {"message": "Foreign key constraint index dropped successfully."}

def list_foreign_keys(database_name: str, table_name: str):
    try:
        data = load_databases()
        if database_name not in data:
            raise HTTPException(status_code=404, detail=f"Database '{database_name}' not found")
        if "tables" not in data[database_name] or table_name not in data[database_name]["tables"]:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in database '{database_name}'")
        
        foreign_keys = data[database_name]["tables"][table_name].get("foreign_keys", {})
        return {
            "foreign_keys": foreign_keys
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")