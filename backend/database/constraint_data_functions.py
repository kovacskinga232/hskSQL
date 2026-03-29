from typing import Any, Optional
from fastapi import FastAPI
from fastapi import HTTPException
from .connect import get_collection
from pydantic import BaseModel
from .database_functions import load_databases, save_databases
from .columns_info import get_columns
class UniqueDataRequest(BaseModel):
    database_name: str
    table_name: str
    field_name: str
    field_value: Optional[Any] = None


def is_data_unique(request: UniqueDataRequest) -> bool:
    collection = get_collection(request.database_name, request.table_name)
    columns = get_columns(request.database_name, request.table_name)["columns"]
    # Each document in collection is assumed to have a "value" field with # separated values
    for doc in collection.find():
        values = doc.get("value", "").split("#")
        columns_no_id = list(columns.keys())[1:]  # Get column names without ID
        if len(values) >= len(columns_no_id):  # Ensure we have enough values
            row = dict(zip(columns_no_id, values))
            if row.get(request.field_name) == str(request.field_value):
                return False
    return True

def has_field_unique_constraint(request: UniqueDataRequest) -> bool:
    data = load_databases()
     
    table_info = data[request.database_name]["tables"][request.table_name]
    
    if "unique_constraints" in table_info and request.field_name in table_info["unique_constraints"]:
        return True
    
    return False

def drop_unique_constraint(request: UniqueDataRequest):
    data = load_databases()
    
    if request.database_name not in data:
        raise HTTPException(status_code=404, detail=f"Database '{request.database_name}' not found.")
    
    if request.table_name not in data[request.database_name]:
        raise HTTPException(status_code=404, detail=f"Table '{request.table_name}' not found in database '{request.database_name}'.")
        
    if "unique_constraints" in data[request.database_name]["tables"][request.table_name] and request.field_name in data[request.database_name]["tables"][request.table_name]["unique_constraints"]:
        del data[request.database_name]["tables"][request.table_name]["unique_constraints"][request.field_name]
        save_databases(data)
        return {"message": f"Unique constraint on field '{request.field_name}' dropped successfully."}
    
    raise HTTPException(status_code=404, detail=f"No unique constraint found on field '{request.field_name}'.")