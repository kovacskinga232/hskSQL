from fastapi import HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Optional, List
import json
import os
from .database_functions import load_databases, save_databases
from .delete_data_functions import delete_collection
from .foreign_key_functions import create_fk_index, ForeignKeyRequest, drop_fk, DropForeignKeyRequest
from .connect import get_client
from .existence_check_functions import database_exists, table_exists

DB_FILE = "databases.json"

class Table(BaseModel):
    database_name: str
    table_name: str
    columns: Optional[Dict[str, str]] = None
    primary_key: Optional[str] = None
    foreign_keys: Optional[Dict[str, Dict[str, str]]] = None
    unique_constraints: Optional[List[str]] = Field(default_factory=list)
    
def create_table(request: Table):
    data = load_databases()

    if not database_exists(request.database_name):
        raise HTTPException(status_code=404, detail="Database not found.")

    if "tables" not in data[request.database_name]:
        data[request.database_name]["tables"] = {}
    if request.table_name in data[request.database_name]["tables"]:
        raise HTTPException(status_code=400, detail="Table already exists.")


    data[request.database_name]["tables"][request.table_name] = {
        "columns": request.columns if request.columns else {},
        "primary_key": request.primary_key if request.primary_key else None,
        "foreign_keys": request.foreign_keys if request.foreign_keys else {},
        "is_foreign_key": {},
        "unique_constraints": getattr(request, "unique_constraints", []),
        "indexes": {}
    }

    save_databases(data)
    if request.foreign_keys:
        for fk_name, fk_details in request.foreign_keys.items():
            create_fk_index(ForeignKeyRequest(
                name=fk_name,
                database=request.database_name,
                fk_table=request.table_name,
                fk_column=fk_details["column_name"],
                reference_table=fk_details["reference_table"],
                reference_column=fk_details["reference_column"]
            ))
    return {"message": f"Table '{request.table_name}' created successfully in database '{request.database_name}'"}


def drop_table(request: Table):
    data = load_databases()

    if not database_exists(request.database_name):
        raise HTTPException(status_code=404, detail="Database not found.")

    if not table_exists(request.database_name, request.table_name):
        raise HTTPException(status_code=400, detail="Table does not exist.")

    is_foreign_key = data[request.database_name]["tables"][request.table_name].get("is_foreign_key", {})
    if is_foreign_key:
        raise HTTPException(status_code=400, detail="Table has foreign key constraints. Drop them first.")

    for fk_name, fk_details in data[request.database_name]["tables"][request.table_name].get("foreign_keys", {}).items():
        drop_fk(DropForeignKeyRequest(
            name=fk_name,
            database=request.database_name,
            fk_table=request.table_name,
            fk_column=fk_details["column_name"],
            reference_table=fk_details["reference_table"]
        ))
    data = load_databases()
    client = get_client()
    db = client[request.database_name]
    if request.table_name in db.list_collection_names():
        delete_collection(request.database_name, request.table_name)

    del data[request.database_name]["tables"][request.table_name]
    save_databases(data)
    return {"message": f"Table '{request.table_name}' dropped successfully from database '{request.database_name}'"}


def get_index_primary_key(database_name: str, table_name: str, index_name: str):
    data = load_databases()
    if not database_exists(database_name):
        raise HTTPException(status_code=404, detail="Database not found.")
    if not table_exists(database_name, table_name):
        raise HTTPException(status_code=404, detail="Table not found.")
    if not (("indexes" in data[database_name]["tables"][table_name]) and (index_name in data[database_name]["tables"][table_name]["indexes"])):
        raise HTTPException(status_code=404, detail="Index not found.")
    index_pk = data[database_name]["tables"][table_name]["indexes"][index_name].get("field", None)
    #output index primary key to console
    return {"index_primary_key": index_pk}