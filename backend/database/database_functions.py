from fastapi import HTTPException
from pydantic import BaseModel
import json
import os
from .delete_data_functions import delete_dbase
DB_FILE = "databases.json"
class Database(BaseModel):
    name: str

def get_full_path(file_name):
    return os.path.join(os.path.dirname(__file__), file_name)

def load_databases():
    with open(get_full_path(DB_FILE), "r") as f:
        return json.load(f)

def save_databases(data):
    with open(get_full_path(DB_FILE), "w") as f:
        json.dump(data, f, indent=4)

def create_database(db: Database):
    data = load_databases()
    if db.name in data:
        raise HTTPException(status_code=400, detail="Database already exists.")
    data[db.name] = {}  
    save_databases(data)
    return {"message": f"Database '{db.name}' created successfully"}

def drop_database(db: Database):
    data = load_databases()
    if db.name not in data:
        raise HTTPException(status_code=400, detail="Database does not exists.")
    del data[db.name]
    save_databases(data)
    delete_dbase(db.name)  
    return {"message": f"Database '{db.name}' dropped successfully"}

def list_databases():
    return load_databases()

def get_database(database_name: str):
    data = load_databases()

    if database_name not in data:
        raise HTTPException(status_code=404, detail=f"Database '{database_name}' not found.")

    tables = data[database_name].get("tables", {})
    
    return {"database": database_name, "tables": tables}

