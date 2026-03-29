from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, Optional, Any, List
import json
import database.database_functions as database_functions
import database.table_functions as table_functions
import database.data_handler_functions as data_operations
import database.index_functions as index_functions
import database.foreign_key_functions as foreign_key_functions
import database.query_executor as query_executor
import database.columns_info as columns_info
import database.index_data_delete_functions as index_data_delete_functions
import database.json_functions as json_functions
import database.models as models

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

class Database(BaseModel):
    name: str

@app.post("/create_database")
def create_database(db: Database):
    return database_functions.create_database(db)

@app.post("/drop_database")
def drop_database(db: Database):
    return database_functions.drop_database(db)

@app.get("/list_databases")
def list_databases():
    return database_functions.list_databases()

@app.get("/get_database")
def get_database(database_name: str):
    return database_functions.get_database(database_name)

@app.get("/list_data")
def list_data(database_name: str, table_name: str):
    return data_operations.list_data(database_name, table_name)
    

class Table(BaseModel):
    database_name: str
    table_name: str
    columns: Optional[Dict[str, str]] = None
    primary_key: Optional[str] = None
    foreign_keys: Optional[Dict[str, Dict[str, str]]] = None    
    unique_constraints: Optional[List[str]] = Field(default_factory=list)


@app.post("/create_table")
def create_table(request: Table):
    return table_functions.create_table(request)

@app.post("/drop_table")
def drop_table(request: Table):
    return table_functions.drop_table(request)


@app.post("/delete")
def delete_data(request: models.DeleteRequest):
    return data_operations.delete_data(request)

@app.post("/insert")
def insert_data(request: models.InsertRequest):
    pk = get_primary_key(request.database_name, request.table_name)["primary_key"]
    return data_operations.insert_data(request,pk)


class CreateIndexRequest(BaseModel):
    database_name: str
    table_name: str
    field_name: str
    unique: Optional[bool] = False

class DropIndexRequest(BaseModel):
    database_name: str
    table_name: str
    index_name: str

@app.post("/create_index")
def create_index(request: CreateIndexRequest):
    return index_functions.create_index(
        request.database_name, 
        request.table_name, 
        request.field_name, 
        request.unique
    )

@app.delete("/drop_index")
def drop_index(request: DropIndexRequest):
    return index_functions.drop_index(
        request.database_name, 
        request.table_name, 
        request.index_name
    )



@app.post("/test_insert_into_existing_index")
def test_insert_into_existing_index(request: models.IndexRequest):
    return index_data_delete_functions.insert_data_into_index(request)

@app.post("/test_delete_from_existing_index")
def test_delete_from_existing_index(request: models.DeleteIndexRequest):
    return index_data_delete_functions.delete_data_from_index(request)

@app.get("/get_primary_key")
def get_primary_key(database_name: str, table_name: str):
    pk_name = json_functions.get_primary_key(database_name, table_name)
    #print(f"Primary key for {database_name}.{table_name}: {pk_name}")
    return pk_name

@app.get("/get_columns")
def get_columns(database_name: str, table_name: str):
    columns = columns_info.get_columns(database_name, table_name)
    ##print(f"Columns for {database_name}.{table_name}: {columns}")
    return columns

@app.get("/list_indexes")
def list_indexes(database_name: str, table_name: str):
    indexes = json_functions.list_indexes(database_name, table_name)
    #print(f"Indexes for {database_name}.{table_name}: {indexes}")
    return indexes



class DropForeignKeyRequest(BaseModel):
    name: str
    database: str
    fk_table: str
    fk_column: str
    reference_table: str
    
@app.post("/drop_fk_constraint")
def drop_fk_constraint(request: DropForeignKeyRequest):
    return foreign_key_functions.drop_fk(request)

@app.get("/list_foreign_keys")
def list_foreign_keys(database_name: str, table_name: str):
    foreign_keys = foreign_key_functions.list_foreign_keys(database_name, table_name)
    #print(f"Foreign keys for {database_name}.{table_name}: {foreign_keys}")
    return foreign_keys

@app.get("/execute_select")
def execute_select(
    database_name: str = Query(...),
    table_name: str = Query(...),
    columns: str = Query(...),  # Receive as string
    conditions: str = Query(None),  # Optional, receive as string
    group_by: str = Query(None),  # Optional, receive as string
    order_by: str = Query(None)  # Optional, receive as string
):
    # Convert JSON strings to Python objects
    try:
        columns_list = json.loads(columns)
        conditions_list = json.loads(conditions) if conditions else None
        group_by_list = json.loads(group_by) if group_by else None
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON format: {str(e)}")
    
    # Parse table_name as it might be a JSON array
    try:
        if table_name.startswith('[') and table_name.endswith(']'):
            table_name_list = json.loads(table_name)
        else:
            table_name_list = table_name
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid table_name format: {str(e)}")
    
    try:
        return query_executor.execute_select(
            table_name=table_name_list,
            columns=columns_list,
            conditions=conditions_list,
            database_name=database_name,
            group_by=group_by_list,
            order_by=order_by
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
