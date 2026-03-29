from fastapi import HTTPException
from .connect import get_client, get_collection
from .database_functions import load_databases, save_databases
from .index_data_insert_functions import insert_data_into_index
from .models import IndexRequest
from pymongo.errors import OperationFailure
from .delete_data_functions import delete_collection
from .existence_check_functions import database_exists, index_exists
from .columns_info import get_column_index

def create_index(database_name: str, table_name: str, field_name: str, unique: bool = False):
    try:
        if not database_exists(database_name):
            raise HTTPException(status_code=404, detail="Database not found")
        
        data = load_databases()
        if not ( (database_name in data) and ("tables" in data[database_name]) and (table_name in data[database_name]["tables"])):
            raise HTTPException(status_code=404, detail="Table not found")
        
        
        if "indexes" not in data[database_name]["tables"][table_name]:
            data[database_name]["tables"][table_name]["indexes"] = {}
        elif index_exists(database_name, table_name, field_name):
            raise HTTPException(status_code=400, detail="Index already exists")
        
        
        if field_name == "_id" and unique:
            return {
                "message": "The _id field already has a unique index by default. No need to create another one."
            }
        
        index_name = f"idx_{table_name}_{field_name}"
        
        collection = get_collection(database_name, table_name)
        columns_info = get_column_index(database_name, table_name, field_name)
        
        if columns_info <= 0:
            raise HTTPException(status_code=400, detail=f"Field '{field_name}' not found in table")
        
        # Create index collection and insert data
        index_collection = get_collection(database_name, index_name)
        
        for doc in collection.find():
            values = doc.get("value", "").split("#")
            if len(values) >= columns_info:
                field_value = values[columns_info - 1]
                pk_value = doc.get("_id")
                
                insert_request = IndexRequest(
                    database_name=database_name,
                    table_name=table_name,
                    field_name=field_name,
                    index_name=index_name,
                    unique=unique,
                    pk_value=str(field_value),
                    field_value=str(pk_value)
                )
                insert_data_into_index(insert_request)
        
        data[database_name]["tables"][table_name]["indexes"][index_name] = {
            "field": field_name,
            "unique": unique
        }
        save_databases(data)
        
        return {
            "message": f"Index created successfully on field '{field_name}'",
            "index_name": index_name
        }
        
    except OperationFailure as e:
        raise HTTPException(status_code=400, detail=f"Failed to create index: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

def drop_index(database_name: str, table_name: str, index_name: str):
    try:
        if not database_exists(database_name):
            raise HTTPException(status_code=404, detail="Database not found")
        data = load_databases()
        if not ( (database_name in data) and ("tables" in data[database_name]) and (table_name in data[database_name]["tables"])):
            raise HTTPException(status_code=404, detail="Table not found")
        if not index_exists(database_name, table_name, index_name):
            raise HTTPException(status_code=404, detail="Index not found")
        
        del data[database_name]["tables"][table_name]["indexes"][index_name]
        save_databases(data)
        client = get_client()
        db = client[database_name]
        if index_name in db.list_collection_names():
            delete_collection(database_name, index_name)
        
        return {
            "message": f"Index '{index_name}' dropped successfully"
        }
    except OperationFailure as e:
        raise HTTPException(status_code=400, detail=f"Failed to drop index: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")