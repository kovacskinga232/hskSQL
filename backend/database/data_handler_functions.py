from fastapi import HTTPException
from pymongo.errors import DuplicateKeyError
from .connect import get_collection, get_client
from .database_functions import load_databases, save_databases
from pydantic import BaseModel
from typing import Dict, Any, List, Optional, Union
from .constraint_data_functions import has_field_unique_constraint, is_data_unique, UniqueDataRequest
from .query_executor import execute_select
from .columns_info import get_columns
from .index_data_insert_functions import insert_data_into_index
from .index_data_delete_functions import delete_data_from_index
from .existence_check_functions import table_exists, index_exists, database_exists
from .json_functions import get_primary_key
from .models import DeleteRequest, InsertRequest, IndexRequest, DeleteIndexRequest


def valid_values_list(value_list: List[str], database_name: str, table_name: str) -> bool:
    columns_info = get_columns(database_name, table_name)
    column_types = list(columns_info["columns"].values())
    print(f"value_list: {value_list}")
    if len(value_list) != len(column_types):
        return False  
    #print(f"Column types: {column_types}")

    for value, col_type in zip(value_list, column_types):
        col_type = col_type.lower()
        #print(f"Value: {value}, Type: {col_type}")
        try:
            if col_type in ["int", "integer"]:
                int(value)
            elif col_type in ["float", "double", "real"]:
                float(value)
            elif col_type in ["bit", "boolean", "bool"]:
                if str(value).lower() in ["on","off"]:
                    value = 1 if str(value).lower() == "on" else 0
                if str(value).lower() not in ["true", "false", "0", "1"]:
                    return False
            #convert to datetime
            elif col_type in ["datetime", "timestamp"]:
                from datetime import datetime
                # Try to parse the value as a datetime
                try:
                    datetime.fromisoformat(value)
                except ValueError:
                    return False
            elif col_type in ["date"]:
                from datetime import datetime
                # Try to parse the value as a date
                try:
                    datetime.strptime(value, "%Y-%m-%d")
                except ValueError:
                    return False
            elif col_type in ["text", "char", "varchar", "string"]:
                str(value)  
            else:
                print(f"Unknown type: {col_type}")
                return False
        except ValueError:
            return False  
    
    return True

def get_indexes_for_table(database_name: str, table_name: str):
    data = load_databases()
    return data.get(database_name, {}).get("tables", {}).get(table_name, {}).get("indexes", {})

def delete_data(request: DeleteRequest):
    try:
        if (not database_exists(request.database_name)):
            raise HTTPException(status_code=404, detail="Database not found")
        if (not table_exists(request.database_name, request.table_name)):
            raise HTTPException(status_code=404, detail="Table not found")
        
        collection = get_collection(request.database_name, request.table_name)
        
        data = load_databases()
        is_foreign_key = data[request.database_name]["tables"][request.table_name].get("is_foreign_key", {})
        if is_foreign_key:
            raise HTTPException(status_code=400, detail="Cannot delete data from a table with foreign key constraints. Drop the foreign key constraints first.")
        
        if request.primary_key is None and request.conditions is None:
            raise HTTPException(status_code=400, detail="Either 'primary_key' or 'conditions' must be provided for deletion.")
        
        if request.primary_key is not None:
            record = collection.find_one({"_id": request.primary_key})
            if not record:
                raise HTTPException(status_code=404, detail="Record not found")
            delete_ids = [{"_id": request.primary_key}]  
        elif request.conditions is not None:
            pk_name = get_primary_key(request.database_name, request.table_name)["primary_key"]
            delete_ids = execute_select(
                table_name=request.table_name,
                columns=[pk_name],
                conditions=request.conditions,
                database_name=request.database_name
            )
            print(f"Raw delete_ids from execute_select: {delete_ids}")  # Debug log
            
            if len(delete_ids) == 0:
                raise HTTPException(status_code=404, detail="No records found matching the conditions")
            
            # Convert the delete_ids to the correct format
            formatted_delete_ids = []
            for record in delete_ids:
                #print(f"Processing record: {record}")  # Debug log
                if isinstance(record, dict):
                    if "_id" in record:
                        formatted_delete_ids.append({"_id": str(record["_id"])})
                    elif "id" in record:
                        formatted_delete_ids.append({"_id": str(record["id"])})
                    else:
                        # If the record is just a single value, assume it's the ID
                        formatted_delete_ids.append({"_id": str(list(record.values())[0])})
                else:
                    # If the record is not a dict, assume it's the ID directly
                    formatted_delete_ids.append({"_id": str(record)})
            
            delete_ids = formatted_delete_ids
            #print(f"Formatted delete_ids: {delete_ids}")  # Debug log

        # Get the first record to access column information
        if not delete_ids:
            raise HTTPException(status_code=404, detail="No records found to delete")
            
        first_record = collection.find_one({"_id": delete_ids[0]["_id"]})
        if not first_record:
            raise HTTPException(status_code=404, detail="First record not found in collection")
            
        columns = list(get_columns(request.database_name, request.table_name)["columns"].keys())
        values = first_record.get("value", "").split("#")
        col_map = dict(zip(columns[1:], values))  


        indexes = get_indexes_for_table(request.database_name, request.table_name)
        for index_name, index_data in indexes.items():
            field = index_data["field"]
            field_value = col_map.get(field)
            if field_value is not None:
                for delete_id in delete_ids:
                    delete_data_from_index(DeleteIndexRequest(
                        database_name=request.database_name,
                        table_name=request.table_name,
                        index_name=index_name,
                        pk=field_value,
                        value=delete_id["_id"]
                    ))
        
        for delete_id in delete_ids:
            result = collection.delete_one(delete_id)
        
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Record not found")
        
        return {"message": "Data deleted successfully"}
    except Exception as e:
        #print(f"Error in delete_data: {str(e)}")  # Debug log
        raise HTTPException(status_code=500, detail=str(e))

def insert_data(request: InsertRequest, primary_key_field: str, is_index: Optional[bool] = False):
    try:
        if (not database_exists(request.database_name)):
            raise HTTPException(status_code=404, detail="Database not found")
        
        collection = get_collection(request.database_name, request.table_name)
        
        inserted_count = 0
        errors = []
        #print(f"pk:{primary_key_field}")
        if not is_index:
            indexes = get_indexes_for_table(request.database_name, request.table_name)
        try: 
            for record in request.records:
                #print(f"Processing record: {record}")
                
                values_list = []
                all_values = []
                for key, value in record.items():
                    if key != primary_key_field:
                        if (has_field_unique_constraint(UniqueDataRequest(
                            database_name=request.database_name,
                            table_name=request.table_name,
                            field_name=key,
                        )) and not is_data_unique(UniqueDataRequest(
                                database_name=request.database_name,
                                table_name=request.table_name,
                                field_name=key,
                                field_value=value
                            ))):
                            raise HTTPException(status_code=400, detail=f"Duplicate value for unique field '{key}': {value}")
                        values_list.append(str(value))
                    else:
                        primary_key = str(value)
                    all_values.append(str(value))

                        
                
                if not is_index and not valid_values_list(all_values, request.database_name, request.table_name):
                    errors.append(f"Invalid value for record: {record}")
                    continue
                
                other_values = "#".join(values_list)
                #print(f"key : {primary_key}, values: {other_values}")
                collection.insert_one({
                    "_id": primary_key,
                    "value": other_values
                })
                
                if not is_index:
                    for index_name, index_data in indexes.items():
                        field = index_data["field"]
                        unique = index_data.get("unique", False)

                        field_value = record.get(field)
                        pk_value = record[primary_key_field]

                        if field_value is not None:
                            insert_data_into_index(IndexRequest(
                                database_name=request.database_name,
                                table_name=request.table_name,
                                field_name=field,
                                index_name=index_name,
                                unique=unique,
                                pk_value=str(pk_value),
                                field_value=str(field_value)
                            ))
                        
                inserted_count += 1
        except DuplicateKeyError:
            errors.append(f"Duplicate primary key: {primary_key}")
        except Exception as e:
            errors.append(f"Error processing record {record}: {str(e)}")
        
        result = {
            "message": f"Insert completed. {inserted_count} records inserted successfully.",
            "inserted_count": inserted_count
        }
        
        if errors:
            result["errors"] = errors
            
        return result
    except Exception as e:
        #print("insertdata error")
        raise HTTPException(status_code=500, detail=str(e))
    

def list_data(database_name: str, table_name: str):
    try:
        if (not database_exists(database_name)):
            raise HTTPException(status_code=404, detail="Database not found")
        if (not table_exists(database_name, table_name)):
            raise HTTPException(status_code=404, detail="Table not found")
        
        results = execute_select(
            table_name=table_name,
            columns=["*"],
            database_name=database_name
        )
        
        #print(f"Records: {records}")
        #print(f"Results: {results}")
    
        #return the columns from results
        return {
            "records": results
        }
    except Exception as e:
        #print("list_data error")
        raise HTTPException(status_code=500, detail=str(e))

