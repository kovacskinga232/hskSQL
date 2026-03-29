from fastapi import HTTPException
from .connect import get_collection
from .database_functions import load_databases, save_databases
from .models import InsertRequest, IndexRequest
from .columns_info import get_column_index
from .existence_check_functions import database_exists, table_exists, index_exists

def insert_data_into_index(request: IndexRequest):
    try:
        print(request)
        if (not database_exists(request.database_name)):
            raise HTTPException(status_code=404, detail="Database not found")
        if (not table_exists(request.database_name, request.table_name)):
            raise HTTPException(status_code=404, detail="Table not found")
        
        if (index_exists(request.database_name, request.table_name, request.index_name)):
            if (request.pk_value is None or request.field_value is None) and (request.field_name is None):
                raise HTTPException(status_code=400, detail="Missing 'pk_value' or 'field_value' or 'field_name' for inserting into existing index.")

            collection = get_collection(request.database_name, request.index_name)
            record = collection.find_one({"_id": request.field_value})
            print("-------> Record found in index:", record)
            if not record:
                collection.insert_one({
                    "_id": request.field_value,
                    "value": request.pk_value
                })
                print("-------> Inserted new record into index")
                return {"message": "Index data inserted successfully"}
            elif request.unique:
                raise HTTPException(status_code=400, detail=f"Unique index already exists for field '{request.field_name}' with value '{request.field_value}'.")
            print("-------> Record already exists in index:", record)
            values = record.get("value", "").split("#")
            if request.pk_value not in values:
                values.append(request.pk_value)
                collection.update_one(
                    {"_id": request.field_value},
                    {"$set": {"value": "#".join(values)}}
                )
                print("-------> Updated existing record in index")
            return {"message": "Index data inserted successfully"}
        else:
            db_name = request.database_name
            index_name = request.index_name

            collection = get_collection(db_name, request.table_name)
            index_collection = get_collection(db_name, index_name)
            
            pk_filed_nr = get_column_index(db_name, request.table_name, request.field_name) - 1
            index = {}
            
            # First pass: collect all values
            for record in collection.find():
                try:
                    values = record.get("value", "").split("#")
                    if len(values) > pk_filed_nr:
                        field_value = values[pk_filed_nr]
                        pk_value = record["_id"]
                        
                        if not request.unique:
                            if field_value not in index:
                                index[field_value] = set()  # Use set to automatically handle duplicates
                            index[field_value].add(pk_value)
                        else:
                            if field_value in index:
                                raise HTTPException(status_code=400, detail=f"Duplicate value found for field '{request.field_name}' while creating unique index.")
                            index[field_value] = pk_value
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"Error processing record {record}: {str(e)}")
            
            # Second pass: insert all values at once
            bulk_operations = []
            for field_value, pk_values in index.items():
                if isinstance(pk_values, set):
                    bulk_operations.append({
                        "_id": field_value,
                        "value": "#".join(str(x) for x in pk_values)
                    })
                else:
                    bulk_operations.append({
                        "_id": field_value,
                        "value": str(pk_values)
                    })
            
            if bulk_operations:
                # Drop the collection if it exists to avoid duplicate key errors
                index_collection.drop()
                # Create the collection and insert all values
                index_collection.insert_many(bulk_operations)

            return {"message": "Index created and data inserted successfully"}  

    except Exception as e:
        print("insert_data_into_index error:", str(e))
        data = load_databases()
        if request.database_name in data and request.table_name in data[request.database_name]["tables"]:
            if "indexes" in data[request.database_name]["tables"][request.table_name]:
                if request.index_name in data[request.database_name]["tables"][request.table_name]["indexes"]:
                    del data[request.database_name]["tables"][request.table_name]["indexes"][request.index_name]
                    save_databases(data)
        raise HTTPException(status_code=500, detail=str(e)) 