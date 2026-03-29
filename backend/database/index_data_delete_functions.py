from fastapi import HTTPException
from .connect import get_collection
from .existence_check_functions import database_exists, table_exists
from .models import DeleteIndexRequest

def delete_data_from_index(request: DeleteIndexRequest):
    try:
        if (not database_exists(request.database_name)):
            raise HTTPException(status_code=404, detail="Database not found")
        if (not table_exists(request.database_name, request.table_name)):
            raise HTTPException(status_code=404, detail="Table not found")
        
        db_name = request.database_name
        index_name = request.index_name

        collection = get_collection(db_name, index_name)
        
        record = collection.find_one({"_id": request.pk})
        if not record:
            #raise HTTPException(status_code=404, detail="Record not found")
            return{"message":"Record not found, already deleted index data"}
        values = record.get("value").split("#")
        if len(values) == 1:
            collection.delete_one({"_id": request.pk})
        else:
            values.remove(request.value)
            collection.update_one(
                {"_id": request.pk},
                {"$set": {"value": "#".join(values)}}  
            )
        
        return {"message": "Index data deleted successfully"}
    except Exception as e:
        print("delete_data_from_index error")
        raise HTTPException(status_code=500, detail=str(e))
