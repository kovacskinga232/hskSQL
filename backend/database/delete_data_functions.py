from fastapi import HTTPException
from .connect import get_client

def delete_collection(database_name: str, table_name: str):
    try:
        client = get_client()
        db = client[database_name]

        if table_name not in db.list_collection_names():
            raise HTTPException(status_code=404, detail="Table not found")

        db.drop_collection(table_name)

        return {"message": "Collection deleted successfully"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def delete_dbase(database_name: str):
    try:
        client = get_client()
        if database_name not in client.list_database_names():
            return {"message": "Database already dropped or does not exist"}
        
        client.drop_database(database_name)
        return {"message": "Database deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
