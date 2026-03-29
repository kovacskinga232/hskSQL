from pydantic import BaseModel
from typing import Dict, Any, List, Optional, Union

class DeleteRequest(BaseModel):
    database_name: str
    table_name: str
    primary_key: Optional[str] = None  # Optional primary key for deletion
    conditions: Optional[List[Dict[str, Union[str, int, float]]]] = None # Optional conditions for deletion

class InsertRequest(BaseModel):
    database_name: str
    table_name: str
    records: List[Dict[str, Any]]

class IndexRequest(BaseModel):
    database_name: str
    table_name: str
    field_name: str
    index_name: str
    unique: Optional[bool] = False
    pk_value: Optional[str] = None
    field_value: Optional[Any] = None

class DeleteIndexRequest(BaseModel):
    database_name: str
    table_name: str
    index_name: str
    pk: str
    value: str 