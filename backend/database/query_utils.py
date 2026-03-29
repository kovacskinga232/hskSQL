from typing import List, Dict, Union, Optional
from .connect import get_collection
from .database_functions import load_databases
from .columns_info import get_columns
from .json_functions import list_indexes, get_primary_key

def _build_index_query(operator: str, value: Union[str, int, float]) -> Dict:
    """Build query for index collection based on operator"""
    # Convert numeric values to string for index collections
    if isinstance(value, (int, float)):
        value = str(value)
    
    # Map SQL operators to MongoDB operators
    operator_map = {
        "=": "$eq",
        ">": "$gt",
        "<": "$lt",
        ">=": "$gte",
        "<=": "$lte"
    }
    
    mongo_operator = operator_map.get(operator)
    if not mongo_operator:
        raise ValueError(f"Unsupported operator: {operator}")
    
    if operator == "=":
        return {"_id": value}  # Direct match for equality
    return {"_id": {mongo_operator: value}}

def _convert_value(value: Union[str, int, float], col_type: str) -> Union[str, int, float, bool]:
    """Convert value to proper type based on column type"""
    if value is None:
        return None
    
    col_type = col_type.lower()
    try:
        if col_type in ["int", "integer"]:
            return int(value)
        elif col_type in ["float", "double", "real"]:
            return float(value)
        elif col_type in ["bit", "boolean", "bool"]:
            return value.lower() in ["true", "1", "yes"]
    except (ValueError, AttributeError):
        pass
    return value

def _matches_conditions(doc: Dict, conditions: List[Dict], column_types: Dict[str, str]) -> bool:
    """Check document against non-indexed conditions with proper type conversion"""
    for cond in conditions:
        val = doc.get(cond["column"])
        col_type = column_types.get(cond["column"], "str")
        
        # Convert condition value to proper type
        try:
            cond_value = _convert_value(cond["value"], col_type)
        except (ValueError, TypeError):
            cond_value = cond["value"]
        
        if not _compare_values(val, cond["operator"], cond_value):
            return False
    return True

def _compare_values(val, operator: str, target) -> bool:
    """Compare values based on operator with type safety"""
    try:
        ops = {
            "=": lambda a, b: a == b,
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b
        }
        
        # Handle None values
        if val is None or target is None:
            return False
            
        # Special handling for different types
        if isinstance(val, str) and isinstance(target, (int, float)):
            try:
                val = float(val) if "." in val else int(val)
            except ValueError:
                pass
        elif isinstance(target, str) and isinstance(val, (int, float)):
            try:
                target = float(target) if "." in target else int(target)
            except ValueError:
                pass
                
        return ops.get(operator, lambda a, b: False)(val, target)
    except TypeError:
        return False

def _deduplicate_results(results: List[Dict]) -> List[Dict]:
    """Remove duplicate results"""
    seen = set()
    unique = []
    for res in results:
        res_hash = hash(frozenset(res.items()))
        if res_hash not in seen:
            seen.add(res_hash)
            unique.append(res)
    return unique

def _convert_condition_values(conditions: List[Dict], columns_info: Dict[str, str]) -> List[Dict]:
    """Convert condition values to proper types based on column definitions"""
    converted_conditions = []
    for cond in conditions:
        new_cond = cond.copy()
        column_name = new_cond["column"]
        col_type = columns_info.get(column_name, "str")
        
        # Skip conversion for join conditions (containing dots)
        if "." in str(new_cond["value"]):
            converted_conditions.append(new_cond)
            continue
            
        try:
            new_cond["value"] = _convert_value(new_cond["value"], col_type)
        except (ValueError, TypeError) as e:
            print(f"Error converting value for: {column_name} with value: {new_cond['value']}: {str(e)}")
            continue
            
        converted_conditions.append(new_cond)
    return converted_conditions

def execute_single_table_query(
    database_name: str,
    table_name: str,
    columns: List[str],
    conditions: List[Dict]
) -> List[Dict]:
    """Execute a query on a single table with proper type handling"""
    try:
        collection = get_collection(database_name, table_name)
        
        # Get table structure
        columns_info = get_columns(database_name, table_name)["columns"]
        column_names = list(columns_info.keys())
        column_types = columns_info
        
        # Get primary key field name
        pk_field_name = get_primary_key(database_name, table_name)["primary_key"]
        
        # Always include primary key in columns if not already present
        if pk_field_name and pk_field_name not in columns and "*" not in columns:
            columns = [pk_field_name] + columns
        
        # Convert condition values to proper types
        converted_conditions = _convert_condition_values(conditions, column_types)
        
        # Prepare query components
        mongo_query = {}
        full_scan_conditions = []
        
        if converted_conditions:
            for cond in converted_conditions:
                column_name = cond["column"]
                col_type = column_types.get(column_name)
                
                if col_type is None:
                    print(f"Warning: No type found for column {column_name}, skipping condition")
                    continue
                    
                # Check if index exists for this column
                index_name = f"idx_{table_name}_{column_name}"
                indexes = list_indexes(database_name, table_name)["indexes"]
                
                if index_name in indexes:
                    # Get index collection
                    index_collection = get_collection(database_name, index_name)
                    # Build query for index collection
                    index_query = _build_index_query(cond["operator"], str(cond["value"]))
                    
                    matching_pks = []
                    for index_doc in index_collection.find(index_query):
                        if "value" in index_doc:
                            matching_pks.extend(index_doc["value"].split("#"))
                    
                    if not matching_pks:
                        return []  # No matches found
                    
                    if "_id" in mongo_query:
                        # Intersect with existing PKs
                        existing_pks = mongo_query["_id"]["$in"]
                        mongo_query["_id"]["$in"] = [pk for pk in existing_pks if pk in matching_pks]
                        if not mongo_query["_id"]["$in"]:
                            return []
                    else:
                        mongo_query["_id"] = {"$in": matching_pks}
                else:
                    full_scan_conditions.append(cond)
        
        # Execute main query
        cursor = collection.find(mongo_query)
        
        # Process results
        results = []
        for doc in cursor:
            # Parse the value field
            values = doc.get("value", "").split("#")
            id_type = column_types.get("_id", "str")
            row = {pk_field_name: _convert_value(doc["_id"], id_type)}  # Use pk_field_name instead of _id
            
            # Map values to columns
            for i, col_name in enumerate(column_names[1:]):  # Skip _id
                if i < len(values):
                    row[col_name] = _convert_value(values[i], column_types.get(col_name, "str"))
            
            # Apply non-indexed conditions with proper type conversion
            if _matches_conditions(row, full_scan_conditions, column_types):
                if columns == ["*"]:
                    results.append(row)
                else:
                    # Only include requested columns
                    filtered_row = {}
                    for col in columns:
                        if col in row:
                            filtered_row[col] = row[col]
                    results.append(filtered_row)
        
        return _deduplicate_results(results)
    except Exception as e:
        print(f"Error in execute_single_table_query: {str(e)}")
        return []