from typing import List, Dict, Union, Optional
from collections import defaultdict
from .connect import get_collection
from .columns_info import get_columns
from .json_functions import list_indexes, get_primary_key

def _convert_value(value: Union[str, int, float], col_type: str) -> Union[str, int, float, bool]:
    """Convert value to proper type based on column type"""
    if value is None or str(value).lower() == "null":
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

def _get_column_type(database_name: str, table_name: str, column_name: str) -> str:
    """Get the type of a column"""
    try:
        columns_info = get_columns(database_name, table_name)["columns"]
        return columns_info.get(column_name, "str")
    except:
        return "str"

def execute_aggregation(
    database_name: str,
    table_name: str,
    group_by_columns: List[str],
    aggregate_columns: List[Dict],
    conditions: List[Dict],
    order_by: Optional[List[Dict]] = None
) -> List[Dict]:
    """Execute aggregation query with GROUP BY and ORDER BY"""
    try:
        print(f"Starting aggregation for {table_name}")  # Debug
        print(f"Group by: {group_by_columns}")  # Debug
        print(f"Aggregate columns: {aggregate_columns}")  # Debug
        print(f"Conditions: {conditions}")  # Debug
        
        collection = get_collection(database_name, table_name)
        columns_info = get_columns(database_name, table_name)["columns"]
        print(f"Columns info: {columns_info}")  # Debug
        
        pk_info = get_primary_key(database_name, table_name)
        pk_field_name = pk_info["primary_key"] if pk_info else "_id"
        
        groups = defaultdict(lambda: defaultdict(list))
        total_rows = 0
        matched_rows = 0
        
        for doc in collection.find():
            total_rows += 1
            row = {}
            
            # Handle the primary key
            id_type = columns_info.get(pk_field_name, "str")
            row[pk_field_name] = _convert_value(doc["_id"], id_type)
            
            # Extract and convert all column values
            
            values = doc.get("value", "").split("#")
            for i, (col_name, col_type) in enumerate(columns_info.items()):
                if col_name == "_id" or col_name == pk_field_name:
                    continue
                if i <= len(values):  # Adjust index since we skipped _id
                    row[col_name] = _convert_value(values[i-1], col_type)
            
            # Check conditions
            #print(f"columns_info: {columns_info}")  # Debug
            if not _matches_conditions(row, conditions, columns_info):
                continue
                
            matched_rows += 1
            group_key = row.get(group_by_columns[0])
            
            for agg in aggregate_columns:
                col_name = agg["column"]
                if agg["function"] == "COUNT" and col_name == "*":
                    col_name = pk_field_name 
                    groups[group_key][col_name].append(1)
                    #print(f"col_name: {col_name}, group_key: {group_key}")  # Debug
                elif agg["function"] == "COUNT":
                    #print(group_by_columns[0])
                    groups[group_key][pk_field_name].append(row[group_by_columns[0]])
                elif col_name in row:
                    groups[group_key][col_name].append(row[col_name])
        
        print(f"Total rows: {total_rows}, Matched rows: {matched_rows}")  # Debug
        print(f"Number of groups: {len(groups)}")  # Debug
        
        results = []
        for group_value, group_data in groups.items():
            result = {
                group_by_columns[0]: group_value  # Add the group by column
            }
            #print(f"Processing group: {group_value}")  # Debug
            # Calculate aggregates
            for agg in aggregate_columns:
                col_name = agg["column"]
                func = agg["function"]
                
                if col_name == "*":
                    col_name = pk_field_name
                
                values = group_data.get(col_name, [])
                                
                if func == "COUNT":
                    # Convert COUNT(id) back to COUNT(*) in the result key
                    if col_name == pk_field_name:
                        result[f"{func}(*)"] = len(values)
                    else:
                        result[f"{func}({col_name})"] = len(values)
                elif func == "SUM":
                    result[f"{func}({col_name})"] = sum(float(v) for v in values if v is not None)
                elif func == "AVG":
                    valid_values = [float(v) for v in values if v is not None]
                    result[f"{func}({col_name})"] = sum(valid_values) / len(valid_values) if valid_values else None
                elif func == "MIN":
                    valid_values = [v for v in values if v is not None]
                    result[f"{func}({col_name})"] = min(valid_values) if valid_values else None
                elif func == "MAX":
                    valid_values = [v for v in values if v is not None]
                    result[f"{func}({col_name})"] = max(valid_values) if valid_values else None
            
            results.append(result)
        
        # Debug print before ordering
        #print(f"Results before ordering: {results}")  # Debug
        
        if order_by:
            print(f"Ordering by: {order_by}")  # Debug
            for order_spec in reversed(order_by):
                col_name = order_spec["column"]
                direction = order_spec.get("direction", "ASC")
                
                # Fix: Use col_name instead of undefined 'col'
                results.sort(
                    key=lambda x: x.get(col_name, float('-inf') if direction == "ASC" else float('inf')),
                    reverse=(direction == "DESC")
                )
        
        #print(f"Final results: {results}")  # Debug
        return results
    
    except Exception as e:
        print(f"Error in execute_aggregation: {str(e)}")
        return []

def _matches_conditions(doc: Dict, conditions: List[Dict], column_types: Dict[str, str]) -> bool:
    """Check if document matches all conditions"""
    for cond in conditions:
        col_name = cond["column"]
        val = doc.get(col_name)
        col_type = column_types.get(col_name, "str")
        
        # Convert condition value to proper type
        try:
            target = _convert_value(cond["value"], col_type)
        except (ValueError, TypeError):
            target = cond["value"]
        
        if not _compare_values(val, cond["operator"], target):
            return False
    return True

def _compare_values(val, operator: str, target) -> bool:
    """Compare values based on operator"""
    try:
        ops = {
            "=": lambda a, b: a == b,
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b
        }
        
        if val is None or target is None:
            return False
            
        return ops.get(operator, lambda a, b: False)(val, target)
    except TypeError:
        return False 