from pymongo import MongoClient
from typing import List, Dict, Union, Optional
from .connect import get_collection
from .database_functions import load_databases
from .columns_info import get_columns
from .json_functions import list_indexes
from .json_functions import get_primary_key
from .join_functions import get_join_conditions, get_table_from_column, execute_join
from .query_utils import execute_single_table_query
from .aggregation_functions import execute_aggregation
import json
import re

def _parse_aggregate_function(column: str) -> Optional[Dict]:
    """Parse aggregate function from column name, handling table-prefixed columns"""
    # Handle aggregate functions with table prefixes like MAX(employees.id)
    match = re.match(r'^(COUNT|SUM|AVG|MIN|MAX)\((.*)\)$', column, re.IGNORECASE)
    if match:
        func_name = match.group(1).upper()
        inner_column = match.group(2).strip()
        
        # Handle COUNT(*) special case
        if inner_column == "*":
            return {
                "function": func_name,
                "column": "*",
                "table": None,
                "original": column
            }
        
        # Extract table and column from table.column format
        if "." in inner_column:
            table_name, col_name = inner_column.split(".", 1)
            return {
                "function": func_name,
                "column": col_name,
                "table": table_name,
                "original": column
            }
        else:
            return {
                "function": func_name,
                "column": inner_column,
                "table": None,
                "original": column
            }
    return None

def _extract_tables_from_columns(columns: List[str]) -> List[str]:
    """Extract unique table names from column specifications, handling aggregate functions"""
    tables = set()
    
    for col in columns:
        if col == "*":
            continue
            
        # Check if it's an aggregate function
        agg_func = _parse_aggregate_function(col)
        if agg_func and agg_func["table"]:
            tables.add(agg_func["table"])
        elif "." in col:
            # Regular table.column format
            table_name = col.split(".", 1)[0]
            tables.add(table_name)
    
    return list(tables)

def _parse_order_by(order_by_str: str) -> List[Dict]:
    """Parse ORDER BY clause, handling aggregate functions and table prefixes"""
    if not order_by_str:
        return []
    
    print(f"Parsing ORDER BY: {order_by_str}")
    order_specs = []
    
    # Split by comma, but be careful with function parentheses
    parts = []
    current_part = ""
    paren_count = 0
    
    for char in order_by_str:
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
        elif char == ',' and paren_count == 0:
            parts.append(current_part.strip())
            current_part = ""
            continue
        current_part += char
    
    
    if current_part.strip():
        parts.append(current_part.strip())
    
    print(f"Split parts: {parts}")
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
        is_direction = False
        direction = "ASC"  # Default direction
        if part in ["ASC", "DESC"]:
            direction = part
            is_direction = True
        else:
            column = part
            agg_func = _parse_aggregate_function(column)
            if agg_func:
                final_column = f"{agg_func['function']}({agg_func['column']})"
            else:
                final_column = column
        
        if is_direction:
            order_specs.append({
                "column": final_column,
                "direction": direction
            })
    
    print(f"Parsed ORDER BY: {order_specs}")
    return order_specs

def _execute_join_with_aggregation(
    database_name: str,
    tables: List[str],
    columns: List[str],
    conditions: List[Dict],
    group_by: Optional[List[str]] = None,
    order_by: Optional[str] = None
) -> List[Dict]:
    """Execute join query with aggregation support"""
    
    # Separate aggregate and regular columns
    aggregate_columns = []
    regular_columns = []
    
    for col in columns:
        agg_func = _parse_aggregate_function(col)
        if agg_func:
            aggregate_columns.append(agg_func)
        else:
            regular_columns.append(col)
        
    if not aggregate_columns and not group_by:
        # No aggregation needed, regular join
        single_table_conditions, join_conditions = get_join_conditions(conditions)
        
        # Load database info for foreign key relationships
        data = load_databases()
        db_info = data.get(database_name, {})
        
        # Generate join conditions from foreign keys
        for table in tables:
            table_info = db_info.get("tables", {}).get(table, {})
            foreign_keys = table_info.get("foreign_keys", {})
            
            for fk_name, fk_info in foreign_keys.items():
                ref_table = fk_info.get("reference_table")
                if ref_table in tables:
                    join_conditions.append({
                        "column": f"{table}.{fk_info['column']}",
                        "operator": "=",
                        "value": f"{ref_table}.{fk_info['reference_column']}"
                    })
        
        return execute_join(
            database_name=database_name,
            tables=tables,
            columns=columns,
            conditions=single_table_conditions,
            join_conditions=join_conditions
        )
    
    # Handle aggregation with joins
    # First, execute the join to get the base result set
    single_table_conditions, join_conditions = get_join_conditions(conditions)
    
    # For joins with aggregation, we need all relevant columns
    join_columns = set(regular_columns)
    if group_by:
        join_columns.update(group_by)
    
    # Add columns referenced in aggregate functions
    for agg_func in aggregate_columns:
        if agg_func["column"] != "*":
            if agg_func["table"]:
                join_columns.add(f"{agg_func['table']}.{agg_func['column']}")
            else:
                # If no table specified, we need to include this column from all tables
                # that might have it, but for safety, let's include all columns
                for table in tables:
                    join_columns.add(f"{table}.{agg_func['column']}")
    
    # Load database info for foreign key relationships
    data = load_databases()
    db_info = data.get(database_name, {})
    
    # Generate join conditions from foreign keys
    for table in tables:
        table_info = db_info.get("tables", {}).get(table, {})
        foreign_keys = table_info.get("foreign_keys", {})
        
        for fk_name, fk_info in foreign_keys.items():
            ref_table = fk_info.get("reference_table")
            if ref_table in tables:
                join_conditions.append({
                    "column": f"{table}.{fk_info['column']}",
                    "operator": "=",
                    "value": f"{ref_table}.{fk_info['reference_column']}"
                })
    
    print(f"Join columns for aggregation: {list(join_columns)}")
    
    # Execute the join
    join_results = execute_join(
        database_name=database_name,
        tables=tables,
        columns=list(join_columns),
        conditions=single_table_conditions,
        join_conditions=join_conditions
    )
    
    print(f"Join results before aggregation: {len(join_results)} rows")
    if join_results:
        print(f"Sample row keys: {list(join_results[0].keys())}")
    
    # Now perform aggregation on the joined results
    if group_by or aggregate_columns:
        return _perform_aggregation_on_results(
            join_results, 
            group_by or [], 
            aggregate_columns,
            _parse_order_by(order_by) if order_by else None,
            database_name=database_name,
            tables=tables,
            regular_columns=regular_columns  # Pass regular columns for aggregation
        )
    
    
    return join_results

def _convert_value_for_ordering(value: any, col_type: str) -> any:
    """Convert value to appropriate type for ordering"""
    if value is None:
        return value
        
    try:
        if col_type in ["int", "integer"]:
            return int(value)
        elif col_type in ["float", "double", "real"]:
            return float(value)
        elif col_type in ["bit", "boolean", "bool"]:
            return value.lower() in ["true", "1", "yes"]
        return value
    except (ValueError, TypeError):
        return value

def _perform_aggregation_on_results(
    results: List[Dict],
    group_by_columns: List[str],
    aggregate_columns: List[Dict],
    order_by: Optional[List[Dict]] = None,
    database_name: str = "TestDB",
    tables: Optional[List[str]] = None,
    regular_columns: Optional[List[str]] = None  # Add this parameter
) -> List[Dict]:
    """Perform aggregation on already joined results"""
    from collections import defaultdict
    
    if not group_by_columns:
        # No grouping, aggregate all results
        if not results:
            return []
        
        aggregated_result = {}
        
        # Add regular columns (take first row values since we're not grouping)
        if regular_columns:
            for col in regular_columns:
                if results and col in results[0]:
                    aggregated_result[col] = results[0][col]
        
        for agg_func in aggregate_columns:
            func_name = agg_func["function"]
            col_name = agg_func["column"]
            
            if agg_func.get("table"):
                full_col_name = f"{agg_func['table']}.{col_name}"
            else:
                full_col_name = col_name
            
            values = []
            for row in results:
                if col_name == "*":
                    values.append(1)  # COUNT(*)
                elif full_col_name in row and row[full_col_name] is not None:
                    try:
                        val = float(row[full_col_name]) if func_name in ["SUM", "AVG"] else row[full_col_name]
                        values.append(val)
                    except (ValueError, TypeError):
                        pass
            
            result_key = f"{func_name}({col_name})"
            
            if func_name == "COUNT":
                aggregated_result[result_key] = len(values) if col_name == "*" else len([v for v in values if v is not None])
            elif func_name == "SUM":
                aggregated_result[result_key] = sum(values) if values else 0
            elif func_name == "AVG":
                aggregated_result[result_key] = sum(values) / len(values) if values else None
            elif func_name == "MIN":
                aggregated_result[result_key] = min(values) if values else None
            elif func_name == "MAX":
                aggregated_result[result_key] = max(values) if values else None
        
        return [aggregated_result]
    
    # Group by specified columns
    groups = defaultdict(lambda: defaultdict(list))
    group_regular_columns = defaultdict(dict)  # Store regular column values per group
    
    for row in results:
        # Create group key
        group_key = tuple(row.get(col, None) for col in group_by_columns)
        
        # Store regular column values for this group (take first occurrence)
        if regular_columns and group_key not in group_regular_columns:
            for col in regular_columns:
                if col in row:
                    group_regular_columns[group_key][col] = row[col]
        
        # Add values to group for aggregation
        for agg_func in aggregate_columns:
            col_name = agg_func["column"]
            if agg_func.get("table"):
                full_col_name = f"{agg_func['table']}.{col_name}"
            else:
                full_col_name = col_name
            
            if col_name == "*":
                groups[group_key]["*"].append(1)
            elif full_col_name in row:
                groups[group_key][full_col_name].append(row[full_col_name])
    
    # Calculate aggregates
    final_results = []
    for group_key, group_data in groups.items():
        result = {}
        
        # Add group by columns
        for i, col in enumerate(group_by_columns):
            result[col] = group_key[i]
        
        # Add regular columns (non-aggregated, non-group-by columns)
        if regular_columns:
            for col in regular_columns:
                if col not in group_by_columns:  # Don't duplicate group by columns
                    result[col] = group_regular_columns[group_key].get(col)
        
        # Calculate aggregates
        for agg_func in aggregate_columns:
            func_name = agg_func["function"]
            col_name = agg_func["column"]
            
            if agg_func.get("table"):
                full_col_name = f"{agg_func['table']}.{col_name}"
            else:
                full_col_name = col_name
            
            values = group_data.get(full_col_name if col_name != "*" else "*", [])
            result_key = f"{func_name}({col_name})"
            
            if func_name == "COUNT":
                result[result_key] = len([v for v in values if v is not None])
            elif func_name == "SUM":
                numeric_values = [float(v) for v in values if v is not None]
                result[result_key] = sum(numeric_values) if numeric_values else 0
            elif func_name == "AVG":
                numeric_values = [float(v) for v in values if v is not None]
                result[result_key] = sum(numeric_values) / len(numeric_values) if numeric_values else None
            elif func_name == "MIN":
                valid_values = [v for v in values if v is not None]
                result[result_key] = min(valid_values) if valid_values else None
            elif func_name == "MAX":
                valid_values = [v for v in values if v is not None]
                result[result_key] = max(valid_values) if valid_values else None
        
        final_results.append(result)
    
    #print(f"final_results: {final_results}")
    
    # Apply ordering
    if order_by:
        for order_spec in reversed(order_by):
            col_name = order_spec["column"]
            direction = order_spec["direction"]
            
            def get_key(row):
                # Handle table-prefixed columns
                if "." in col_name:
                    table_name, actual_col = col_name.split(".", 1)
                    # Get column type from the correct table
                    col_type = get_columns(database_name, table_name)["columns"].get(actual_col, "str")
                else:
                    col_type = get_columns(database_name, tables[0])["columns"].get(col_name, "str")
                
                val = row.get(col_name)
                if val is None:
                    return float('-inf') if direction == "ASC" else float('inf')
                
                return _convert_value_for_ordering(val, col_type)
            
            final_results.sort(key=get_key, reverse=(direction == "DESC"))
    
    return final_results

def execute_select(
    table_name: Union[str, List[str]],
    columns: List[str],
    conditions: Optional[List[Dict[str, Union[str, int, float]]]] = None,
    database_name: str = "TestDB",
    group_by: Optional[List[str]] = None,
    order_by: Optional[str] = None
) -> List[Dict]:
    try:
        print(f"table_name: {table_name}")
        print(f"columns: {columns}")
        print(f"conditions: {conditions}")
        print(f"database_name: {database_name}")
        print(f"group_by: {group_by}")
        print(f"order_by: {order_by}")
        
        if conditions is None:
            conditions = []
            
        # Handle table_name as either string or list
        if isinstance(table_name, str):
            if table_name.startswith('[') and table_name.endswith(']'):
                try:
                    tables = json.loads(table_name)
                except json.JSONDecodeError:
                    tables = [table_name]
            else:
                tables = [table_name]
        else:
            tables = table_name
        
        # Extract tables from column specifications
        column_tables = _extract_tables_from_columns(columns)
        all_tables = list(set(tables + column_tables))
        
        # Check if this is a join query
        if len(all_tables) > 1:
            return _execute_join_with_aggregation(
                database_name=database_name,
                tables=all_tables,
                columns=columns,
                conditions=conditions,
                group_by=group_by,
                order_by=order_by
            )
        
        # Single table query
        if group_by or any(_parse_aggregate_function(col) for col in columns):
            # Handle aggregate functions
            aggregate_columns = []
            regular_columns = []
            
            for col in columns:
                agg_func = _parse_aggregate_function(col)
                if agg_func:
                    # For single table, remove table prefix from column
                    clean_column = agg_func["column"]
                    aggregate_columns.append({
                        "function": agg_func["function"],
                        "column": clean_column
                    })
                else:
                    # Remove table prefix from regular columns too
                    if "." in col:
                        clean_col = col.split(".", 1)[1]
                        regular_columns.append(clean_col)
                    else:
                        regular_columns.append(col)
            
            # For single table aggregation, clean group_by columns
            clean_group_by = []
            if group_by:
                for col in group_by:
                    if "." in col:
                        clean_group_by.append(col.split(".", 1)[1])
                    else:
                        clean_group_by.append(col)
            
            # Add regular columns to aggregate if not in group by
            for col in regular_columns:
                if not clean_group_by or col not in clean_group_by:
                    aggregate_columns.append({
                        "function": "COUNT",
                        "column": col
                    })
            
            # Execute aggregation
            results = execute_aggregation(
                database_name=database_name,
                table_name=tables[0],
                group_by_columns=clean_group_by,
                aggregate_columns=aggregate_columns,
                conditions=conditions,
                order_by=_parse_order_by(order_by) if order_by else None
            )
            
            return results
        else:
            # Regular single table query
            # Clean column names (remove table prefixes)
            clean_columns = []
            for col in columns:
                if col == "*":
                    clean_columns.append(col)
                elif "." in col:
                    clean_columns.append(col.split(".", 1)[1])
                else:
                    clean_columns.append(col)
            
            results = execute_single_table_query(
                database_name=database_name,
                table_name=tables[0],
                columns=clean_columns,
                conditions=conditions
            )
            
            # Apply ORDER BY if specified
            if order_by:
                order_specs = _parse_order_by(order_by)
                for order_spec in reversed(order_specs):
                    col_name = order_spec["column"]
                    direction = order_spec["direction"]
                    
                    # Clean column name for single table
                    if "." in col_name:
                        table_name, actual_col = col_name.split(".", 1)
                        # Get column type from the correct table
                        col_type = get_columns(database_name, table_name)["columns"].get(actual_col, "str")
                    else:
                        col_type = get_columns(database_name, tables[0])["columns"].get(col_name, "str")
                    
                    def get_key(row):
                        val = row.get(col_name)
                        if val is None:
                            return float('-inf') if direction == "ASC" else float('inf')
                        
                        return _convert_value_for_ordering(val, col_type)
                    
                    results.sort(key=get_key, reverse=(direction == "DESC"))
            
            return results
    
    except Exception as e:
        raise Exception(f"Query execution failed: {str(e)}")