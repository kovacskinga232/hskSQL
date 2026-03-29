from typing import List, Dict, Union, Optional, Tuple, Set
import time
from collections import defaultdict
from .connect import get_collection
from .database_functions import load_databases
from .columns_info import get_columns
from .json_functions import get_primary_key, list_indexes
from .query_utils import execute_single_table_query

def get_table_size(database_name: str, table_name: str) -> int:
    """Get the number of rows in a table with caching"""
    if not hasattr(get_table_size, '_cache'):
        get_table_size._cache = {}
    
    cache_key = f"{database_name}.{table_name}"
    if cache_key not in get_table_size._cache:
        collection = get_collection(database_name, table_name)
        get_table_size._cache[cache_key] = collection.count_documents({})
    
    return get_table_size._cache[cache_key]

def get_join_conditions(conditions: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """Split conditions into single-table and join conditions"""
    single_table_conditions = []
    join_conditions = []
    
    for cond in conditions:
        if "." in cond["column"] and isinstance(cond["value"], str) and "." in cond["value"]:
            # This is a join condition (e.g., "users.id = groups.id")
            join_conditions.append(cond)
        else:
            single_table_conditions.append(cond)
    
    return single_table_conditions, join_conditions

def get_table_from_column(column: str) -> str:
    """Extract table name from a column reference (e.g., 'users.id' -> 'users')"""
    return column.split(".")[0]

def get_column_name(column: str) -> str:
    """Extract column name from a column reference (e.g., 'users.id' -> 'id')"""
    return column.split(".", 1)[1] if "." in column else column

def get_join_tables(conditions: List[Dict]) -> List[str]:
    """Get list of tables involved in the join"""
    tables = set()
    for cond in conditions:
        if "." in cond["column"]:
            tables.add(get_table_from_column(cond["column"]))
    return list(tables)

def get_join_graph(join_conditions: List[Dict]) -> Dict[str, Set[str]]:
    """Build a graph of table relationships based on join conditions"""
    graph = defaultdict(set)
    for cond in join_conditions:
        table1 = get_table_from_column(cond["column"])
        if isinstance(cond["value"], str) and "." in cond["value"]:
            table2 = get_table_from_column(cond["value"])
            # Add bidirectional edges
            graph[table1].add(table2)
            graph[table2].add(table1)
    return dict(graph)

def estimate_join_selectivity(database_name: str, join_condition: Dict) -> float:
    """Estimate join selectivity based on table sizes and column cardinality"""
    left_table = get_table_from_column(join_condition["column"])
    right_table = get_table_from_column(join_condition["value"])
    
    left_size = get_table_size(database_name, left_table)
    right_size = get_table_size(database_name, right_table)
    
    # Simple heuristic: smaller table / larger table
    # In practice, you'd want to use column statistics
    if left_size == 0 or right_size == 0:
        return 0.0
    
    return min(left_size, right_size) / max(left_size, right_size)

def get_optimal_join_order(database_name: str, tables: List[str], join_conditions: List[Dict]) -> List[str]:
    """Determine optimal join order using cost-based optimization"""
    if len(tables) <= 1:
        return tables
    
    # Build join graph with selectivity estimates
    join_graph = get_join_graph(join_conditions)
    join_costs = {}
    
    # Calculate join costs
    for cond in join_conditions:
        left_table = get_table_from_column(cond["column"])
        right_table = get_table_from_column(cond["value"])
        selectivity = estimate_join_selectivity(database_name, cond)
        cost = get_table_size(database_name, left_table) * get_table_size(database_name, right_table) * selectivity
        join_costs[(left_table, right_table)] = cost
        join_costs[(right_table, left_table)] = cost
    
    # Check for foreign key relationships to guide join order
    db_info = load_databases()
    if database_name in db_info and "tables" in db_info[database_name]:
        for cond in join_conditions:
            left_table = get_table_from_column(cond["column"])
            right_table = get_table_from_column(cond["value"])
            
            left_table_info = db_info[database_name]["tables"].get(left_table, {})
            foreign_keys = left_table_info.get("foreign_keys", {})
            
            for fk_info in foreign_keys.values():
                if fk_info.get("reference_table") == right_table:
                    # Foreign key relationship found - prefer this order
                    return [left_table, right_table] + [t for t in tables if t not in [left_table, right_table]]
    
    # Greedy approach: start with smallest table that has connections
    connected_tables = [t for t in tables if t in join_graph and join_graph[t]]
    if not connected_tables:
        return sorted(tables, key=lambda t: get_table_size(database_name, t))
    
    # Start with the smallest connected table
    start_table = min(connected_tables, key=lambda t: get_table_size(database_name, t))
    
    # Build join order using minimum cost approach
    remaining = set(tables) - {start_table}
    join_order = [start_table]
    current_result_size = get_table_size(database_name, start_table)
    
    while remaining:
        best_next = None
        best_cost = float('inf')
        
        for table in remaining:
            if table in join_graph.get(join_order[-1], set()):
                # Estimate cost of joining this table
                estimated_cost = current_result_size * get_table_size(database_name, table)
                join_key = (join_order[-1], table)
                if join_key in join_costs:
                    estimated_cost *= join_costs[join_key]
                
                if estimated_cost < best_cost:
                    best_cost = estimated_cost
                    best_next = table
        
        if best_next:
            join_order.append(best_next)
            remaining.remove(best_next)
            # Update estimated result size (simplified)
            current_result_size = int(current_result_size * 0.1)  # Assume 10% selectivity
        else:
            # No connected table found, add smallest remaining
            smallest = min(remaining, key=lambda t: get_table_size(database_name, t))
            join_order.append(smallest)
            remaining.remove(smallest)
    
    return join_order

def build_hash_table(rows: List[Dict], join_column: str) -> Dict[str, List[Dict]]:
    """Build an in-memory hash table for join optimization"""
    hash_table = defaultdict(list)
    for row in rows:
        key = str(row.get(join_column, ''))
        hash_table[key].append(row)
    return dict(hash_table)

def _evaluate_join_condition(row: Dict, condition: Dict, database_name: str) -> bool:
    """Evaluate a join condition on a row with proper type conversion"""
    left_col = condition["column"]
    left_val = row.get(left_col)
    
    # Get the column type for proper conversion
    left_table = get_table_from_column(left_col)
    left_col_name = get_column_name(left_col)
    
    try:
        columns_info = get_columns(database_name, left_table)
        left_col_type = columns_info["columns"].get(left_col_name, "str")
    except:
        left_col_type = "str"
    
    # Convert left value
    try:
        if left_col_type.lower() in ["int", "integer"]:
            left_val = int(left_val) if left_val is not None else None
        elif left_col_type.lower() in ["float", "double", "real"]:
            left_val = float(left_val) if left_val is not None else None
    except (ValueError, TypeError):
        pass
    
    # Handle right value
    if isinstance(condition["value"], str) and "." in condition["value"]:
        # This is a column reference
        right_col = condition["value"]
        right_val = row.get(right_col)
        
        # Convert right value
        right_table = get_table_from_column(right_col)
        right_col_name = get_column_name(right_col)
        
        try:
            columns_info = get_columns(database_name, right_table)
            right_col_type = columns_info["columns"].get(right_col_name, "str")
        except:
            right_col_type = "str"
        
        try:
            if right_col_type.lower() in ["int", "integer"]:
                right_val = int(right_val) if right_val is not None else None
            elif right_col_type.lower() in ["float", "double", "real"]:
                right_val = float(right_val) if right_val is not None else None
        except (ValueError, TypeError):
            pass
    else:
        right_val = condition["value"]
        # Convert literal value based on left column type
        try:
            if left_col_type.lower() in ["int", "integer"]:
                right_val = int(right_val) if right_val is not None else None
            elif left_col_type.lower() in ["float", "double", "real"]:
                right_val = float(right_val) if right_val is not None else None
        except (ValueError, TypeError):
            pass
    
    ops = {
        "=": lambda a, b: a == b,
        ">": lambda a, b: a > b if a is not None and b is not None else False,
        "<": lambda a, b: a < b if a is not None and b is not None else False,
        ">=": lambda a, b: a >= b if a is not None and b is not None else False,
        "<=": lambda a, b: a <= b if a is not None and b is not None else False
    }
    
    if left_val is None or right_val is None:
        return False
        
    return ops.get(condition["operator"], lambda a, b: False)(left_val, right_val)

def get_best_join_strategy(database_name: str, outer_table: str, inner_table: str, join_conditions: List[Dict]) -> Optional[Dict]:
    """Analyze and return the best join strategy"""
    strategies = []
    print(f"Analyzing join conditions for {outer_table} and {inner_table}")
    for cond in join_conditions:
        left_table = get_table_from_column(cond["column"])
        right_table = get_table_from_column(cond["value"])
        
            
        # Check for index on inner table
        if left_table == inner_table:
            join_column = get_column_name(cond["column"])
            index_name = f"idx_{inner_table}_{join_column}"
            print(f"Checking index {index_name} on inner table {inner_table} for join condition {cond}")
            try:
                available_indexes = list_indexes(database_name, inner_table)["indexes"]
                if index_name in available_indexes:
                    strategies.append({
                        'type': 'inner_index',
                        'index_name': index_name,
                        'outer_field': f"{outer_table}.{get_column_name(cond['value'])}",
                        'inner_field': f"{inner_table}.{join_column}",
                        'cost': 1  # Lower cost for indexed access
                    })
            except:
                pass
        
        # Check for index on outer table (for hash join style)
        else:
            join_column = get_column_name(cond["column"])
            index_name = f"idx_{outer_table}_{join_column}"
            print(f"Checking index {index_name} on outer table {outer_table} for join condition {cond}")
            try:
                available_indexes = list_indexes(database_name, outer_table)["indexes"]
                if index_name in available_indexes:
                    strategies.append({
                        'type': 'outer_index',
                        'index_name': index_name,
                        'outer_field': f"{outer_table}.{join_column}",
                        'inner_field': f"{inner_table}.{get_column_name(cond['value'])}",
                        'cost': 2  # Slightly higher cost
                    })
            except:
                pass
    
    # Return the best strategy (lowest cost)
    if strategies:
        return min(strategies, key=lambda s: s['cost'])
    
    return None

def execute_indexed_join(database_name: str, outer_results: List[Dict], inner_results: List[Dict], 
                        join_strategy: Dict, join_conditions: List[Dict]) -> List[Dict]:
    """Execute join using the selected index strategy"""
    joined_results = []
    
    if join_strategy['type'] == 'inner_index':
        # Use index on inner table with batching
        index_collection = get_collection(database_name, join_strategy['index_name'])
        
        # Batch collect unique join values from outer table
        join_values = set()
        for row in outer_results:
            val = row.get(join_strategy['outer_field'])
            if val is not None:
                join_values.add(str(val))
        
        if not join_values:
            return []
        
        # Batch fetch matching records from index
        batch_size = 1000  # Process in batches to avoid memory issues
        join_values_list = list(join_values)
        
        all_matching_records = []
        for i in range(0, len(join_values_list), batch_size):
            batch = join_values_list[i:i + batch_size]
            matching_records = list(index_collection.find({"_id": {"$in": batch}}))
            all_matching_records.extend(matching_records)
        
        # Build mapping of join value -> matching primary keys
        join_map = defaultdict(set)
        for rec in all_matching_records:
            pks = rec.get("value", "").split("#")
            join_map[rec["_id"]].update(pks)
        
        # Build hash table for inner results by primary key
        inner_table = join_strategy['inner_field'].split('.')[0]
        try:
            pk_field = get_primary_key(database_name, inner_table)['primary_key']
            inner_pk_field = f"{inner_table}.{pk_field}"
            inner_hash = build_hash_table(inner_results, inner_pk_field)
        except:
            # Fallback to nested loop if primary key lookup fails
            for row in outer_results:
                join_value = str(row.get(join_strategy['outer_field'], ''))
                matching_pks = join_map.get(join_value, set())
                
                for inner_row in inner_results:
                    if str(inner_row.get(inner_pk_field, '')) in matching_pks:
                        joined_results.append({**row, **inner_row})
            return joined_results
        
        # Perform the join using hash tables
        for row in outer_results:
            join_value = str(row.get(join_strategy['outer_field'], ''))
            matching_pks = join_map.get(join_value, set())
            
            for pk in matching_pks:
                matching_inner_rows = inner_hash.get(pk, [])
                for inner_row in matching_inner_rows:
                    joined_results.append({**row, **inner_row})
    
    elif join_strategy['type'] == 'outer_index':
        # Similar logic for outer index, but reversed
        index_collection = get_collection(database_name, join_strategy['index_name'])
        
        # Batch collect unique join values from inner table
        join_values = set()
        for row in inner_results:
            val = row.get(join_strategy['inner_field'])
            if val is not None:
                join_values.add(str(val))
        
        if not join_values:
            return []
        
        # Batch fetch and process similar to inner_index case
        batch_size = 1000
        join_values_list = list(join_values)
        
        all_matching_records = []
        for i in range(0, len(join_values_list), batch_size):
            batch = join_values_list[i:i + batch_size]
            matching_records = list(index_collection.find({"_id": {"$in": batch}}))
            all_matching_records.extend(matching_records)
        
        join_map = defaultdict(set)
        for rec in all_matching_records:
            pks = rec.get("value", "").split("#")
            join_map[rec["_id"]].update(pks)
        
        # Build hash table for outer results
        outer_table = join_strategy['outer_field'].split('.')[0]
        try:
            pk_field = get_primary_key(database_name, outer_table)['primary_key']
            outer_pk_field = f"{outer_table}.{pk_field}"
            outer_hash = build_hash_table(outer_results, outer_pk_field)
        except:
            # Fallback
            for inner_row in inner_results:
                join_value = str(inner_row.get(join_strategy['inner_field'], ''))
                matching_pks = join_map.get(join_value, set())
                
                for row in outer_results:
                    if str(row.get(outer_pk_field, '')) in matching_pks:
                        joined_results.append({**row, **inner_row})
            return joined_results
        
        # Perform the join
        for inner_row in inner_results:
            join_value = str(inner_row.get(join_strategy['inner_field'], ''))
            matching_pks = join_map.get(join_value, set())
            
            for pk in matching_pks:
                matching_outer_rows = outer_hash.get(pk, [])
                for outer_row in matching_outer_rows:
                    joined_results.append({**outer_row, **inner_row})
    
    return joined_results

def execute_hash_join(outer_results: List[Dict], inner_results: List[Dict], 
                     outer_field: str, inner_field: str) -> List[Dict]:
    """Execute hash join when no indexes are available"""
    # Build hash table on smaller relation
    if len(outer_results) <= len(inner_results):
        # Build hash table on outer (smaller)
        hash_table = build_hash_table(outer_results, outer_field)
        joined_results = []
        
        for inner_row in inner_results:
            join_key = str(inner_row.get(inner_field, ''))
            matching_outer_rows = hash_table.get(join_key, [])
            for outer_row in matching_outer_rows:
                joined_results.append({**outer_row, **inner_row})
    else:
        # Build hash table on inner (smaller)
        hash_table = build_hash_table(inner_results, inner_field)
        joined_results = []
        
        for outer_row in outer_results:
            join_key = str(outer_row.get(outer_field, ''))
            matching_inner_rows = hash_table.get(join_key, [])
            for inner_row in matching_inner_rows:
                joined_results.append({**outer_row, **inner_row})
    
    return joined_results

def execute_join(
    database_name: str,
    tables: List[str],
    columns: List[str],
    conditions: List[Dict],
    join_conditions: List[Dict]
) -> List[Dict]:
    """Execute optimized indexed nested loop join with multiple fallback strategies"""
    
    total_start_time = time.time()
    
    # First, apply single-table conditions to each table
    filter_start_time = time.time()
    filtered_tables = {}
    for table in tables:
        # Get conditions for this table (without table prefix)
        table_conditions = [
            {
                "column": get_column_name(c["column"]),
                "operator": c["operator"],
                "value": c["value"] if "." not in str(c["value"]) else get_column_name(str(c["value"]))
            }
            for c in conditions 
            if get_table_from_column(c["column"]) == table
        ]
        
        # Get columns for this table
        table_columns = []
        if "*" in columns:
            db_info = load_databases()
            if database_name in db_info and "tables" in db_info[database_name] and table in db_info[database_name]["tables"]:
                table_info = db_info[database_name]["tables"][table]
                table_columns = list(table_info["columns"].keys())
        else:
            for col in columns:
                if get_table_from_column(col) == table:
                    table_columns.append(get_column_name(col))
        
        if not table_columns:
            db_info = load_databases()
            if database_name in db_info and "tables" in db_info[database_name] and table in db_info[database_name]["tables"]:
                table_info = db_info[database_name]["tables"][table]
                table_columns = list(table_info["columns"].keys())
        
        # Get results for this table
        results = execute_single_table_query(
            database_name=database_name,
            table_name=table,
            columns=table_columns,
            conditions=table_conditions
        )
        
        # Add table prefix to column names in results
        prefixed_results = []
        for row in results:
            prefixed_row = {}
            for col, val in row.items():
                prefixed_row[f"{table}.{col}"] = val
            prefixed_results.append(prefixed_row)
        
        filtered_tables[table] = prefixed_results
    
    filter_end_time = time.time()
    print(f"Table filtering time: {filter_end_time - filter_start_time:.4f} seconds")
    
    # Get optimal join order
    join_order_start_time = time.time()
    join_order = get_optimal_join_order(database_name, tables, join_conditions)
    join_order_end_time = time.time()
    print(f"Join order determination time: {join_order_end_time - join_order_start_time:.4f} seconds")
    print(f"Join order: {' -> '.join(join_order)}")
    
    # Start with the first table
    results = filtered_tables[join_order[0]]
    
    # For each subsequent table, perform the join
    join_execution_start_time = time.time()
    for i in range(1, len(join_order)):
        inner_table = join_order[i]
        outer_table = join_order[i-1] if i == 1 else "intermediate"
        
        # Get join conditions between current result and inner table
        relevant_join_conditions = []
        for cond in join_conditions:
            cond_tables = {get_table_from_column(cond["column"]), get_table_from_column(cond["value"])}
            if inner_table in cond_tables:
                # Check if the other table is in our current result set
                other_tables = set(join_order[:i])
                if cond_tables.intersection(other_tables):
                    relevant_join_conditions.append(cond)
                    join_conditions.remove(cond)  # Remove to avoid re-evaluation
        
        if not relevant_join_conditions:
            # No join conditions found, this might be a cartesian product
            print(f"Warning: No join conditions found between {join_order[:i]} and {inner_table}")
            continue
        
        # Find the best join strategy
        best_strategy = get_best_join_strategy(database_name, outer_table, inner_table, relevant_join_conditions)
        
        inner_results = filtered_tables[inner_table]
        
        if best_strategy:
            print(f"Using indexed join strategy: {best_strategy['type']} with index {best_strategy['index_name']}")
            joined_results = execute_indexed_join(database_name, results, inner_results, best_strategy, relevant_join_conditions)
        else:
            print(f"No suitable index found, using hash join for {inner_table}")
            # Use hash join as fallback
            if relevant_join_conditions:
                first_cond = relevant_join_conditions[0]
                outer_field = first_cond["column"] if get_table_from_column(first_cond["column"]) != inner_table else first_cond["value"]
                inner_field = first_cond["value"] if get_table_from_column(first_cond["value"]) == inner_table else first_cond["column"]
                joined_results = execute_hash_join(results, inner_results, outer_field, inner_field)
            else:
                # Fallback to nested loop join
                joined_results = []
                for outer_row in results:
                    for inner_row in inner_results:
                        match = True
                        for cond in relevant_join_conditions:
                            if not _evaluate_join_condition({**outer_row, **inner_row}, cond, database_name):
                                match = False
                                break
                        if match:
                            joined_results.append({**outer_row, **inner_row})
        
        results = joined_results
        print(f"After joining {inner_table}: {len(results)} rows")
    
    join_execution_end_time = time.time()
    print(f"Total join execution time: {join_execution_end_time - join_execution_start_time:.4f} seconds")
    
    # Apply any remaining join conditions that weren't handled during the join process
    final_conditions_start_time = time.time()
    final_results = []
    for row in results:
        if all(_evaluate_join_condition(row, cond, database_name) for cond in join_conditions):
            final_results.append(row)
    
    final_conditions_end_time = time.time()
    print(f"Final conditions evaluation time: {final_conditions_end_time - final_conditions_start_time:.4f} seconds")
    
    total_end_time = time.time()
    print(f"Total execution time: {total_end_time - total_start_time:.4f} seconds")
    print(f"Final result count: {len(final_results)} rows")
    
    return final_results