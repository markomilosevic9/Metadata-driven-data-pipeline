from pyspark.sql import SparkSession
from pipeline.sink import write_df


# consolidator module

# handles consolidation of batch outputs into unified and deduplicated data
# supports incremental consolidation to avoid reprocessing all batches

# read multiple batches and dedup by key column (policy_number)
def deduplicate_records(spark: SparkSession, 
                        input_path: str, 
                        key_column: str, 
                        order_by: str, 
                        order_direction: str = "DESC") -> tuple:

    # read all batches
    df_all = spark.read.format("json").option("mode", "PERMISSIVE").load(input_path)
    
    total_before = df_all.count()
    
    # create temp view for deduplication using sql
    df_all.createOrReplaceTempView("all_batches_temp")
    
    # get all column names except for row_num
    columns = df_all.columns
    columns_str = ", ".join(columns)
    
    # apply deduplication using window function approach
    dedup_sql = f"""
                SELECT {columns_str}
                FROM (
                    SELECT *, 
                        ROW_NUMBER() OVER (
                            PARTITION BY {key_column} 
                            ORDER BY {order_by} {order_direction}
                        ) AS row_num
                    FROM all_batches_temp
                ) ranked
                WHERE row_num = 1
                """
    
    df_dedup = spark.sql(dedup_sql)
    total_after = df_dedup.count()
    
    return df_dedup, total_before, total_after

# consolidate and deduplicate OK/valid records from all batches
# supports both full and incremental modes
def consolidate_ok_records(spark: SparkSession, config: dict, metadata: dict) -> dict:
    consolidation_config = metadata.get("consolidation", {})
    
    if not consolidation_config.get("enabled", False):
        return {"status": "skipped", "reason": "Consolidation not enabled"}
    
    ok_config = consolidation_config.get("ok_records", {})
    
    # get paths and deduplication settings
    input_pattern = ok_config.get("input_pattern")  # per-batch outputs
    output_path = ok_config.get("output_path")  # consolidated output
    dedup_config = ok_config.get("deduplication", {})
    
    if not dedup_config.get("enabled", False):
        # consolidation enabled but deduplication disabled, just copy all records
        df_all = spark.read.format("json").option("mode", "PERMISSIVE").load(input_pattern)
        record_count = df_all.count()
        
        write_df(df_all, output_path, fmt="json", mode="overwrite")
        
        return {
            "status": "success",
            "deduplication_enabled": False,
            "total_records": record_count,
            "output_path": output_path
        }
    
    # deduplication enabled - check if consolidated output already exists
    consolidated_exists = False
    df_existing_consolidated = None
    
    try:
        # read files
        df_existing_consolidated = spark.read.format("json").option("mode", "PERMISSIVE").load(f"{output_path}/*.json")
        existing_count = df_existing_consolidated.count()
        if existing_count > 0:
            consolidated_exists = True
    except:
        # if consolidated output does not exist yet, perform full consolidation
        consolidated_exists = False
    
    # read all per-batch outputs
    df_all_batches = spark.read.format("json").option("mode", "PERMISSIVE").load(input_pattern)
    batch_count = df_all_batches.count()
    
    key_column = dedup_config.get("key_column", "policy_number")
    order_by = dedup_config.get("order_by", "batch_date")
    order_direction = dedup_config.get("order_direction", "DESC")
    
    if consolidated_exists:
        # incremental mode, union existing consolidated + all per-batch outputs, then deduplicate
        df_all_batches.createOrReplaceTempView("batches_temp")
        df_existing_consolidated.createOrReplaceTempView("consolidated_temp")
        
        # get all columns (assumption is that per-batch and consolidated have same schema)
        columns = df_all_batches.columns
        columns_str = ", ".join(columns)
        
        # union all + deduplicate in one query
        incremental_dedup_sql = f"""
        SELECT {columns_str}
        FROM (
            SELECT *, 
                ROW_NUMBER() OVER (
                    PARTITION BY {key_column} 
                    ORDER BY {order_by} {order_direction}
                ) AS row_num
            FROM (
                SELECT * FROM batches_temp
                UNION ALL
                SELECT * FROM consolidated_temp
            ) combined
        ) ranked
        WHERE row_num = 1
        """
        
        df_dedup = spark.sql(incremental_dedup_sql)
        total_after = df_dedup.count()
        
        # write consolidated output
        write_df(df_dedup, output_path, fmt="json", mode="overwrite")
        
        return {
            "status": "success",
            "consolidation_mode": "incremental",
            "deduplication_enabled": True,
            "key_column": key_column,
            "order_by": order_by,
            "order_direction": order_direction,
            "existing_consolidated_records": existing_count,
            "per_batch_records": batch_count,
            "total_records_after": total_after,
            "output_path": output_path
        }
    
    else:
        # for first time consolidation, just deduplicate all currently existing per-batch outputs
        df_dedup, total_before, total_after = deduplicate_records(
            spark, input_pattern, key_column, order_by, order_direction
        )
        
        # write consolidated output
        write_df(df_dedup, output_path, fmt="json", mode="overwrite")
        
        duplicates_removed = total_before - total_after
        
        return {
            "status": "success",
            "consolidation_mode": "full",
            "deduplication_enabled": True,
            "key_column": key_column,
            "order_by": order_by,
            "order_direction": order_direction,
            "total_records_before": total_before,
            "total_records_after": total_after,
            "duplicates_removed": duplicates_removed,
            "output_path": output_path
        }

# main consolidation function
def consolidate_data(spark: SparkSession, config: dict, metadata: dict) -> dict:
    results = {}
    
    # consolidate OK records
    ok_result = consolidate_ok_records(spark, config, metadata)
    results["ok_records"] = ok_result
    
    # KO records are not consolidated 
    results["ko_records"] = {
        "status": "skipped",
        "reason": "KO records stay in per-batch folders"
    }
    
    return results