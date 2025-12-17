from pyspark.sql import DataFrame
from typing import Optional


# transformer module

# gets input pyspark df, name of column to add and sql function to use
# returns df with the new column added
# raises ValueError if function is not supported
def add_field_sql(df: DataFrame, col_name: str, function_name: str, 
                  batch_id: Optional[str] = None, run_id: Optional[str] = None) -> DataFrame:
    # function mapping
    function_map = {
        "current_timestamp": "current_timestamp()",
        "batch_id": f"'{batch_id}'" if batch_id else "NULL",
        "batch_date": f"to_date('{batch_id}')" if batch_id else "NULL",
        "run_id": f"'{run_id}'" if run_id else "NULL"
    }

    if function_name not in function_map:
        raise ValueError(
            f"Unsupported function: '{function_name}'. "
            f"Supported functions: {list(function_map.keys())}"
        )

    sql_function = function_map[function_name]

    # sparksql adds the column
    df.createOrReplaceTempView("tmp_transform_table")

    df_with_col = df.sql_ctx.sql(f"""
                                 SELECT *, {sql_function} AS {col_name}
                                 FROM tmp_transform_table
                                 """)

    return df_with_col

# reads input temp view, adds computed column
# gets spark session, input view (name of temp view to read from), list of field definitions from metadata (add_fields)
# optionally gets batch_id and run_id for batch metadata columns
# returns tuple (transformed df + number of records processed)
def execute_add_fields(spark, input_view: str, add_fields: list, 
                      batch_id: Optional[str] = None, run_id: Optional[str] = None) -> tuple:
    df_input = spark.table(input_view)
    records_processed = df_input.count()

    # apply each field addition
    for field in add_fields:
        field_name = field["name"]
        field_function = field["function"]
        df_input = add_field_sql(df_input, field_name, field_function, batch_id, run_id)

    return df_input, records_processed