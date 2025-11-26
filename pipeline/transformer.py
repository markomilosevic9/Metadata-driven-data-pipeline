from pyspark.sql import DataFrame

# transformer module


# add computed column (current timestamp), returns df with the new column 
def add_field_sql(df: DataFrame, col_name: str, function_name: str) -> DataFrame:
    # atm only what is actually used in pipeline; it is possible to extend map
    function_map = {
        "current_timestamp": "current_timestamp()"
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

# executes aforementioned add_fields 
# reads input temp view, adds computed column, returns tuple (df + number of records processed)
def execute_add_fields(spark, input_view: str, add_fields: list) -> tuple:
    df_input = spark.table(input_view)
    records_processed = df_input.count()

    # apply addition
    for field in add_fields:
        field_name = field["name"]
        field_function = field["function"]
        df_input = add_field_sql(df_input, field_name, field_function)

    return df_input, records_processed