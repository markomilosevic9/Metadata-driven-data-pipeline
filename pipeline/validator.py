# validator module

# generates sparksql expressions that collect all validation failures per field
# for each validation config it generates expression that produces a `<field>_error` column of type array<string> or null
# if the field is missing from json schema entirely: (array('fieldMissing') AS <field>_error)
# if the field exists: collects all failing rule names into array (e.g. ['notNull', 'minValue: 18']); returns null if no rules fail for that field

# gets list of validation configs from metadata, df columns 
# returns list of sparksql expressions
def generate_validation_sql(validations, df_columns):
    sql_exprs = []

    for v in validations:
        field = v["field"]
        rules = v["rules"]

        # handle completely missing fields
        if field not in df_columns:
            # if column not present at all, always error with 'fieldMissing'
            sql_exprs.append(f"array('fieldMissing') AS {field}_error")
            continue

        conditions = []

        for rule in rules:
            if isinstance(rule, str):
                # simple string rules
                if rule == "notNull":
                    conditions.append(
                        f"CASE WHEN {field} IS NULL THEN 'notNull' END"
                    )

                elif rule == "notEmpty":
                    # check for empty/whitespace string, distinct from NULL
                    conditions.append(
                        f"CASE WHEN {field} IS NOT NULL AND "
                        f"trim(CAST({field} AS STRING)) = '' "
                        f"THEN 'notEmpty' END"
                    )

                else:
                    # only support notNull / notEmpty as simple rules for demo
                    raise ValueError(
                        f"Unsupported validation rule for field '{field}': '{rule}'"
                    )

            elif isinstance(rule, dict):
                # rules with parameters
                rule_name = rule["name"]
                rule_params = rule.get("params")

                if rule_name == "regex":
                    # escape single quotes for sql (double them)
                    safe_pattern = str(rule_params).replace("'", "''")

                    conditions.append(
                        f"CASE WHEN {field} IS NOT NULL AND "
                        f"NOT regexp_like(CAST({field} AS STRING), '{safe_pattern}') "
                        f"THEN 'regex: {safe_pattern}' END"
                    )

                elif rule_name == "minValue":
                    conditions.append(
                        f"CASE WHEN {field} IS NOT NULL AND "
                        f"CAST({field} AS DOUBLE) < {rule_params} "
                        f"THEN 'minValue: {rule_params}' END"
                    )

                else:
                    # drop any unused/unsupported types like max value, length or whatever
                    raise ValueError(
                        f"Unsupported validation rule for field '{field}': '{rule_name}'"
                    )
            else:
                raise ValueError(
                    f"Invalid validation rule configuration for field '{field}': {rule}"
                )

        if not conditions:
            # if no usable conditions then no validation; always NULL error column
            sql_exprs.append(f"CAST(NULL AS array<string>) AS {field}_error")
            continue

        # build array of rule results, remove nulls with array_compact
        # then return null if empty using nullif
        array_expr = f"array({', '.join(conditions)})"
        sql_expr = f"nullif(array_compact({array_expr}), array()) AS {field}_error"

        sql_exprs.append(sql_expr)

    return sql_exprs


# execute validation on input view
# reads input temp view, applies validation rules, splits into OK or KO dfs according to these rules
# returns (as tuple):
# df with valid records (df_ok)
# df with invalid records (df_ko); includes validation_errors
# ok count / number of valid records (ok_count)
# ko count / number of invalid records (ko_count)
# total count of processed records (total_count)
def execute_validation(spark, input_view: str, validations: list) -> tuple:
    # get input df and columns
    df_input = spark.table(input_view)
    input_columns = df_input.columns

    # generate validation sql expressions
    validation_exprs = generate_validation_sql(validations, input_columns)

    # make validated view with error columns
    validation_query = f"""
                        SELECT *, {', '.join(validation_exprs)}
                        FROM {input_view}
                        """

    df_validated = spark.sql(validation_query)
    df_validated.createOrReplaceTempView("validated")

    # identify original columns, exclude error columns
    original_columns = [
        col for col in df_validated.columns
        if not col.endswith('_error')
    ]

    # make error column list and conditions
    error_columns = [f"{v['field']}_error" for v in validations]
    error_condition = " OR ".join([f"{c} IS NOT NULL" for c in error_columns])
    error_map_entries = [f"'{v['field']}', {v['field']}_error" for v in validations]

    # make KO df for records with validation errors
    df_ko = spark.sql(f"""
                        SELECT 
                            {', '.join(original_columns)},
                            map_filter(
                                map({', '.join(error_map_entries)}),
                                (k, v) -> v IS NOT NULL
                            ) AS validation_errors
                        FROM validated 
                        WHERE {error_condition}
                        """)

    # make OK df for records without validation errors
    df_ok = spark.sql(f"""
                        SELECT {', '.join(original_columns)}
                        FROM validated 
                        WHERE NOT ({error_condition})
                       """)

    # get counts
    ok_count = df_ok.count()
    ko_count = df_ko.count()
    total_count = ok_count + ko_count

    return df_ok, df_ko, ok_count, ko_count, total_count