from typing import Callable, Dict


# validator module
# validates records according to rules from metadata

# currently supported validation rules are simple rules - without parameters and parametrized rules - require parameters

# simple rules: 
# a) nutNull - field must not be null
# b) not Empty - field must not be empty string after trimming

# parametrized rules:
# a) regex - field must match regex pattern 
# b) minValue - field must be >= minimum value / numeric comparison

# for multiple rules per field, rules are evaluated independently and all failures are collected
# if a field is completely missing, validator automatically returns fieldMissing (without evaluating other rules)

# OK records / valid records have no validation_errors column
# KO records / invalid records have validation_errors map<string, array<string>> containing field [errors]

# rule registry and all validation rules are defined here


# generate sql for notNull validation
def _notNull_rule(field: str) -> str:
    return f"CASE WHEN {field} IS NULL THEN 'notNull' END"

# generate sql for notEmpty validation
def _notEmpty_rule(field: str) -> str:
    return (
        f"CASE WHEN {field} IS NOT NULL AND "
        f"trim(CAST({field} AS STRING)) = '' "
        f"THEN 'notEmpty' END"
    )

# generate sql for regex validation
def _regex_rule(field: str, pattern: str) -> str:
    # escape single quotes for sql
    safe_pattern = str(pattern).replace("'", "''")
    return (
        f"CASE WHEN {field} IS NOT NULL AND "
        f"NOT regexp_like(CAST({field} AS STRING), '{safe_pattern}') "
        f"THEN 'regex: {safe_pattern}' END"
    )

# generate sql for minVaue validation
def _minValue_rule(field: str, min_val) -> str:
    return (
        f"CASE WHEN {field} IS NOT NULL AND "
        f"CAST({field} AS DOUBLE) < {min_val} "
        f"THEN 'minValue: {min_val}' END"
    )


# simple rules, no params
SIMPLE_RULES: Dict[str, Callable[[str], str]] = {
    "notNull": _notNull_rule,
    "notEmpty": _notEmpty_rule,
}

# parameterized rules, require params 
PARAMETERIZED_RULES: Dict[str, Callable[[str, any], str]] = {
    "regex": _regex_rule,
    "minValue": _minValue_rule,
}


# validation: sql generation

# generates sparksql expressions that collect all validation failures per field
# for each validation config it generates expression that produces a <field>_error column 
# gets list of validation configs from metadata, list of column names in df
# returns list of sparksql expressions
# raises ValueError if meets unsupported validation rule
def generate_validation_sql(validations: list, df_columns: list) -> list:
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
                # simple string rules mentioned above
                if rule in SIMPLE_RULES:
                    conditions.append(SIMPLE_RULES[rule](field))
                else:
                    supported_simple = sorted(SIMPLE_RULES.keys())
                    raise ValueError(
                        f"Unsupported validation rule for field '{field}': '{rule}'. "
                        f"Supported rules: {supported_simple}"
                    )

            elif isinstance(rule, dict):
                 # parametrized rules mentioned above
                rule_name = rule.get("name")
                rule_params = rule.get("params")

                if rule_name in PARAMETERIZED_RULES:
                    conditions.append(PARAMETERIZED_RULES[rule_name](field, rule_params))
                else:
                    supported_parameterized = sorted(PARAMETERIZED_RULES.keys())
                    raise ValueError(
                        f"Unsupported validation rule for field '{field}': '{rule_name}'. "
                        f"Supported rules: {supported_parameterized}"
                    )
            else:
                raise ValueError(
                    f"Invalid validation rule configuration for field '{field}': {rule}. "
                )

        if not conditions:
            sql_exprs.append(f"CAST(NULL AS array<string>) AS {field}_error")
            continue

        # build array of rule results, remove nulls, return null if empty
        array_expr = f"array({', '.join(conditions)})"
        sql_expr = f"nullif(array_compact({array_expr}), array()) AS {field}_error"

        sql_exprs.append(sql_expr)

    return sql_exprs


# execute validation 

# reads input temp view, applies validation rules, splits into OK or KO dfs 
# gets spark session, input view/name of temp view to validate, list of validation configs from metadata
# returns (as tuple):
# df with valid records (df_ok); no validation_errors column
# df with invalid records (df_ko); includes validation_errors map
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