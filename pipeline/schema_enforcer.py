from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    TimestampType,
    DateType
)


# schema enforcement module
# enables explicit schema enforcement at read time

# raised when schema enforcement fails during data read
# indicates structural mismatch between expected schema and actual data
class SchemaEnforcementError(Exception):
    pass


# type (re)mapping from json metadata to spark types
TYPE_MAPPING = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "float": FloatType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
    "date": DateType()
}

# converts json schema definitions from metadata to spark structtype
# returns spark structtype ready for use
# raises ValueError if schema definition is invalid or contains unsupported types
# or SchemaEnforcementError if schema structure is malformed
def build_spark_schema(schema_def: dict) -> StructType:
    # checks top-level schema structure
    if not isinstance(schema_def, dict):
        raise SchemaEnforcementError(
            f"Schema definition must be a dictionary, got {type(schema_def)}"
        )
    
    if schema_def.get("type") != "struct":
        raise SchemaEnforcementError(
            f"Schema type must be 'struct', got '{schema_def.get('type')}'"
        )
    
    if "fields" not in schema_def:
        raise SchemaEnforcementError("Schema definition missing 'fields' key")
    
    fields_list = schema_def["fields"]
    
    if not isinstance(fields_list, list):
        raise SchemaEnforcementError(
            f"Schema 'fields' must be a list, got {type(fields_list)}"
        )
    
    if len(fields_list) == 0:
        raise SchemaEnforcementError("Schema must define at least one field")
    
    # build list of struct fields
    spark_fields = []
    
    for idx, field in enumerate(fields_list):
        # validate field structure
        if not isinstance(field, dict):
            raise SchemaEnforcementError(
                f"Field at index {idx} must be a dictionary, got {type(field)}"
            )
        
        # extract required field properties
        field_name = field.get("name")
        field_type = field.get("type")
        field_nullable = field.get("nullable")
        
        # validate  if required properties exist
        if field_name is None:
            raise SchemaEnforcementError(
                f"Field at index {idx} missing 'name' property"
            )
        
        if field_type is None:
            raise SchemaEnforcementError(
                f"Field '{field_name}' missing 'type' property"
            )
        
        if field_nullable is None:
            raise SchemaEnforcementError(
                f"Field '{field_name}' missing 'nullable' property"
            )
        
        # validate property types
        if not isinstance(field_name, str):
            raise SchemaEnforcementError(
                f"Field name must be string, got {type(field_name)} for field at index {idx}"
            )
        
        if not isinstance(field_type, str):
            raise SchemaEnforcementError(
                f"Field '{field_name}' type must be string, got {type(field_type)}"
            )
        
        if not isinstance(field_nullable, bool):
            raise SchemaEnforcementError(
                f"Field '{field_name}' nullable must be boolean, got {type(field_nullable)}"
            )
        
        # map json type to spark type
        if field_type not in TYPE_MAPPING:
            supported_types = sorted(TYPE_MAPPING.keys())
            raise ValueError(
                f"Unsupported type '{field_type}' for field '{field_name}'. "
                f"Supported types: {supported_types}"
            )
        
        spark_type = TYPE_MAPPING[field_type]
        
        # create struct field
        # kept in json for documentation purposes only
        struct_field = StructField(field_name, spark_type, field_nullable)
        
        spark_fields.append(struct_field)
    
    # create and return structtype
    return StructType(spark_fields)