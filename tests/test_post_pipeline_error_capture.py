import pytest
from pyspark.sql import functions as F

# the 3rd post-pipeline test, aims to capture error validation
# actually, checks if rejected/discarded records (KO) has proper error info (meaning error tracking/capture works correctly)


# all KO records must have validation_errors column
# validation_errors must not be non-null map
# validation_errors must have at least 1 error
# error field names must match expected validation fields
@pytest.mark.post_pipeline
def test_ko_records_have_validation_errors(spark):
    # get path
    ko_path = "s3a://motor-policy-ko/output"
    
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    total_ko = df_ko.count()
    
    # 1st check, validation_errors column exists 
    assert "validation_errors" in df_ko.columns, "KO records missing validation_errors column"
    
    # 2nd check, validation_errors must not be null
    null_errors = df_ko.filter(F.col("validation_errors").isNull()).count()
    assert null_errors == 0, f"Found {null_errors} KO records with null validation_errors, should be empty map instead"
    
    # 3rd check, validation_errors struct should have at least 1 field with errors
    # get schema to inspect fields
    validation_errors_fields = [field.name for field in df_ko.schema["validation_errors"].dataType.fields]
    assert len(validation_errors_fields) > 0, "validation_errors struct has no fields, schema has problem"
    
    # 4th check, error field names match expected validation fields
    expected_fields = {"plate_number", "driver_age", "policy_number"}
    
    actual_error_fields = set(validation_errors_fields)
    # error fields should be subset of expected fields
    unexpected_fields = actual_error_fields - expected_fields
    assert not unexpected_fields, f"Found unexpected error fields: {unexpected_fields}, expected: {expected_fields}"
    
    # finally, summary 
    assert total_ko > 0, "No KO records found, cannot validate error capture"