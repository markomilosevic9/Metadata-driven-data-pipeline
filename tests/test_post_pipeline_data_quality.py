import pytest
from pyspark.sql import functions as F

# the 2nd post-pipeline test, aims for data quality
# checks if accepted records (already marked as OK) actually pass all validation rules


# actually, re-applies validation logic again 
# policy_number col - no OK record has: null policy_number
# driver_age col - no OK record has: null driver_age, driver_age < 18
# plate_number col - no OK record has: null plate_number, empty plate_number, invalid plate_number format (must match pattern)
# ingestion_dt - no OK records without this column, must not be null - checks for earlier transform stage
# if any OK records fails these checks, the pipeline has a bug
@pytest.mark.post_pipeline
def test_ok_records_pass_validation(spark):
    # get path
    ok_path = "s3a://motor-policy-ok/output"
    
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    
    # 1st re-validation, policy_number must not be null
    null_policy = df_ok.filter(F.col("policy_number").isNull()).count()
    assert null_policy == 0, f"Found {null_policy} OK records with null policy_number, validation failed"
    
    # 2nd re-validation, driver_age must not be null
    null_age = df_ok.filter(F.col("driver_age").isNull()).count()
    assert null_age == 0, f"Found {null_age} OK records with null driver_age, validation failed"
    
    # 3rd re-validation, driver_age must not be < 18
    age_not_allowed = df_ok.filter(F.col("driver_age") < 18).count()
    assert age_not_allowed == 0, f"Found {age_not_allowed} OK records with driver_age < 18, validation failed"
    
    # 4th re-validation, plate_number must not be null
    null_plate = df_ok.filter(F.col("plate_number").isNull()).count()
    assert null_plate == 0, f"Found {null_plate} OK records with null plate_number, validation failed"
    
    # 5th re-validation, plate_number must not be empty (added trimming)
    empty_plate = df_ok.filter(
        F.col("plate_number").isNotNull() & 
        (F.trim(F.col("plate_number")) == "")
    ).count()
    assert empty_plate == 0, f"Found {empty_plate} OK records with empty plate_number, validation failed"
    
    # 6th re-validation, plate_number must match regex pattern (^[A-Z0-9-]+$)
    invalid_plate_format = df_ok.filter(
        F.col("plate_number").isNotNull() &
        ~F.col("plate_number").rlike("^[A-Z0-9-]+$")
    ).count()
    assert invalid_plate_format == 0, f"Found {invalid_plate_format} OK records with invalid plate_number format, validation failed"
    
    # 7th re-validation, aims for earlier transformation, ingestion_dt must exist and not be null
    null_ingestiondt = df_ok.filter(F.col("ingestion_dt").isNull()).count()
    assert null_ingestiondt == 0, f"Found {null_ingestiondt} OK records with null ingestion_dt, transformation failed"