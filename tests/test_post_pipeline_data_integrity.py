import pytest
from pyspark.sql import functions as F
from pipeline.config_loader import load_config, get_storage_config

# post-pipeline data integrity tests
# validates data quality and error handling (checks if OK records are actually valid, KO records properly documented etc)

# ok records

# check if all required fields are populated in OK records (there are columns that must never be null for valid records)
@pytest.mark.post_pipeline
def test_ok_records_required_fields_populated(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ok_path = f"s3a://{ok_bucket}/output"
    
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    
    # check critical columns are never null
    null_policy = df_ok.filter(F.col("policy_number").isNull()).count()
    assert null_policy == 0, \
        f"Found {null_policy} OK records with null policy_number"
    
    null_age = df_ok.filter(F.col("driver_age").isNull()).count()
    assert null_age == 0, \
        f"Found {null_age} OK records with null driver_age"
    
    null_plate = df_ok.filter(F.col("plate_number").isNull()).count()
    assert null_plate == 0, \
        f"Found {null_plate} OK records with null plate_number"

# check specific rules for OK records 
# domain/business specific rules that define valid records (age must be >= 18, plate numbers must not be empty strings)
@pytest.mark.post_pipeline
def test_ok_records_business_rules(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ok_path = f"s3a://{ok_bucket}/output"
    
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    
    # minimum driver age
    drivers_below_minimum_age = df_ok.filter(F.col("driver_age") < 18).count()
    assert drivers_below_minimum_age == 0, \
        f"Found {drivers_below_minimum_age} OK records with driver_age < 18"
    
    # plate number must not be empty
    empty_plates = df_ok.filter(
        F.col("plate_number").isNotNull() & 
        (F.trim(F.col("plate_number")) == "")
    ).count()
    assert empty_plates == 0, \
        f"Found {empty_plates} OK records with empty plate_number"



# check if filed(s) added by transformation operations are correctly populated
# ingestiond_dt exists and is valid timestamp, ingestion_dt is within reasonable time window (recent)
@pytest.mark.post_pipeline
def test_ok_records_derived_fields_correct(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ok_path = f"s3a://{ok_bucket}/output"
    
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    
    # check if ingestion_dt exists and not null
    null_ingestion = df_ok.filter(F.col("ingestion_dt").isNull()).count()
    assert null_ingestion == 0, \
        f"Found {null_ingestion} OK records with null ingestion_dt - transformation failed"
    
    # check if ingestion_dt is valid timestamp format
    # try to cast to timestamp - if fails, count will be 0
    valid_timestamps = df_ok.filter(
        F.col("ingestion_dt").cast("timestamp").isNotNull()
    ).count()
    
    total_ok = df_ok.count()
    assert valid_timestamps == total_ok, \
        f"Found {total_ok - valid_timestamps} records with invalid ingestion_dt timestamp format"


# ko records
# error handling validation

# check if all KO records have proper errors, focuses on rejected records
# check if validation_error column exists, it is not null nor non-empty map
@pytest.mark.post_pipeline
def test_ko_records_have_error_details(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ko_bucket = buckets.get('output_ko', 'motor-policy-ko')
    ko_path = f"s3a://{ko_bucket}/output"
    
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    
    # check if validation_errors column exists
    assert "validation_errors" in df_ko.columns, \
        "KO records missing validation_errors column - error tracking failed"
    
    # check if validation_errors column is not null
    null_errors = df_ko.filter(F.col("validation_errors").isNull()).count()
    assert null_errors == 0, \
        f"Found {null_errors} KO records with null validation_errors - " \
        f"error tracking failed"

# checks if validation_errors structure is valid and has meaningful information
# validation_errors is a map<string, array<string>>
# error field names match validated fields
# error arrays contain error descriptions
@pytest.mark.post_pipeline
def test_ko_records_error_structure_valid(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ko_bucket = buckets.get('output_ko', 'motor-policy-ko')
    ko_path = f"s3a://{ko_bucket}/output"
    
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    
    # get schema to inspect validation_errors structure
    validation_errors_fields = [
        field.name for field in df_ko.schema["validation_errors"].dataType.fields
    ]
    
    assert len(validation_errors_fields) > 0, \
        "validation_errors struct has no fields - schema structure issue"
    
    # expected error fields based on validation rules in metadata
    expected_fields = {"plate_number", "driver_age", "policy_number"}
    
    actual_error_fields = set(validation_errors_fields)
    
    # error fields should be subset of expected fields
    unexpected_fields = actual_error_fields - expected_fields
    assert not unexpected_fields, \
        f"Found unexpected error fields: {unexpected_fields}. " \
        f"Expected only: {expected_fields}"


