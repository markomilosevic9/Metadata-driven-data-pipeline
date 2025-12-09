import pytest
from pyspark.sql import functions as F
from pipeline.config_loader import load_config, get_storage_config


# post-pipeline output completeness tests
# checks if pipeline execution produced expected outputs with correct structure


# checks if pipeline produced outputs in both OK and KO buckets, while checking basic execution success
@pytest.mark.post_pipeline
def test_pipeline_outputs_exist(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ko_bucket = buckets.get('output_ko', 'motor-policy-ko')
    
    ok_path = f"s3a://{ok_bucket}/output"
    ko_path = f"s3a://{ko_bucket}/output"
    
    # read outputs
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    
    ok_count = df_ok.count()
    ko_count = df_ko.count()
    
    # both buckets should contain data
    assert ok_count > 0, "OK bucket is empty - no valid records produced"
    assert ko_count > 0, "KO bucket is empty - no invalid records captured"

# check the count - if there is data loss or duplication (input = OK + KO)
# check if each input record was processed and routed to exactly one output
@pytest.mark.post_pipeline
def test_record_counts_correct(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    input_bucket = buckets.get('input', 'input-data')
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ko_bucket = buckets.get('output_ko', 'motor-policy-ko')
    
    input_path = f"s3a://{input_bucket}/input*.jsonl"
    ok_path = f"s3a://{ok_bucket}/output"
    ko_path = f"s3a://{ko_bucket}/output"
    
    # reads data
    df_input = spark.read.format("json").option("mode", "PERMISSIVE").load(input_path)
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    
    input_count = df_input.count()
    ok_count = df_ok.count()
    ko_count = df_ko.count()
    total_output = ok_count + ko_count
    
    # check for data loss or duplication
    assert total_output == input_count, \
        f"Record count mismatch - Input: {input_count}, OK: {ok_count}, KO: {ko_count}, total output: {total_output}. " \
        f"Expected: OK + KO = Input"

# checks if both OK and KO outputs have correct schema structure
# both have required fields (policy_number, ingestion_dt)
# OK records do not have validation_errors, but KO do have
@pytest.mark.post_pipeline
def test_output_schema_correctness(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ko_bucket = buckets.get('output_ko', 'motor-policy-ko')
    
    ok_path = f"s3a://{ok_bucket}/output"
    ko_path = f"s3a://{ko_bucket}/output"
    
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    
    # check if required columns are present in both
    assert "policy_number" in df_ok.columns, "OK records missing policy_number column"
    assert "policy_number" in df_ko.columns, "KO records missing policy_number column"
    
    assert "ingestion_dt" in df_ok.columns, "OK records missing ingestion_dt column"
    assert "ingestion_dt" in df_ko.columns, "KO records missing ingestion_dt column"
    
    # check validation_errors column in both buckets
    assert "validation_errors" not in df_ok.columns, \
        "OK records should NOT have validation_errors column - these are valid records"
    
    assert "validation_errors" in df_ko.columns, \
        "KO records missing validation_errors column - error details required"

# checks if there are no duplicate policy_number in outputs
# each input record should appear exactly once (in either OK or KO)
@pytest.mark.post_pipeline
def test_no_duplicate_records(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ko_bucket = buckets.get('output_ko', 'motor-policy-ko')
    
    ok_path = f"s3a://{ok_bucket}/output"
    ko_path = f"s3a://{ko_bucket}/output"
    
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    
    # check for duplicates within OK bucket
    ok_total = df_ok.count()
    ok_distinct = df_ok.select("policy_number").distinct().count()
    assert ok_total == ok_distinct, \
        f"Found duplicate policy_numbers in OK bucket: {ok_total} records, {ok_distinct} unique"
    
    # check for duplicates within KO bucket
    ko_total = df_ko.count()
    ko_distinct = df_ko.select("policy_number").distinct().count()
    assert ko_total == ko_distinct, \
        f"Found duplicate policy_numbers in KO bucket: {ko_total} records, {ko_distinct} unique"
    
    # check for records in both OK and KO (critical issue)
    df_ok_policies = df_ok.select("policy_number").withColumnRenamed("policy_number", "policy_ok")
    df_ko_policies = df_ko.select("policy_number").withColumnRenamed("policy_number", "policy_ko")
    
    overlap = df_ok_policies.join(
        df_ko_policies, 
        df_ok_policies.policy_ok == df_ko_policies.policy_ko, 
        "inner"
    ).count()
    
    assert overlap == 0, \
        f"Found {overlap} policy_numbers in BOTH OK and KO buckets - records should be in exactly one bucket"