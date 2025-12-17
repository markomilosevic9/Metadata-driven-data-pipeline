import pytest
from pyspark.sql import functions as F
from pipeline.config_loader import load_config, get_storage_config


# post-pipeline output completeness tests
# checks if pipeline execution produced expected outputs with correct structure


# checks if pipeline produced outputs in both OK and KO buckets per-batch folders
@pytest.mark.post_pipeline
def test_pipeline_batch_outputs_exist(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ko_bucket = buckets.get('output_ko', 'motor-policy-ko')
    
    # read outputs from all batch folders 
    ok_path = f"s3a://{ok_bucket}/batch-*/output/*.json"
    ko_path = f"s3a://{ko_bucket}/batch-*/output/*.json"
    
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    
    ok_count = df_ok.count()
    ko_count = df_ko.count()
    
    # both buckets should contain data
    assert ok_count > 0, "OK bucket is empty - no valid records produced"
    assert ko_count > 0, "KO bucket is empty - no invalid records captured"

# check if consolidated output exists
@pytest.mark.post_pipeline
def test_consolidated_output_exists(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    consolidated_bucket = buckets.get('output_ok_consolidated', 'motor-policy-ok-consolidated')
    consolidated_path = f"s3a://{consolidated_bucket}/output/*.json"
    
    df_consolidated = spark.read.format("json").option("mode", "PERMISSIVE").load(consolidated_path)
    consolidated_count = df_consolidated.count()
    
    assert consolidated_count > 0, "Consolidated bucket is empty, consolidation may have failed"

# check the count - if there is data loss or duplication (input = OK + KO across all batches)
# check if each input record was processed and routed to exactly one output
@pytest.mark.post_pipeline
def test_record_counts_correct(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    input_bucket = buckets.get('input', 'input-data')
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ko_bucket = buckets.get('output_ko', 'motor-policy-ko')
    
    # read all batches 
    input_path = f"s3a://{input_bucket}/batch-*/input*.jsonl"
    ok_path = f"s3a://{ok_bucket}/batch-*/output/*.json"
    ko_path = f"s3a://{ko_bucket}/batch-*/output/*.json"
    
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
    
    ok_path = f"s3a://{ok_bucket}/batch-*/output/*.json"
    ko_path = f"s3a://{ko_bucket}/batch-*/output/*.json"
    
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

# checks if batch detail columns exist in outputs
@pytest.mark.post_pipeline
def test_batch_detail_columns_exist(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ko_bucket = buckets.get('output_ko', 'motor-policy-ko')
    
    ok_path = f"s3a://{ok_bucket}/batch-*/output/*.json"
    ko_path = f"s3a://{ko_bucket}/batch-*/output/*.json"
    
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    
    # check batch detail columns in OK records
    assert "source_batch" in df_ok.columns, "OK records missing source_batch column"
    assert "batch_date" in df_ok.columns, "OK records missing batch_date column"
    assert "processed_run_id" in df_ok.columns, "OK records missing processed_run_id column"
    
    # check batch detail columns in KO records
    assert "source_batch" in df_ko.columns, "KO records missing source_batch column"
    assert "batch_date" in df_ko.columns, "KO records missing batch_date column"
    assert "processed_run_id" in df_ko.columns, "KO records missing processed_run_id column"

# checks if there are no duplicate policy_number in per-batch outputs
@pytest.mark.post_pipeline
def test_no_duplicate_records_per_batch(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    ok_bucket = buckets.get('output_ok', 'motor-policy-ok')
    ko_bucket = buckets.get('output_ko', 'motor-policy-ko')
    
    ok_path = f"s3a://{ok_bucket}/batch-*/output/*.json"
    ko_path = f"s3a://{ko_bucket}/batch-*/output/*.json"
    
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    
    # check for duplicates within each batch in OK bucket
    # group by source_batch and policy_number, count occurrences
    ok_duplicates = df_ok.groupBy("source_batch", "policy_number").count().filter(F.col("count") > 1)
    ok_dup_count = ok_duplicates.count()
    
    assert ok_dup_count == 0, \
        f"Found {ok_dup_count} duplicate policy_numbers within batches in OK bucket"
    
    # check for duplicates within each batch in KO bucket
    ko_duplicates = df_ko.groupBy("source_batch", "policy_number").count().filter(F.col("count") > 1)
    ko_dup_count = ko_duplicates.count()
    
    assert ko_dup_count == 0, \
        f"Found {ko_dup_count} duplicate policy_numbers within batches in KO bucket"

# check if consolidated output has no duplicates
@pytest.mark.post_pipeline
def test_consolidated_no_duplicates(spark):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets = storage_config.get('buckets', {})
    
    consolidated_bucket = buckets.get('output_ok_consolidated', 'motor-policy-ok-consolidated')
    consolidated_path = f"s3a://{consolidated_bucket}/output/*.json"
    
    df_consolidated = spark.read.format("json").option("mode", "PERMISSIVE").load(consolidated_path)
    
    # check for duplicates
    total_count = df_consolidated.count()
    distinct_count = df_consolidated.select("policy_number").distinct().count()
    
    assert total_count == distinct_count, \
        f"Found duplicate policy_numbers in consolidated output: {total_count} records, {distinct_count} unique"