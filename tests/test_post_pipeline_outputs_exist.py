import pytest

# the 1st post-pipeline test, basic pipeline execution success
# checks if pipeline outputs exist in correct buckets (they are not empty) 
# checks if record counts match


# checks: ok bucket has data, ko bucket has data, total record count (ok + ko) equals input records, proposed column structure is fine
@pytest.mark.post_pipeline
def test_pipeline_outputs_exist_and_counts_match(spark):
    # paths
    input_path = "s3a://input-data/input*.jsonl"
    ok_path = "s3a://motor-policy-ok/output"
    ko_path = "s3a://motor-policy-ko/output"
    
    # get input data
    df_input = spark.read.format("json").option("mode", "PERMISSIVE").load(input_path)
    input_count = df_input.count()
    
    # get output data
    df_ok = spark.read.format("json").option("mode", "PERMISSIVE").load(ok_path)
    df_ko = spark.read.format("json").option("mode", "PERMISSIVE").load(ko_path)
    
    ok_count = df_ok.count()
    ko_count = df_ko.count()
    total_output = ok_count + ko_count
    
    # check if outputs exist
    assert ok_count > 0, "OK bucket is empty,  no records found"
    assert ko_count > 0, "KO bucket is empty, no records found"
    
    # check if there is count (mis)match
    assert total_output == input_count, f"Count mismatch: input={input_count}, OK={ok_count}, KO={ko_count}, total otput={total_output}"
    
    # check for basic elements of proposed structure
    # per design, the policy_number is must have column for each output file, as well as ingestion_dt

    # policy_number column is a must have
    assert "policy_number" in df_ok.columns, "OK records missing policy_number column"
    assert "policy_number" in df_ko.columns, "KO records missing policy_number column"
    
    # ingestion_dt column is a must have
    assert "ingestion_dt" in df_ok.columns, "OK records missing ingestion_dt column"
    assert "ingestion_dt" in df_ko.columns, "KO records missing ingestion_dt column"
    
    # ok records should not have validation_errors
    assert "validation_errors" not in df_ok.columns, "OK records should not have validation_errors column"
    
    # ko records must have validation_errors
    assert "validation_errors" in df_ko.columns, "KO records missing validation_errors column"