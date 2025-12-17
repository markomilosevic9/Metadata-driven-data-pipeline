import json
import pytest
from pathlib import Path
from minio.error import S3Error
from pipeline.config_loader import load_config, get_storage_config, get_spark_master_url


# pre-pipeline (smoke) tests focused on infrastructure and input data
# checks if all required infra is accessible and operational
# precisely, it checks: minio and spark connectivity, input data presence 


# minio infra tests


# check if minio endpoint is reachable and responsive
def test_minio_endpoint_reachable(minio_client):
    try:
        buckets = minio_client.list_buckets()
        assert buckets is not None, "MinIO returned None for bucket list"
    except S3Error as e:
        pytest.fail(f"Cannot connect to MinIO: {e}")
    except Exception as e:
        pytest.fail(f"MinIO connectivity error: {e}")

# check if all required buckets exist in minio
def test_required_buckets_exist(minio_client):
    config = load_config()
    storage_config = get_storage_config(config)
    buckets_config = storage_config.get('buckets', {})
    
    required_buckets = [
        buckets_config.get('input', 'input-data'),
        buckets_config.get('output_ok', 'motor-policy-ok'),
        buckets_config.get('output_ko', 'motor-policy-ko'),
        buckets_config.get('output_ok_consolidated', 'motor-policy-ok-consolidated'),
        buckets_config.get('pipeline_state', 'pipeline-state'),
        buckets_config.get('pipeline_logs', 'pipeline-logs')
    ]
    
    try:
        existing_buckets = {bucket.name for bucket in minio_client.list_buckets()}
        missing_buckets = set(required_buckets) - existing_buckets
        
        assert not missing_buckets, \
            f"Missing required MinIO buckets: {sorted(missing_buckets)}. " \
            f"Existing buckets: {sorted(existing_buckets)}"
    
    except S3Error as e:
        pytest.fail(f"Error listing MinIO buckets: {e}")


# spark infra tests

# check if spark master url is valid and spark session can be created
def test_spark_connectivity(spark):
    config = load_config()
    spark_master = get_spark_master_url(config)
    
    # check url format
    assert spark_master, "Spark master URL is empty or invalid"
    assert spark_master.startswith("spark://"), \
        f"Spark master URL must use spark:// protocol, got: {spark_master}"
    
    # check if spark session is created successfully
    assert spark is not None, "Spark session could not be created"


# manifest tests (incremental mode)

# check if manifest bucket exists for state tracking
def test_manifest_bucket_exists(minio_client):
    config = load_config()
    storage_config = get_storage_config(config)
    manifest_bucket = storage_config.get('buckets', {}).get('pipeline_state', 'pipeline-state')
    
    try:
        existing_buckets = {bucket.name for bucket in minio_client.list_buckets()}
        assert manifest_bucket in existing_buckets, \
            f"Manifest bucket '{manifest_bucket}' does not exist. Required for incremental processing."
    except S3Error as e:
        pytest.fail(f"Error checking manifest bucket: {e}")


# batch discovery tests

# check if batch folders exist in input bucket
def test_batch_folders_exist(minio_client):
    config = load_config()
    storage_config = get_storage_config(config)
    input_bucket = storage_config.get('buckets', {}).get('input', 'input-data')
    
    try:
        # list objects in input bucket
        objects = list(minio_client.list_objects(input_bucket, prefix='batch-', recursive=False))
        
        assert len(objects) > 0, \
            f"No batch folders found in '{input_bucket}'."
        
        # check if objects are folders (end with /)
        batch_folders = [obj.object_name for obj in objects if obj.object_name.endswith('/')]
        assert len(batch_folders) > 0, \
            f"Found objects but no batch folders in '{input_bucket}'. Objects: {[obj.object_name for obj in objects]}"
    
    except S3Error as e:
        pytest.fail(f"Error listing batch folders: {e}")


# input data tests

# check if input files exist in batch folders and are valid json
def test_input_data_exists_and_valid(minio_client):
    config = load_config()
    storage_config = get_storage_config(config)
    input_bucket = storage_config.get('buckets', {}).get('input', 'input-data')
    
    try:
        # list all batch folders
        batch_folders = [
            obj.object_name for obj in minio_client.list_objects(input_bucket, prefix='batch-', recursive=False)
            if obj.object_name.endswith('/')
        ]
        
        assert len(batch_folders) > 0, \
            f"No batch folders found in '{input_bucket}'."
        
        total_files_found = 0
        
        for batch_folder in batch_folders:
            # list files within each batch folder
            batch_objects = list(minio_client.list_objects(input_bucket, prefix=batch_folder, recursive=True))
            
            # filter for actual files (not folders)
            batch_files = [obj for obj in batch_objects if not obj.object_name.endswith('/')]
            
            # check if batch has files
            if len(batch_files) == 0:
                pytest.fail(
                    f"Batch folder '{batch_folder}' is empty. "
                    f"Expected input files like 'input.jsonl'."
                )
            
            total_files_found += len(batch_files)
            
            # check if files are not empty and valid JSON
            for obj in batch_files:
                # skip non-json files
                if not (obj.object_name.endswith('.json') or obj.object_name.endswith('.jsonl')):
                    continue
                
                # check file size
                if obj.size == 0:
                    pytest.fail(
                        f"File '{obj.object_name}' in batch '{batch_folder}' is empty. "
                        f"Data generation may be incomplete."
                    )
                
                # validate json format
                try:
                    response = minio_client.get_object(input_bucket, obj.object_name)
                    first_chunk = response.read(1024).decode('utf-8')
                    response.close()
                    response.release_conn()
                    
                    # try to parse 1st line as json
                    first_line = first_chunk.split('\n')[0].strip()
                    if first_line:
                        json.loads(first_line)
                
                except json.JSONDecodeError:
                    pytest.fail(
                        f"File '{obj.object_name}' is not valid JSON. "
                    )
                
                except Exception as e:
                    pytest.fail(f"Error reading file '{obj.object_name}': {e}")
        
        assert total_files_found > 0, \
            f"No input files found in any batch folders."
    
    except S3Error as e:
        pytest.fail(f"Error accessing input data: {e}")