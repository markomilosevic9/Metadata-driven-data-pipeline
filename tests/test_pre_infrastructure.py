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


# input data tests

# extract input path from metadata
def _get_input_path_from_metadata():
    metadata_path = Path("config/metadata_motor.json")
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    sources = metadata["dataflows"][0]["sources"]
    return sources[0]["path"]

# parse minio/s3a path into bucket and prefix components
def _parse_s3_path(s3_path: str) -> tuple:
    path_parts = s3_path.replace("s3a://", "").split("/", 1)
    bucket = path_parts[0]
    prefix_pattern = path_parts[1] if len(path_parts) > 1 else ""
    
    # get prefix
    if "*" in prefix_pattern:
        prefix = prefix_pattern.split("*")[0]
    else:
        prefix = prefix_pattern
    
    return bucket, prefix

# list objects within minio bucket
def _list_input_objects(minio_client, bucket: str, prefix: str) -> list:
    try:
        return list(minio_client.list_objects(bucket, prefix=prefix, recursive=False))
    except S3Error as e:
        if e.code == "NoSuchBucket":
            pytest.fail(f"Input bucket '{bucket}' does not exist")
        else:
            pytest.fail(f"Error accessing input bucket '{bucket}': {e}")

# check if input file exists at expected location, if it is empy and if it is valid json
def test_input_data_exists_and_valid(minio_client):
    input_path = _get_input_path_from_metadata()
    bucket, prefix = _parse_s3_path(input_path)
    
    # get objects matching input path
    objects = _list_input_objects(minio_client, bucket, prefix)
    
    # check if files exist
    assert len(objects) > 0, \
        f"No input files found at '{input_path}'. " \
        f"Searched bucket '{bucket}' with prefix '{prefix}'. " \
        f"Data generation step may have failed."
    
    # check if files are not empty
    empty_files = [obj.object_name for obj in objects if obj.size == 0]
    assert not empty_files, \
        f"Found empty input files: {empty_files}. " \
        f"Data generation may be incomplete."
    
    # check if files are valid json 
    for obj in objects:
        # only check for json/jsonl files
        if not (obj.object_name.endswith('.json') or obj.object_name.endswith('.jsonl')):
            continue
        
        try:
            # get 1KB to check json validity
            response = minio_client.get_object(bucket, obj.object_name)
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
                f"Data generation may have produced corrupted output."
            )
        
        except Exception as e:
            pytest.fail(f"Error reading file '{obj.object_name}': {e}")