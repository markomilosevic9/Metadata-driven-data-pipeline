import json
import pytest
from pathlib import Path
from minio.error import S3Error

# 3rd pre-pipeline test, checks if input data sample is present, accessible and valid

# get input path from metadata
def get_input_path_from_metadata():
    metadata_path = Path("config/metadata_motor.json")
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    sources = metadata["dataflows"][0]["sources"]
    return sources[0]["path"]

# parse path (into bucket and prefix), return as tuple 
def _parse_s3_path(s3_path):
    path_parts = s3_path.replace("s3a://", "").split("/", 1)
    bucket = path_parts[0]
    prefix_pattern = path_parts[1] if len(path_parts) > 1 else ""
    
    # get path prefix
    if "*" in prefix_pattern:
        prefix = prefix_pattern.split("*")[0]
    else:
        prefix = prefix_pattern
    
    return bucket, prefix

# helper to list objects once, returns list of objects present in minio bucket
def _list_input_objects(minio_client, bucket, prefix):
    try:
        return list(minio_client.list_objects(bucket, prefix=prefix, recursive=False))
    except S3Error as e:
        if e.code == "NoSuchBucket":
            pytest.fail(f"Input bucket '{bucket}' does not exist")
        else:
            pytest.fail(f"Error accessing input bucket '{bucket}': {e}")


# more comprehensive check: input file exists, isn't empty (size isn't 0), it is valid json (parses few lines)
def test_input_data_comprehensive(minio_client):
    input_path = get_input_path_from_metadata()
    bucket, prefix = _parse_s3_path(input_path)
    
    # get objects
    objects = _list_input_objects(minio_client, bucket, prefix)
    
    # 1st check, if files exist
    assert len(objects) > 0, \
        f"No input files found '{input_path}'. " \
        f"Searched bucket '{bucket}' with prefix '{prefix}'"
    
    # 2nd check, file size (isn't empty)
    empty_files = [obj.object_name for obj in objects if obj.size == 0]
    assert not empty_files, f"Found empty input files: {empty_files}"
    
    # 3rd check, file is valid json
    for obj in objects:
        # check only json and jsonl
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
            pytest.fail(f"File '{obj.object_name}' is not valid json")
        
        except Exception as e:
            pytest.fail(f"Error reading file '{obj.object_name}': {e}")