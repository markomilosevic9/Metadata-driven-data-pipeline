import pytest
from minio.error import S3Error

# 2nd pre-pipeline test, checks for connectivity (minio is reachable, bucket exists)

# check if minio endpoint is reachable 
def test_minio_endpoint_reachable(minio_client):
    try:
        # just verify connectivity
        buckets = minio_client.list_buckets()
        assert buckets is not None, "Minio returned none for bucket list"
    except S3Error as e:
        pytest.fail(f"Cannot connect to MinIO: {e}")
    except Exception as e:
        pytest.fail(f"Minio connectivity error: {e}")


# check if all required buckets exists in minio
def test_required_buckets_exist(minio_client):
    required_buckets = [
        "input-data",
        "motor-policy-ok",
        "motor-policy-ko",
        "pipeline-logs"
    ]
    
    try:
        existing_buckets = {bucket.name for bucket in minio_client.list_buckets()}
        
        missing_buckets = set(required_buckets) - existing_buckets
        
        assert not missing_buckets, \
            f"Missing required Minio buckets: {sorted(missing_buckets)}. " \
            f"Existing buckets: {sorted(existing_buckets)}"
    
    except S3Error as e:
        pytest.fail(f"Error listing MinIO buckets: {e}")