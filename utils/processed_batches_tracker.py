import json
from datetime import datetime
from io import BytesIO
from typing import Dict, Optional
from minio import Minio
from minio.error import S3Error


# pipeline state tracking module

# tracks which batches have been processed, enabling incremental processing
# handles pipeline state tracking via manifest json file stored in minio bucket (pipeline-state)
# manifest json file serves as source of truth when it comes to tracking which batches already have been processed


# get current timestamp (in ISO format)
def get_timestamp() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

# create new manifest structure for pipeline state tracking
# returns dict with initialized structure
def create_manifest(pipeline_name: str) -> Dict:
    return {
        "pipeline_name": pipeline_name,
        "last_processed_batch": None,
        "last_success_run_id": None,
        "last_success_timestamp": None,
        "total_batches_processed": 0,
        "created_at": get_timestamp()
    }

# read existing manifest from minio
def read_manifest(manifest_config: dict) -> Optional[Dict]:
    endpoint = manifest_config['endpoint'].replace('http://', '').replace('https://', '')
    
    client = Minio(
        endpoint=endpoint,
        access_key=manifest_config['access_key'],
        secret_key=manifest_config['secret_key'],
        secure=manifest_config.get('secure', False)
    )
    
    try:
        response = client.get_object(
            manifest_config['bucket'], 
            manifest_config['object_name']
        )
        json_content = response.read().decode("utf-8")
        return json.loads(json_content)
    except S3Error as e:
        if e.code == "NoSuchKey":
            # if manifest does not exist yet
            return None
        else:
            raise
    finally:
        if 'response' in locals():
            response.close()
            response.release_conn()

# after successful processing, update manifest
def update_manifest(manifest: Dict, batch_id: str, run_id: str) -> Dict:
    manifest["last_processed_batch"] = batch_id
    manifest["last_success_run_id"] = run_id
    manifest["last_success_timestamp"] = get_timestamp()
    manifest["total_batches_processed"] = manifest.get("total_batches_processed", 0) + 1
    
    return manifest

# upload manifest to minio bucket
def upload_manifest(manifest: Dict, manifest_config: dict) -> str:
    endpoint = manifest_config['endpoint'].replace('http://', '').replace('https://', '')
    
    client = Minio(
        endpoint=endpoint,
        access_key=manifest_config['access_key'],
        secret_key=manifest_config['secret_key'],
        secure=manifest_config.get('secure', False)
    )
    
    # prettify json
    json_content = json.dumps(manifest, indent=2, ensure_ascii=False)
    json_bytes = json_content.encode("utf-8")
    json_stream = BytesIO(json_bytes)
    
    try:
        client.put_object(
            bucket_name=manifest_config['bucket'],
            object_name=manifest_config['object_name'],
            data=json_stream,
            length=len(json_bytes),
            content_type="application/json"
        )
        return manifest_config['object_name']
    except S3Error as e:
        print(f"Failed to upload manifest to MinIO: {e}")
        print(f"Manifest content:\n{json_content}")
        raise

# build manifest config from pipeline config
def get_manifest_config(config: dict) -> dict:
    storage_config = config.get('storage', {})
    manifest_settings = config.get('pipeline', {}).get('manifest', {})
    
    endpoint = storage_config.get('endpoint', 'http://minio:9000')
    
    return {
        'bucket': manifest_settings.get('bucket', 'pipeline-state'),
        'object_name': manifest_settings.get('object_name', 'motor-policy-manifest.json'),
        'endpoint': endpoint,
        'access_key': storage_config.get('access_key', ''),
        'secret_key': storage_config.get('secret_key', ''),
        'secure': storage_config.get('secure', False)
    }