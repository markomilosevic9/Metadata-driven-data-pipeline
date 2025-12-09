import json
from datetime import datetime
from io import BytesIO
from typing import Dict, Any, Optional
from minio import Minio
from minio.error import S3Error


# simple logging system that writes structured json logs to minio
# tracks pipeline execution stages/substages with timestamps


# get current timestamp (iso)
def get_timestamp() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

# get duration expressed in seconds between 2 iso timestamps
def calculate_duration(start_time: str, end_time: str) -> float:
    start = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    end = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    return round((end - start).total_seconds(), 2)

# init base log structure for pipelien run, get run_id, returns dict
def init_log_structure(run_id: str) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "started_at": get_timestamp(),
        "completed_at": None,
        "duration_seconds": None,
        "status": "running",
        "stages": []
    }

# create and add new stage to log structure, gets main log dict + stage name, returns the created stage dict
def start_stage(log_structure: Dict[str, Any], stage_name: str) -> Dict[str, Any]:
    stage = {
        "stage_name": stage_name,
        "started_at": get_timestamp(),
        "completed_at": None,
        "duration_seconds": None,
        "status": "running",
        "sub_stages": []
    }
    log_structure["stages"].append(stage)
    return stage

# mark stage as finished, gets dict, status, details
def end_stage(stage: Dict[str, Any], status: str = "success", **details):
    stage["completed_at"] = get_timestamp()
    stage["duration_seconds"] = calculate_duration(
        stage["started_at"], 
        stage["completed_at"]
    )
    stage["status"] = status
    
    # any additional details
    for key, value in details.items():
        stage[key] = value

# finalize pipeline log with completion time and overall status, gets main log dict, status
def finalize_log(log_structure: Dict[str, Any], status: str = "success"):
    log_structure["completed_at"] = get_timestamp()
    log_structure["duration_seconds"] = calculate_duration(
        log_structure["started_at"],
        log_structure["completed_at"]
    )
    log_structure["status"] = status

# upload the log structure as json to minio
def upload_log_to_minio(log_structure: Dict[str, Any], logging_config: dict) -> str:
    run_id = log_structure["run_id"]
    object_key = f"{run_id}.json"
    
    client = Minio(
        endpoint=logging_config['endpoint'],
        access_key=logging_config['access_key'],
        secret_key=logging_config['secret_key'],
        secure=logging_config['secure']
    )
    
    # prettify JSON
    json_content = json.dumps(log_structure, indent=2, ensure_ascii=False)
    json_bytes = json_content.encode("utf-8")
    json_stream = BytesIO(json_bytes)
    
    try:
        client.put_object(
            bucket_name=logging_config['bucket'],
            object_name=object_key,
            data=json_stream,
            length=len(json_bytes),
            content_type="application/json"
        )
        return object_key
    except S3Error as e:
        # if upload fails, can only print to console as fallback
        print(f"Failed to upload log to MinIO: {e}")
        print(f"Log content:\n{json_content}")
        raise

# read an existing log from Minio, for updating across task/stages/substages, returns log dict
def read_log_from_minio(run_id: str, logging_config: dict) -> Optional[Dict[str, Any]]:
    object_key = f"{run_id}.json"
    
    client = Minio(
        endpoint=logging_config['endpoint'],
        access_key=logging_config['access_key'],
        secret_key=logging_config['secret_key'],
        secure=logging_config['secure']
    )
    
    try:
        response = client.get_object(logging_config['bucket'], object_key)
        json_content = response.read().decode("utf-8")
        return json.loads(json_content)
    except S3Error:
        # object does not exist yet
        return None
    finally:
        if 'response' in locals():
            response.close()
            response.release_conn()

# create a substage dict for use in core pipeline script
def create_sub_stage(name: str, 
                     stage_type: str, 
                     started_at: str, 
                     completed_at: str, 
                     status: str = "success", 
                     **details) -> Dict[str, Any]:
    sub_stage = {
        "name": name,
        "type": stage_type,
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_seconds": calculate_duration(started_at, completed_at),
        "status": status
    }
    sub_stage.update(details)
    return sub_stage