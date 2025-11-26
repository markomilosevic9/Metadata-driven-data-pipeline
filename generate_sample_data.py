import json
import random
import string
from io import BytesIO
from typing import Dict, Tuple
from minio import Minio
from minio.error import S3Error
from utils.json_logger import (
    init_log_structure,
    read_log_from_minio,
    start_stage,
    end_stage,
    upload_log_to_minio
)
from pipeline.config_loader import load_config, get_storage_config, get_pipeline_config, get_logging_config

# generates input data sample (100k), uploads to minio, creates log structure

TOTAL_RECORDS_DEFAULT = 100_000

# generate plate_number in format XYZ-123 
def generate_plate_number() -> str:
    letters = ''.join(random.choices(string.ascii_uppercase, k=3))
    digits = ''.join(random.choices(string.digits, k=3))
    return f"{letters}-{digits}"

# generate unique 5 digit policy number
def generate_policy_number(index: int) -> str:
    return str(10000 + index).zfill(5)

# generate driver age, ranged 17-80
def generate_driver_age() -> int:
    return random.randint(17, 80)

# generate jsonl, return missing age count and empty plate count
def generate_jsonl_and_stats(total_records: int = TOTAL_RECORDS_DEFAULT) -> Tuple[str, int, int]:
    indices = list(range(total_records))
    random.shuffle(indices)

    # cca 95% of records have driver_age, 5% are missing
    num_with_age = int(total_records * 0.95)
    indices_with_age = set(indices[:num_with_age])

    lines = []
    empty_plate_count = 0
    missing_age_count = 0

    for i in range(total_records):
        record: Dict = {}
        record["policy_number"] = generate_policy_number(i)

        if i in indices_with_age:
            record["driver_age"] = generate_driver_age()
        else:
            missing_age_count += 1

        if random.random() < 0.05:
            record["plate_number"] = ""
            empty_plate_count += 1
        else:
            record["plate_number"] = generate_plate_number()

        lines.append(json.dumps(record))

    content = "\n".join(lines) + "\n"
    return content, missing_age_count, empty_plate_count

# upload jsonl to minio
def upload_jsonl_to_minio(
    jsonl_content: str,
    bucket: str,
    object_name: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    secure: bool = False,
) -> None:
    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

    data_bytes = jsonl_content.encode("utf-8")
    data_len = len(data_bytes)
    data_stream = BytesIO(data_bytes)

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=data_stream,
        length=data_len,
        content_type="application/json",
    )

# generates data, handles logging, actually 1st script in the pipeline so it creates the log structure, gets run_id for logging
def main(run_id: str = None):
    # load config
    config = load_config()
    data_gen_config = get_pipeline_config(config).get('data_generation', {})
    storage_config = get_storage_config(config)
    logging_config = get_logging_config(config)
    
    # get settings
    total_records = data_gen_config.get('total_records', TOTAL_RECORDS_DEFAULT)
    input_bucket = data_gen_config.get('input_bucket', 'input-data')
    input_object = data_gen_config.get('input_object', 'input.jsonl')
    endpoint = storage_config['endpoint'].replace('http://', '').replace('https://', '')
    
    # init logging if run_id provided
    log_structure = None
    stage = None
    
    if run_id:
        # try to read existing log / since does not exist - create new
        try:
            log_structure = read_log_from_minio(run_id, **logging_config)
        except:
            log_structure = init_log_structure(run_id)
        
        stage = start_stage(log_structure, "data_generation")
    
    try:
        # deterministic output, thus fixed seed
        random.seed(data_gen_config.get('seed', 42))
        
        # generate data
        jsonl_content, missing_age_count, empty_plate_count = generate_jsonl_and_stats(total_records)
        
        # upload to minio
        upload_jsonl_to_minio(
            jsonl_content=jsonl_content,
            bucket=input_bucket,
            object_name=input_object,
            endpoint=endpoint,
            access_key=storage_config['access_key'],
            secret_key=storage_config['secret_key'],
            secure=storage_config.get('secure', False)
        )
        
        # log success
        if stage:
            end_stage(
                stage,
                status="success",
                records_generated=total_records,
                output_bucket=input_bucket,
                output_object=input_object
            )
            upload_log_to_minio(log_structure, **logging_config)

    except Exception as e:
        # log failure
        if stage:
            end_stage(stage, status="failed", error_message=str(e))
            upload_log_to_minio(log_structure, **logging_config)
        raise


if __name__ == "__main__":
    main()