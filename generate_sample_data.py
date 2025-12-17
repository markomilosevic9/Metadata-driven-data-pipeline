import json
import random
import string
from io import BytesIO
from typing import Dict, Tuple, List
from minio import Minio
from utils.json_logger import (
    init_log_structure,
    start_stage,
    end_stage,
    upload_log_to_minio
)
from pipeline.config_loader import load_config, get_storage_config, get_pipeline_config, get_logging_config


# generates input data sample (100k records), uploads to minio, creates log structure
# uses config_loader for all configuration

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

# generate input data as jsonl, return missing age count and empty plate count
def generate_jsonl_and_stats(total_records: int, start_index: int = 0) -> Tuple[str, int, int, List[Dict]]:
    indices = list(range(total_records))
    random.shuffle(indices)

    # cca 95% of records have driver_age, 5% are missing
    num_with_age = int(total_records * 0.95)
    indices_with_age = set(indices[:num_with_age])

    lines = []
    empty_plate_count = 0
    missing_age_count = 0
    valid_records = []  # Track valid records for potential reuse

    for i in range(total_records):
        record: Dict = {}
        record["policy_number"] = generate_policy_number(start_index + i)

        has_age = i in indices_with_age
        has_valid_plate = random.random() >= 0.05

        if has_age:
            record["driver_age"] = generate_driver_age()
        else:
            missing_age_count += 1

        if has_valid_plate:
            record["plate_number"] = generate_plate_number()
        else:
            record["plate_number"] = ""
            empty_plate_count += 1

        lines.append(json.dumps(record))
        
        # for easier mock-up data sample generation, track valid records (age >= 18, non-empty plate)
        if has_age and has_valid_plate and record.get("driver_age", 0) >= 18:
            valid_records.append(record.copy())

    content = "\n".join(lines) + "\n"
    return content, missing_age_count, empty_plate_count, valid_records

# generate batch with reused valid records from batch 1
def generate_batch_with_reuse(batch_size: int, start_index: int, reuse_records: List[Dict], reuse_percentage: float) -> Tuple[str, int, int]:
    reuse_count = int(batch_size * reuse_percentage)
    new_count = batch_size - reuse_count
    
    indices = list(range(new_count))
    random.shuffle(indices)
    
    # cca 95% of new records have driver_age, 5% are missing
    num_with_age = int(new_count * 0.95)
    indices_with_age = set(indices[:num_with_age])
    
    lines = []
    empty_plate_count = 0
    missing_age_count = 0
    
    # add reused valid records from batch 1
    selected_reuse = random.sample(reuse_records, min(reuse_count, len(reuse_records)))
    for record in selected_reuse:
        lines.append(json.dumps(record))
    
    # generate new records with same distribution as original
    for i in range(new_count):
        record: Dict = {}
        record["policy_number"] = generate_policy_number(start_index + i)
        
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
def upload_jsonl_to_minio(jsonl_content: str, 
                          bucket: str, 
                          object_name: str, 
                          endpoint: str, 
                          access_key: str, 
                          secret_key: str, 
                          secure: bool) -> None:

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

# generates data, handles logging
# actually 1st script in the pipeline so it creates the log structure, gets run_id for logging
def main(run_id: str = None):
    # load all configs
    config = load_config()
    data_gen_config = get_pipeline_config(config).get('data_generation', {})
    storage_config = get_storage_config(config)
    logging_config = get_logging_config(config)
    
    # get settings from config
    total_records = data_gen_config.get('total_records', 100000)
    input_bucket = data_gen_config.get('input_bucket', 'input-data')
    seed = data_gen_config.get('seed', 42)
    
    # batch settings
    num_batches = data_gen_config.get('num_batches', 3)
    batch_dates = data_gen_config.get('batch_dates', ['2025-12-01', '2025-12-02', '2025-12-03'])
    batch3_reuse_percentage = data_gen_config.get('batch3_reuse_percentage', 0.2)  # 20% reuse in batch 3
    
    # extract endpoint 
    endpoint = storage_config['endpoint'].replace('http://', '').replace('https://', '')
    
    # init logging if run_id provided
    log_structure = None
    stage = None
    
    if run_id:
        # try to read existing log / since does not exist - create new
        log_structure = read_log_from_minio(run_id, **logging_config)
        if log_structure is None:
            log_structure = init_log_structure(run_id)
        
        stage = start_stage(log_structure, "data_generation")
    
    try:
        # seed for reproducible output
        random.seed(seed)
        
        records_per_batch = total_records // num_batches
        batch_details = []
        batch1_valid_records = []
        
        for batch_idx, batch_date in enumerate(batch_dates[:num_batches]):
            batch_folder = f"batch-{batch_date}"
            object_name = f"{batch_folder}/input.jsonl"
            start_index = batch_idx * records_per_batch
            
            if batch_idx < 2:
                # batches 1 and 2 generate normally with original logic
                jsonl_content, missing_age_count, empty_plate_count, valid_records = generate_jsonl_and_stats(
                    records_per_batch, 
                    start_index
                )
                
                # store batch 1 valid records for reuse in batch 3
                if batch_idx == 0:
                    batch1_valid_records = valid_records
                
                batch_details.append({
                    "batch_date": batch_date,
                    "batch_folder": batch_folder,
                    "records_generated": records_per_batch,
                    "missing_age_count": missing_age_count,
                    "empty_plate_count": empty_plate_count,
                    "reused_count": 0,
                    "output_path": f"{input_bucket}/{object_name}"
                })
            else:
                # batch 3: 80% new records, 20% reused valid records from batch 1
                jsonl_content, missing_age_count, empty_plate_count = generate_batch_with_reuse(
                    records_per_batch,
                    start_index,
                    batch1_valid_records,
                    batch3_reuse_percentage
                )
                
                reused_count = int(records_per_batch * batch3_reuse_percentage)
                
                batch_details.append({
                    "batch_date": batch_date,
                    "batch_folder": batch_folder,
                    "records_generated": records_per_batch,
                    "missing_age_count": missing_age_count,
                    "empty_plate_count": empty_plate_count,
                    "reused_count": reused_count,
                    "reused_from": "batch-2025-12-01",
                    "output_path": f"{input_bucket}/{object_name}"
                })
            
            # Upload to minio
            upload_jsonl_to_minio(
                jsonl_content=jsonl_content,
                bucket=input_bucket,
                object_name=object_name,
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
                total_batches=num_batches,
                total_records_generated=total_records,
                records_per_batch=records_per_batch,
                batch3_reuse_percentage=batch3_reuse_percentage,
                batches=batch_details,
                output_bucket=input_bucket
            )
            upload_log_to_minio(log_structure, logging_config)

    except Exception as e:
        # log failure
        if stage:
            end_stage(stage, status="failed", error_message=str(e))
            upload_log_to_minio(log_structure, logging_config)
        raise


if __name__ == "__main__":

    main()
