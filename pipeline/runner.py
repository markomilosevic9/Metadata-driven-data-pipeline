import os
import json
import re
from datetime import datetime
from typing import List, Tuple
from pyspark.sql import SparkSession
from pipeline.transformer import execute_add_fields
from pipeline.sink import write_df
from pipeline.validator import execute_validation
from pipeline.schema_enforcer import build_spark_schema, SchemaEnforcementError
from pipeline.consolidator import consolidate_data
from pipeline.config_loader import (
    load_config, 
    get_spark_master_url, 
    get_metadata_path, 
    get_storage_config,
    get_logging_config,
    get_spark_config
)
from utils.json_logger import (
    get_timestamp,
    create_sub_stage,
    start_stage,
    end_stage,
    upload_log_to_minio,
    read_log_from_minio
)
from utils.processed_batches_tracker import (
    create_manifest,
    read_manifest,
    update_manifest,
    upload_manifest,
    get_manifest_config
)

# core pipeline script
# handles the entire data processing pipeline
# logging goes to structured json in designated minio bucket

# find all available batches in the input minio bucket
# returns sorted list of batches (batch dates)
def discover_batches(spark: SparkSession, input_bucket: str, batch_prefix: str, date_format: str) -> List[str]:
    from minio import Minio
    
    config = load_config()
    storage_config = get_storage_config(config)
    
    endpoint = storage_config['endpoint'].replace('http://', '').replace('https://', '')
    
    client = Minio(
        endpoint=endpoint,
        access_key=storage_config['access_key'],
        secret_key=storage_config['secret_key'],
        secure=storage_config.get('secure', False)
    )
    
    # list all objects in bucket with batch prefix
    objects = client.list_objects(input_bucket, prefix=batch_prefix, recursive=False)
    
    batch_dates = []
    pattern = f"^{batch_prefix}(.+)/$"
    
    for obj in objects:
        match = re.match(pattern, obj.object_name)
        if match:
            date_str = match.group(1)
            # check date format
            try:
                datetime.strptime(date_str, date_format)
                batch_dates.append(date_str)
            except ValueError:
                # skip folders that do not match expected date format
                continue
    
    return sorted(batch_dates)

# filter batches to find only new/unprocessed batches
# returns list of batches (batch dates) that are not processed yet (batches > last_processed)
# rejects batches <= last_processed_batch to prevent duplicate processing
def filter_new_batches(all_batches: List[str], last_processed: str) -> List[str]:
    if last_processed is None:
        # if no batches processed yet, return all
        return all_batches
    
    # return only batches after last_processed, strictly greater than (reject duplicates)
    new_batches = [batch for batch in all_batches if batch > last_processed]
    
    # log rejected batches for visibility
    rejected_batches = [batch for batch in all_batches if batch <= last_processed]
    if rejected_batches:
        print(f"Rejected {len(rejected_batches)} already-processed batches: {rejected_batches}")
    
    return new_batches


# main pipeline execution function
def run_pipeline(metadata_path: str = None, config_path: str = None):
    # get run_id from environment (set by airflow task)
    run_id = os.getenv('RUN_ID')
    if not run_id:
        raise ValueError("RUN_ID environment variable not set")
    
    # load configs
    config = load_config(config_path)
    logging_config = get_logging_config(config)
    spark_master = get_spark_master_url(config)
    storage_config = get_storage_config(config)
    spark_config = get_spark_config(config)
    pipeline_config = config.get('pipeline', {})
    
    # get existing log from minio (must exist, because it is created by generate_sample_data earlier)
    log_structure = read_log_from_minio(run_id, logging_config)
    
    if log_structure is None:
        raise ValueError(
            f"Log structure not found for run_id '{run_id}'. "
            "The 'generate_sample_data' task may have failed or not run yet."
        )
    
    # start spark_pipeline stage
    stage = start_stage(log_structure, "spark_pipeline")
    
    try:
        # make spark session 
        spark = SparkSession.builder \
            .master(spark_master) \
            .appName(spark_config['app_name']) \
            .getOrCreate()

         # config for minio access 
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", storage_config['access_key'])
        hadoop_conf.set("fs.s3a.secret.key", storage_config['secret_key'])
        hadoop_conf.set("fs.s3a.endpoint", storage_config['endpoint'])
        hadoop_conf.set("fs.s3a.path.style.access", str(storage_config.get('path_style_access', True)).lower())
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", str(storage_config.get('secure', False)).lower())

        # get metadata path from config and load metadata
        if metadata_path is None:
            metadata_path = get_metadata_path(config)
        
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
        
        # get manifest config and read/create manifest for tracking batches
        manifest_config = get_manifest_config(config)
        manifest = read_manifest(manifest_config)
        
        if manifest is None:
            # if it is first run, create new manifest for tracking batches
            manifest = create_manifest("motor-policy")
            upload_manifest(manifest, manifest_config)
        
        # discover and filter batches
        batch_discovery_config = pipeline_config.get('batch_discovery', {})
        input_bucket = batch_discovery_config.get('input_bucket', 'input-data')
        batch_prefix = batch_discovery_config.get('batch_prefix', 'batch-')
        date_format = batch_discovery_config.get('date_format', '%Y-%m-%d')
        
        all_batches = discover_batches(spark, input_bucket, batch_prefix, date_format)
        last_processed = manifest.get('last_processed_batch')
        new_batches = filter_new_batches(all_batches, last_processed)
        
        if not new_batches:
            # if no new batches to process
            end_stage(
                stage, 
                status="success", 
                message="No new batches to process",
                all_batches=all_batches,
                last_processed_batch=last_processed
            )
            upload_log_to_minio(log_structure, logging_config)
            spark.stop()
            return

        # process each new batch
        for batch_date in new_batches:
            batch_start = get_timestamp()
            
            # process each dataflow defined within metadata file
            for flow in metadata["dataflows"]:
                # load sources with batch date substitution
                for source in flow["sources"]:
                    source_start = get_timestamp()
                    source_name = source["name"]
                    # default is required if not specified
                    required = source.get("required", True)  
                    
                    # substitute {date} placeholder in path
                    source_path = source["path"].replace("{date}", batch_date)
                    
                    try:
                        # check if schema enforcement is enabled
                        schema_def = source.get("schema")
                        enforcement_config = source.get("schema_enforcement", {})
                        enforcement_enabled = enforcement_config.get("enabled", False)
                        
                        if schema_def and enforcement_enabled:
                            # schema enforcement enabled, build and apply schema
                            try:
                                # build spark structtype from metadata schema
                                expected_schema = build_spark_schema(schema_def)
                                
                                # read with explicit schema enforcement
                                df = spark.read \
                                          .schema(expected_schema) \
                                          .format(source.get("format", "json")) \
                                          .options(**source.get("options", {})) \
                                          .load(source_path)
                                
                                record_count = df.count()
                                source_end = get_timestamp()
                                
                                # log source loading in json (with schema enforcement details)
                                stage["sub_stages"].append(create_sub_stage(
                                    name=f"source_load_{source_name}_batch_{batch_date}",
                                    stage_type="source",
                                    started_at=source_start,
                                    completed_at=source_end,
                                    status="success",
                                    records_loaded=record_count,
                                    source_path=source_path,
                                    batch_date=batch_date,
                                    schema_enforced=True,
                                    enforced_fields=[f["name"] for f in schema_def["fields"]],
                                    source_required=required
                                ))
                            
                            except Exception as schema_error:
                                # schema enforcement failed
                                source_end = get_timestamp()
                                
                                if required:
                                    # required source failed schema enforcement, log and fail pipeline
                                    stage["sub_stages"].append(create_sub_stage(
                                        name=f"source_load_{source_name}_batch_{batch_date}",
                                        stage_type="source",
                                        started_at=source_start,
                                        completed_at=source_end,
                                        status="failed",
                                        records_loaded=0,
                                        source_path=source_path,
                                        batch_date=batch_date,
                                        error_type="schema_enforcement_failure",
                                        error_message=str(schema_error),
                                        schema_enforced=True,
                                        source_required=True
                                    ))
                                    raise  # re-raise to fail pipeline
                                else:
                                    # optional source failed schema enforcement, log skip and continue
                                    stage["sub_stages"].append(create_sub_stage(
                                        name=f"source_load_{source_name}_batch_{batch_date}",
                                        stage_type="source",
                                        started_at=source_start,
                                        completed_at=source_end,
                                        status="skipped",
                                        records_loaded=0,
                                        source_path=source_path,
                                        batch_date=batch_date,
                                        skip_reason=f"Schema enforcement failed: {str(schema_error)}",
                                        schema_enforced=True,
                                        source_required=False
                                    ))
                                    continue  # skip this source
                        
                        else:
                            # if no schema enforcement, use spark inference 
                            df = spark.read \
                                      .format(source.get("format", "json")) \
                                      .option("mode", "PERMISSIVE") \
                                      .options(**source.get("options", {})) \
                                      .load(source_path)
                            
                            record_count = df.count()
                            source_end = get_timestamp()
                            
                            # log source loading in json without schema enforcement
                            stage["sub_stages"].append(create_sub_stage(
                                name=f"source_load_{source_name}_batch_{batch_date}",
                                stage_type="source",
                                started_at=source_start,
                                completed_at=source_end,
                                status="success",
                                records_loaded=record_count,
                                source_path=source_path,
                                batch_date=batch_date,
                                schema_enforced=False,
                                source_required=required
                            ))
                        
                        # create temp view for transformations
                        df.createOrReplaceTempView(source_name)
                    
                    except Exception as e:
                        # handle source loading error based on required flag
                        source_end = get_timestamp()
                        
                        if required:
                            # if required source failed, log and fail pipeline
                            stage["sub_stages"].append(create_sub_stage(
                                name=f"source_load_{source_name}_batch_{batch_date}",
                                stage_type="source",
                                started_at=source_start,
                                completed_at=source_end,
                                status="failed",
                                records_loaded=0,
                                source_path=source_path,
                                batch_date=batch_date,
                                error_message=str(e),
                                source_required=True
                            ))
                            raise  # re-raise to fail pipeline
                        
                        else:
                            # if optional source failed, log skip and continue
                            stage["sub_stages"].append(create_sub_stage(
                                name=f"source_load_{source_name}_batch_{batch_date}",
                                stage_type="source",
                                started_at=source_start,
                                completed_at=source_end,
                                status="skipped",
                                records_loaded=0,
                                source_path=source_path,
                                batch_date=batch_date,
                                skip_reason=str(e),
                                source_required=False
                            ))
                            # do not create view, skip this source entirely
                            continue

                # apply transformations
                for transform in flow["transformations"]:
                    transform_start = get_timestamp()
                    transform_type = transform["type"]
                    transform_name = transform["name"]
                    params = transform["params"]
                    input_view = params["input"]

                    # check if input view exists
                    if not spark.catalog._jcatalog.tableExists(input_view):
                        raise ValueError(f"Input view '{input_view}' does not exist")

                    if transform_type == "validate_fields":
                        # execute validation
                        df_ok, df_ko, ok_count, ko_count, total_count = execute_validation(
                            spark, input_view, params["validations"]
                        )

                        # derive output view names from transformation name (no magic strings)
                        ok_view_name = f"{transform_name}_ok"
                        ko_view_name = f"{transform_name}_ko"
                        
                        # make output views
                        df_ok.createOrReplaceTempView(ok_view_name)
                        df_ko.createOrReplaceTempView(ko_view_name)

                        transform_end = get_timestamp()
                        
                        # log validation results
                        stage["sub_stages"].append(create_sub_stage(
                            name=f"{transform_name}_batch_{batch_date}",
                            stage_type="transformation",
                            started_at=transform_start,
                            completed_at=transform_end,
                            status="success",
                            batch_date=batch_date,
                            transformation_type="validate_fields",
                            total_records=total_count,
                            ok_count=ok_count,
                            ko_count=ko_count,
                            ok_percentage=round((ok_count / total_count * 100), 2) if total_count > 0 else 0,
                            ko_percentage=round((ko_count / total_count * 100), 2) if total_count > 0 else 0,
                            ok_view_name=ok_view_name,
                            ko_view_name=ko_view_name
                        ))

                    elif transform_type == "add_fields":
                        # execute transformation(s), pass batch_date and run_id for metadata columns
                        df_result, records_processed = execute_add_fields(
                            spark, input_view, params["addFields"], 
                            batch_id=batch_date, 
                            run_id=run_id
                        )

                        # make output view
                        df_result.createOrReplaceTempView(transform_name)
                        
                        transform_end = get_timestamp()
                        
                        # log add_fields transformation
                        stage["sub_stages"].append(create_sub_stage(
                            name=f"{transform_name}_batch_{batch_date}",
                            stage_type="transformation",
                            started_at=transform_start,
                            completed_at=transform_end,
                            status="success",
                            batch_date=batch_date,
                            transformation_type="add_fields",
                            fields_added=[f["name"] for f in params["addFields"]],
                            records_processed=records_processed
                        ))

                # write outputs with batch date substitution
                for sink in flow["sinks"]:
                    sink_start = get_timestamp()
                    sink_input = sink["input"]
                    sink_name = sink["name"]
                    sink_path_template = sink["path"] if "path" in sink else sink["paths"][0]
                    
                    # substitute {date} placeholder in sink path
                    sink_path = sink_path_template.replace("{date}", batch_date)
                    
                    try:
                        # check if view exists
                        if not spark.catalog._jcatalog.tableExists(sink_input):
                            raise ValueError(f"Sink input '{sink_input}' does not exist")

                        df_to_write = spark.table(sink_input)
                        record_count = df_to_write.count()

                        write_df(
                            df_to_write,
                            sink_path,
                            sink.get("format", "json"),
                            sink.get("saveMode", "overwrite"),
                        )
                        
                        sink_end = get_timestamp()
                        
                        # log sink write
                        stage["sub_stages"].append(create_sub_stage(
                            name=f"{sink_name}_batch_{batch_date}",
                            stage_type="sink",
                            started_at=sink_start,
                            completed_at=sink_end,
                            status="success",
                            batch_date=batch_date,
                            records_written=record_count,
                            sink_path=sink_path,
                            format=sink.get("format", "json")
                        ))
                    
                    except Exception as e:
                        # handle sink write error if any, log and fail pipeline
                        sink_end = get_timestamp()
                        stage["sub_stages"].append(create_sub_stage(
                            name=f"{sink_name}_batch_{batch_date}",
                            stage_type="sink",
                            started_at=sink_start,
                            completed_at=sink_end,
                            status="failed",
                            batch_date=batch_date,
                            records_written=0,
                            sink_path=sink_path,
                            error_message=str(e)
                        ))
                        raise  # re-raise to fail pipeline
            
            # update manifest after successful batch processing
            manifest = update_manifest(manifest, batch_date, run_id)
            upload_manifest(manifest, manifest_config)
            
            batch_end = get_timestamp()
        
        # consolidate data after all batches processed
        consolidation_start = get_timestamp()
        consolidation_results = consolidate_data(spark, config, metadata)
        consolidation_end = get_timestamp()
        
        # log consolidation stage
        stage["sub_stages"].append(create_sub_stage(
            name="consolidate_data",
            stage_type="consolidation",
            started_at=consolidation_start,
            completed_at=consolidation_end,
            status="success",
            consolidation_results=consolidation_results
        ))

        spark.stop()
        
        # end stage successfully
        end_stage(
            stage, 
            status="success", 
            dataflow_name=metadata["dataflows"][0]["name"],
            batches_processed=len(new_batches),
            batch_list=new_batches,
            last_processed_batch=manifest.get('last_processed_batch')
        )
        
    except Exception as e:
        # log failure
        end_stage(stage, status="failed", error_message=str(e))
        upload_log_to_minio(log_structure, logging_config)
        raise
    
    # upload (updated) log
    upload_log_to_minio(log_structure, logging_config)


if __name__ == "__main__":
    run_pipeline()