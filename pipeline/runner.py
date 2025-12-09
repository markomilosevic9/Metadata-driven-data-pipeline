import os
import json
from pyspark.sql import SparkSession
from pipeline.transformer import execute_add_fields
from pipeline.sink import write_df
from pipeline.validator import execute_validation
from pipeline.schema_enforcer import build_spark_schema, SchemaEnforcementError
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

# core pipeline script
# handles the entire data processing pipeline
# logging goes to structured json in designated minio bucket


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

        # process each dataflow defined within metadata file
        for flow in metadata["dataflows"]:
            # load sources 
            for source in flow["sources"]:
                source_start = get_timestamp()
                source_name = source["name"]
                required = source.get("required", True)  # default to required if not specified
                
                try:
                    # check if schema enforcement is enabled
                    schema_def = source.get("schema")
                    enforcement_config = source.get("schema_enforcement", {})
                    enforcement_enabled = enforcement_config.get("enabled", False)
                    
                    if schema_def and enforcement_enabled:
                        # schema enforcement enabled, build and apply schema
                        # build spark structtype from metadata schema
                        expected_schema = build_spark_schema(schema_def)
                        
                        # read with explicit schema enforcement
                        df = spark.read \
                                  .schema(expected_schema) \
                                  .format(source.get("format", "json")) \
                                  .options(**source.get("options", {})) \
                                  .load(source["path"])
                        
                        record_count = df.count()
                        source_end = get_timestamp()
                        
                        # log source loading in json (with schema enforcement details)
                        stage["sub_stages"].append(create_sub_stage(
                            name=f"source_load_{source_name}",
                            stage_type="source",
                            started_at=source_start,
                            completed_at=source_end,
                            status="success",
                            records_loaded=record_count,
                            source_path=source["path"],
                            schema_enforced=True,
                            enforced_fields=[f["name"] for f in schema_def["fields"]],
                            source_required=required
                        ))
                    
                    else:
                        # if no schema enforcement, use spark inference 
                        df = spark.read \
                                  .format(source.get("format", "json")) \
                                  .option("mode", "PERMISSIVE") \
                                  .options(**source.get("options", {})) \
                                  .load(source["path"])
                        
                        record_count = df.count()
                        source_end = get_timestamp()
                        
                        # log source loading in json without schema enforcement
                        stage["sub_stages"].append(create_sub_stage(
                            name=f"source_load_{source_name}",
                            stage_type="source",
                            started_at=source_start,
                            completed_at=source_end,
                            status="success",
                            records_loaded=record_count,
                            source_path=source["path"],
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
                            name=f"source_load_{source_name}",
                            stage_type="source",
                            started_at=source_start,
                            completed_at=source_end,
                            status="failed",
                            records_loaded=0,
                            source_path=source["path"],
                            error_message=str(e),
                            source_required=True
                        ))
                        raise  # re-raise to fail pipeline
                    
                    else:
                        # if optional source failed, log skip and continue
                        stage["sub_stages"].append(create_sub_stage(
                            name=f"source_load_{source_name}",
                            stage_type="source",
                            started_at=source_start,
                            completed_at=source_end,
                            status="skipped",
                            records_loaded=0,
                            source_path=source["path"],
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

                    # make output views
                    df_ok.createOrReplaceTempView("validation_ok")
                    df_ko.createOrReplaceTempView("validation_ko")

                    transform_end = get_timestamp()
                    
                    # log validation results
                    stage["sub_stages"].append(create_sub_stage(
                        name=transform_name,
                        stage_type="transformation",
                        started_at=transform_start,
                        completed_at=transform_end,
                        status="success",
                        transformation_type="validate_fields",
                        total_records=total_count,
                        ok_count=ok_count,
                        ko_count=ko_count,
                        ok_percentage=round((ok_count / total_count * 100), 2) if total_count > 0 else 0,
                        ko_percentage=round((ko_count / total_count * 100), 2) if total_count > 0 else 0
                    ))

                elif transform_type == "add_fields":
                    # execute transformation(s), currently it is add_fields
                    df_result, records_processed = execute_add_fields(
                        spark, input_view, params["addFields"]
                    )

                    # make output view
                    df_result.createOrReplaceTempView(transform_name)
                    
                    transform_end = get_timestamp()
                    
                    # log add_fields transformation
                    stage["sub_stages"].append(create_sub_stage(
                        name=transform_name,
                        stage_type="transformation",
                        started_at=transform_start,
                        completed_at=transform_end,
                        status="success",
                        transformation_type="add_fields",
                        fields_added=[f["name"] for f in params["addFields"]],
                        records_processed=records_processed
                    ))

            # write outputs
            for sink in flow["sinks"]:
                sink_start = get_timestamp()
                sink_input = sink["input"]
                sink_name = sink["name"]
                sink_path = sink["path"] if "path" in sink else sink["paths"][0]
                
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
                        name=sink_name,
                        stage_type="sink",
                        started_at=sink_start,
                        completed_at=sink_end,
                        status="success",
                        records_written=record_count,
                        sink_path=sink_path,
                        format=sink.get("format", "json")
                    ))
                
                except Exception as e:
                    # handle sink write error if any, log and fail pipeline
                    sink_end = get_timestamp()
                    stage["sub_stages"].append(create_sub_stage(
                        name=sink_name,
                        stage_type="sink",
                        started_at=sink_start,
                        completed_at=sink_end,
                        status="failed",
                        records_written=0,
                        sink_path=sink_path,
                        error_message=str(e)
                    ))
                    raise  # re-raise to fail pipeline

        spark.stop()
        
        # end stage successfully
        end_stage(stage, status="success", dataflow_name=metadata["dataflows"][0]["name"])
        
    except Exception as e:
        # log failure
        end_stage(stage, status="failed", error_message=str(e))
        upload_log_to_minio(log_structure, logging_config)
        raise
    
    # upload (updated) log
    upload_log_to_minio(log_structure, logging_config)


if __name__ == "__main__":
    run_pipeline()