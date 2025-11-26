import os
from pyspark.sql import SparkSession
from metadata_loader import load_metadata
from transformer import execute_add_fields
from sink import write_df
from validator import execute_validation
from config_loader import (
    load_config, 
    get_spark_master_url, 
    get_metadata_path, 
    get_storage_config,
    get_logging_config
)
from json_logger import (
    get_timestamp,
    create_sub_stage,
    start_stage,
    end_stage,
    upload_log_to_minio,
    read_log_from_minio
)

# core pipeline script
# gets metadata, processes input data sample, applies transformations, handles outputs
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
    
    # get existing log from minio
    log_structure = read_log_from_minio(run_id, **logging_config)
    
    # start spark_pipeline stage
    stage = start_stage(log_structure, "spark_pipeline")
    
    try:
        # make spark session
        spark = SparkSession.builder \
            .master(spark_master) \
            .appName(config['spark']['app_name']) \
            .getOrCreate()

        # config for minio access 
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", storage_config['access_key'])
        hadoop_conf.set("fs.s3a.secret.key", storage_config['secret_key'])
        hadoop_conf.set("fs.s3a.endpoint", storage_config['endpoint'])
        hadoop_conf.set("fs.s3a.path.style.access", str(storage_config.get('path_style_access', True)).lower())
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", str(storage_config.get('secure', False)).lower())

        # get metadata path
        if metadata_path is None:
            metadata_path = get_metadata_path(config)
        
        metadata = load_metadata(metadata_path)

        for flow in metadata["dataflows"]:
            # load sources
            for source in flow["sources"]:
                source_start = get_timestamp()
                
                df = spark.read.format(source.get("format", "json")) \
                               .option("mode", "PERMISSIVE") \
                               .options(**source.get("options", {})) \
                               .load(source["path"])

                source_name = source["name"]
                df.createOrReplaceTempView(source_name)
                
                record_count = df.count()
                source_end = get_timestamp()
                
                # log source loadings in json
                stage["sub_stages"].append(create_sub_stage(
                    name=f"source_load_{source_name}",
                    stage_type="source",
                    started_at=source_start,
                    completed_at=source_end,
                    records_loaded=record_count,
                    source_path=source["path"]
                ))

            # apply transformation
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
                    
                    # log validation 
                    stage["sub_stages"].append(create_sub_stage(
                        name=transform_name,
                        stage_type="transformation",
                        started_at=transform_start,
                        completed_at=transform_end,
                        transformation_type="validate_fields",
                        total_records=total_count,
                        ok_count=ok_count,
                        ko_count=ko_count,
                        ok_percentage=round((ok_count / total_count * 100), 2) if total_count > 0 else 0,
                        ko_percentage=round((ko_count / total_count * 100), 2) if total_count > 0 else 0
                    ))

                elif transform_type == "add_fields":
                    # execute add_fields 
                    df_result, records_processed = execute_add_fields(
                        spark, input_view, params["addFields"]
                    )

                    # make output view with transformation name
                    df_result.createOrReplaceTempView(transform_name)
                    
                    transform_end = get_timestamp()
                    
                    # log add_fields transformation
                    stage["sub_stages"].append(create_sub_stage(
                        name=transform_name,
                        stage_type="transformation",
                        started_at=transform_start,
                        completed_at=transform_end,
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
                    records_written=record_count,
                    sink_path=sink_path,
                    format=sink.get("format", "json")
                ))

        spark.stop()
        
        # end stage successfully
        end_stage(stage, status="success", dataflow_name=metadata["dataflows"][0]["name"])
        
    except Exception as e:
        # log failure
        end_stage(stage, status="failed", error_message=str(e))
        upload_log_to_minio(log_structure, **logging_config)
        raise
    
    # upload (updated) log
    upload_log_to_minio(log_structure, **logging_config)


if __name__ == "__main__":
    run_pipeline()