import os
import sys
import pytest
from pyspark.sql import SparkSession
from minio import Minio
from pipeline.config_loader import (
    load_config, 
    get_spark_master_url, 
    get_storage_config, 
    get_logging_config
)


# script contains shared fixtures used by pre- and post-pipeline test
# also handles json logging integration (within airflow DAG execution) via hooks

# test fixtures

# spark session configured and used by post-pipeline tests (when reading output data)
# event logging disabled for tests, they only require minio/s3a access for reading/writing, not for spark event history
# in runner script, event logging remains enabled via spark-defaults.conf and credentials are passed via --conf flags in the DAG
@pytest.fixture(scope="session")
def spark():
    # get config
    config = load_config()
    spark_master = get_spark_master_url(config)
    storage_config = get_storage_config(config)

    # create spark session 
    # event logging disabled for tests, overrides spark-defaults.conf
    spark = (
        SparkSession.builder
        .master(spark_master)
        .appName("MotorPolicyTests")
        .config("spark.eventLog.enabled", "false")
        .getOrCreate()
    )

    # minio config from config file
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", storage_config['access_key'])
    hadoop_conf.set("fs.s3a.secret.key", storage_config['secret_key'])
    hadoop_conf.set("fs.s3a.endpoint", storage_config['endpoint'])
    hadoop_conf.set("fs.s3a.path.style.access", str(storage_config.get('path_style_access', True)).lower())
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", str(storage_config.get('secure', False)).lower())

    yield spark
    spark.stop()

# minio client for pre-pipeline tests (connectivity, data presence/existence)
# uses config_loader
@pytest.fixture(scope="session")
def minio_client():
    config = load_config()
    storage_config = get_storage_config(config)
    
    # get endpoint
    endpoint = storage_config['endpoint'].replace('http://', '').replace('https://', '')
    
    client = Minio(
        endpoint=endpoint,
        access_key=storage_config['access_key'],
        secret_key=storage_config['secret_key'],
        secure=storage_config.get('secure', False)
    )
    
    return client


# pytest hooks, json logging

# gets called at the start of test session, only if run_id env variable is set within DAG execution context
def pytest_sessionstart(session):
    run_id = os.getenv('RUN_ID')
    if not run_id:
        # if not running in DAG context, skip logging
        return
    
    from utils.json_logger import read_log_from_minio, start_stage, upload_log_to_minio
    
    config = load_config()
    logging_config = get_logging_config(config)
    
    # read existing log structure (must exist, created by generate_sample_data)
    log_structure = read_log_from_minio(run_id, logging_config)
    
    if log_structure is None:
        raise ValueError(
            f"Log structure not found for run_id '{run_id}'. "
            "Previous pipeline tasks may have failed or not run yet."
        )
    
    # use pytest markers to determine stage name (pre- vs post-pipeline)
    if '-m post_pipeline' in ' '.join(sys.argv):
        stage_name = "post_pipeline_tests"
    else:
        stage_name = "pre_pipeline_tests"
    
    stage = start_stage(log_structure, stage_name)
    
    # store for session end
    session.config._log_structure = log_structure
    session.config._log_stage = stage
    session.config._run_id = run_id

# gets called at the end of test session, in the case of post-pipeline stage also finalize the whole log
def pytest_sessionfinish(session, exitstatus):
    if not hasattr(session.config, '_log_structure'):
        # if not running in DAG context
        return
    
    from utils.json_logger import end_stage, finalize_log, upload_log_to_minio
    
    config = load_config()
    logging_config = get_logging_config(config)
    
    log_structure = session.config._log_structure
    stage = session.config._log_stage
    run_id = session.config._run_id
    
    # determine status based on exit status
    status = "success" if exitstatus == 0 else "failed"
    test_result = "all_passed" if exitstatus == 0 else "some_failed"
    
    # end this test stage
    end_stage(stage, status=status, test_result=test_result)
    
    # check if this is the last stage (post-pipeline_tests) and should finalize
    finalize = os.getenv('FINALIZE_LOG', 'false').lower() == 'true'
    
    if finalize:
        # since this is the last stage, finalize whole pipeline log
        overall_status = "success" if exitstatus == 0 else "failed"
        finalize_log(log_structure, status=overall_status)
    
    # upload (updated) log
    upload_log_to_minio(log_structure, logging_config)