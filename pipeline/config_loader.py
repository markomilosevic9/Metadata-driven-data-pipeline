import os
import yaml
from pathlib import Path


# module serves as single source of truth for configuration details
# script loads pipeline_config.yaml, provides env variables etc.
# all scripts should use this module to access config

# get pipeline configuration from yaml file, returns dict with config values
def load_config(config_path: str = None) -> dict:
    if config_path is None:
        project_root = Path(__file__).parent.parent
        config_path = project_root / "config" / "pipeline_config.yaml"
    
    with open(config_path, 'r') as f:
        raw_yaml = f.read()
    
    # simple env variable substitution
    substituted_yaml = os.path.expandvars(raw_yaml)
    config = yaml.safe_load(substituted_yaml)
    
    return config

# get spark-related config, returns dict with spark settings
def get_spark_config(config: dict = None) -> dict:
    if config is None:
        config = load_config()
    
    return config.get('spark', {})

# get minio config, returns dict with minio settings
def get_storage_config(config: dict = None) -> dict:
    if config is None:
        config = load_config()
    
    return config.get('storage', {})

# get pipeline-related config, returns dict with pipeline settings
def get_pipeline_config(config: dict = None) -> dict:
    if config is None:
        config = load_config()
    
    return config.get('pipeline', {})

# get url of spark master, returns url as a string
def get_spark_master_url(config: dict = None) -> str:
    spark_config = get_spark_config(config)
    return os.getenv('SPARK_MASTER_URL', spark_config.get('master_url', 'local[*]'))

# get path to metadata json file, returns path to this file
def get_metadata_path(config: dict = None) -> str:
    pipeline_config = get_pipeline_config(config)
    return pipeline_config.get('metadata_path', 'config/metadata_motor.json')

# get logging config for json logs, returns dict with logging settings (bucket/endpoint/credentials)
def get_logging_config(config: dict = None) -> dict:
    if config is None:
        config = load_config()
    
    storage_config = config.get('storage', {})
    
    # extract endpoint 
    endpoint = storage_config.get('endpoint', 'http://minio:9000')
    endpoint_clean = endpoint.replace('http://', '').replace('https://', '')
    
    return {
        'bucket': storage_config.get('buckets', {}).get('pipeline_logs', 'pipeline-logs'),
        'endpoint': endpoint_clean,
        'access_key': storage_config.get('access_key', ''),
        'secret_key': storage_config.get('secret_key', ''),
        'secure': storage_config.get('secure', False)
    }