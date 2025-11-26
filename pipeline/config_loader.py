import os
import yaml
from pathlib import Path


# script loads pipeline_config.yaml, provides helper functions to access config values
# all the functions gets their respective info from pre-loaded config dict (the very 1st step); otherwise load fresh

# get pipeline configuration from yaml file, returns dict with config values
def load_config(config_path: str = None) -> dict:
    if config_path is None:
        # find config/pipeline_config.yaml from project root
        project_root = Path(__file__).parent.parent
        config_path = project_root / "config" / "pipeline_config.yaml"
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
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

# get path to metadata json, returns path to this file
def get_metadata_path(config: dict = None) -> str:
    pipeline_config = get_pipeline_config(config)
    return pipeline_config.get('metadata_path', 'config/metadata_motor.json')

# get logging config for json logs, returns dict with logging settings
def get_logging_config(config: dict = None) -> dict:
    if config is None:
        config = load_config()
    
    storage_config = config.get('storage', {})
    
    return {
        'bucket': storage_config.get('buckets', {}).get('pipeline_logs', 'pipeline-logs'),
        'endpoint': storage_config.get('endpoint', 'http://minio:9000').replace('http://', '').replace('https://', ''),
        'access_key': storage_config.get('access_key', 'minioadmin'),
        'secret_key': storage_config.get('secret_key', 'minioadmin'),
        'secure': storage_config.get('secure', False)
    }