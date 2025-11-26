from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from pipeline.config_loader import load_config, get_spark_config

# DAG
# aim is clean orchestration/scheduling only
# airflow orchestrates while each script handles its own logging
# 1st script creates log, last script finalizes it


# get config
config = load_config()
spark_config = get_spark_config(config)

# get values
PROJECT_ROOT = "/opt/motor-policy"
SPARK_MASTER = spark_config['master_url']
SPARK_CLIENT_CONTAINER = "spark-client"

# default args
default_args = {
    'owner': 'marko',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'motor_policy_pipeline',
    default_args=default_args,
    description='DAG for pipeline',
    schedule_interval=None, # or '@hourly' - it is just a demo
    catchup=False,
    max_active_runs=1, 
    is_paused_upon_creation=False,
)


# init run_id (for coordination)
# generate it and save for all subsequent tasks
def create_run_id(**context):
    exec_date = context['execution_date']
    run_id = f"run_{exec_date.strftime('%Y-%m-%d_%H%M%S')}"
    
    # write for all following tasks to read
    with open(f'{PROJECT_ROOT}/.run_id', 'w') as f:
        f.write(run_id)
    
    return run_id

init_run = PythonOperator(
    task_id='init_run_id',
    python_callable=create_run_id,
    dag=dag,
)

# generate sample data, creates log, logs itself
def generate_sample_data_task(**context):
    from generate_sample_data import main
    
    # get run_id 
    with open(f'{PROJECT_ROOT}/.run_id', 'r') as f:
        run_id = f.read().strip()
    
    # script creates log structure and handles everything
    main(run_id=run_id)

generate_data = PythonOperator(
    task_id='generate_sample_data',
    python_callable=generate_sample_data_task,
    dag=dag,
)

# pre-pipeline pytest suite 
pre_pipeline_tests = BashOperator(
    task_id='pre_pipeline_tests',
    bash_command=(
        f'docker exec {SPARK_CLIENT_CONTAINER} bash -c "'
        f'cd {PROJECT_ROOT} && '
        f'export RUN_ID=$(cat {PROJECT_ROOT}/.run_id) && '
        f'export SPARK_MASTER_URL={SPARK_MASTER} && '
        f'python3 -m pytest tests -m \\"not post_pipeline\\" -v --tb=short'
        '"'
    ),
    dag=dag,
)


# core spark pipeline
jars_list = ','.join(spark_config.get('jars', []))
run_pipeline = BashOperator(
    task_id='run_spark_pipeline',
    bash_command=(
        f'docker exec {SPARK_CLIENT_CONTAINER} bash -c "'
        f'cd {PROJECT_ROOT} && '
        f'export RUN_ID=$(cat {PROJECT_ROOT}/.run_id) && '
        f'spark-submit '
        f'--master {SPARK_MASTER} '
        f'--deploy-mode client '
        f'--jars {jars_list} '
        f'pipeline/runner.py'
        '"'
    ),
    dag=dag,
)

# post-pipeline pytest suite 
post_pipeline_tests = BashOperator(
    task_id='post_pipeline_tests',
    bash_command=(
        f'docker exec {SPARK_CLIENT_CONTAINER} bash -c "'
        f'cd {PROJECT_ROOT} && '
        f'export RUN_ID=$(cat {PROJECT_ROOT}/.run_id) && '
        f'export SPARK_MASTER_URL={SPARK_MASTER} && '
        f'export FINALIZE_LOG=true && '
        f'python3 -m pytest tests -m post_pipeline -v --tb=short'
        '"'
    ),
    dag=dag,
)

# always clean-up run_id
cleanup_run_id = BashOperator(
    task_id='cleanup_run_id',
    bash_command=f'docker exec {SPARK_CLIENT_CONTAINER} rm -f {PROJECT_ROOT}/.run_id',
    trigger_rule=TriggerRule.ALL_DONE,  
    dag=dag,
)

# dependency chain
(
    init_run
    >> generate_data
    >> pre_pipeline_tests
    >> run_pipeline
    >> post_pipeline_tests
    >> cleanup_run_id
)