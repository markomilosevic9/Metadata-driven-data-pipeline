<a href="#"><p align="left">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/python-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/docker-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/apachespark-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/apacheairflow-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/pytest-dark.svg" width="50">

</p></a>

# Aim of the project
The goal of this repo is to demonstrate a dynamic, metadata-driven data pipeline. 

The pipeline relies entirely on a metadata.json file, which is used to generate Spark SQL clauses for data transformation, validation, and ingestion. It uses Python, (Py)Spark, and MinIO as object storage. Orchestration is handled by Airflow, and pipeline components run in separate Docker containers. A test suite (PyTest) covers both pre-pipeline and post-processing checks (including data quality). Moreover, a lightweight custom logging system tracks key events across pipeline runs.

## Project structure

<pre>
│   .env                                              <- Env variables/secrets
│   docker-compose.yml                                <- Docker config
│   Dockerfile.airflow                                <- Dockerfile for Airflow
│   Dockerfile.spark                                  <- Dockerfile for Spark
│   generate_sample_data.py                           <- Python script, generates input data sample
│   Makefile                                          <- Set of Makefile commands for quick setup of the project
│   pytest.ini                                        <- Config file used by `pytest`
│   requirements.txt                                  <- The requirements file
│
├───airflow
│   ├───dags
│   │       motor_policy_pipeline_dag.py              <- Airflow DAG file
│   └───logs
├───config
│       metadata_motor.json                           <- Metadata JSON file - pipeline definition (sources, transforms, sinks)
│       pipeline_config.yaml                          <- Config file used by pipeline - storage, logging, JARs etc.
│       spark-defaults.conf                           <- Config file used by Spark (JVM settings)
│
├───pipeline
│       config_loader.py                              <- YAML config parser with helper functions
│       metadata_loader.py                            <- Module that loads JSON metadata
│       runner.py                                     <- Core of the pipeline: processes data, applies transformations, and writes outputs
│       sink.py                                       <- Module that writes final datasets to storage
│       transformer.py                                <- Module that transforms validated data
│       validator.py                                  <- Module that validates fields according to metadata rules
│
├───tests
│       conftest.py                                   <- Provides fixtures, hooks for testing suite 
│       test_post_pipeline_data_quality.py            <- Post-hoc checks for data quality (if accepted records passes validation rules)
│       test_post_pipeline_error_capture.py           <- Post-hoc checks for capturing errors (error validation)
│       test_post_pipeline_outputs_exist.py           <- Post-hoc checks for pipeline success (logical pipeline issues)
│       test_pre_config_integrity.py                  <- Pre-pipeline checks for configs, internal consistency
│       test_pre_input_data_exists.py                 <- Pre-pipeline checks for input data availability/presence and validity
│       test_pre_minio_connectivity.py                <- Pre-pipeline checks for connectivity (Minio)
│       test_pre_spark_connectivity.py                <- Pre-pipeline checks for connectivity (Spark)
│
└───utils
        json_logger.py                                <- Simple custom logging
</pre>

# Workflow overview

<img width="940" height="445" alt="image" src="https://github.com/user-attachments/assets/2dfce5ed-4abb-4bbd-ba5e-7eae17882efc" />

## Data sample generation
The data sample is produced by a Python script and contains 100 000 rows stored in a .jsonl file. It simulates motor policy data with three fields: policy_number, driver_age, and plate_number. The data sample is intentionally made reproducible with deterministic random seed so every run generates identical output.

A few details:
  *	policy_number is always unique and there is no missing data.
  *	driver_age is missing in 5% of records (the key is omitted); otherwise ranges from 17 to 80.
  *	plate_number contains empty strings in 5% of records.

Also, this step initializes a JSON log structure stored in `pipeline-logs`. It represents a lightweight solution for simple logging, improving observability by providing an easily readable overview of pipeline execution. Spark and Airflow logs, on the other hand, are more useful for debugging.

## Object storage
<img width="800" height="400" alt="image" src="https://github.com/user-attachments/assets/ea230c53-e7d7-4fff-9d88-fdbedb263c55" />

MinIO is used as the object storage. After project setup, five required buckets are created: `input-data`, `motor-policy-ok`, `motor-policy-ko`, `spark-logs` and `pipeline-logs`.

These buckets store all files generated during data creation, pipeline execution, testing, and logging.

## Data processing
The core of the data processing is a pipeline that uses metadata.json to dynamically generate Spark SQL clauses. Although the approach is SQL-driven, the pipeline is organized into several modules: the metadata loader, runner, transformer, validator, and sink. The pipeline logic is entirely defined by the rules in the metadata file. At a lower level, it relies on a sequence of temporary views to produce the expected output.

* Metadata loader: loads JSON metadata
* Validator: generates SparkSQL expressions that validate each field according to the metadata rules. It detects missing fields, records failing rules (names and details), and detects when no predefined rule fails (more precisely, returns null error column when all rules pass for a field). 
* Transformer: adds additional column (precisely, ingestion timestamp) to the data that passed validation stage. From practical standpoint, it easily might be extended with additional functions.  
* Sink: writes the final datasets to object storage
* Runner: reads the metadata, processes input sources by applying predefined operations, and writes results to MinIO. Actually, runner - in some sense - orchestrates all other pipeline modules in the defined order, delegating pieces of logic to validator/transformer.

### Rationale
Primary reason to choose SQL-centric approach is because Spark SQL views enable declarative querying and easy chaining of transformations without materializing intermediate datasets.Thus, it seemed as adequate approach when it comes to generating from metadata. More precisely, the temp view management/chaining is handled by scripts desribed above, forming coherent and logical order of operations. 

On high level of abstraction the pipeline might be simply described in following way: 
* Metadata file is being read, as well as input sample data. Temporary view (named `policy_inputs`) appears. 
* Reading the input view is followed by generating of SparkSQL expressions used for validation. These expressions evaluates predefined rules and returns either if all rules pass for specific field or an array of errors if any of rules fail.
* Intermediate validated view appears; on top of previous view, containing all the columns with their error columns, as well as all records (both valid and invalid).
* Operating on previous view produces 2 new views: OK and KO. Naturally, KO view get map that contains validation errors.
* Before sink writing, the views are enriched with additional column (ingestion timestamp). Finally, materialization occurs during the sink write operation; results are saved as JSON files in predesigned buckets.



## Testing suite/data quality
Since the aforementioned pipeline interacts with multiple services, several lightweight pre-pipeline (smoke) tests are included. These tests verify config integrity, MinIO and Spark connectivity and input data availability. In other words, they check if endpoints are reachable, as well as existence of fields, inputs, references, input data and storage buckets required for further steps within workflow.

After pipeline execution, a set of post-hoc tests is performed. These tests detect logical pipeline issues (whether output files are written to the correct buckets, whether record counts match expectations, and whether column structures are correct). They also evaluate data quality in broader sense, checking for null or empty fields, simple „business rules“ (e.g. driver age must be >= 18), pattern validation (e.g., regex checks), and ingestion timestamps. 

PyTest hooks handles logging for described testing suites. 

## Containerization 
The pipeline relies on a set of Docker containers that provide all required services, including Spark, Airflow (with its metadata DB), and MinIO. Spark and Airflow each have dedicated Dockerfiles, while the general environment (volumes, networks, service config etc.) is defined in the `docker-compose.yml` file.

## Workflow orchestration 
<img width="1019" height="281" alt="image" src="https://github.com/user-attachments/assets/778763c8-ef8b-49cf-a864-ba1872761d42" />

Apache Airflow manages scheduling and orchestration through a simple DAG. Each pipeline run begins with an initialization step that creates a temporary `.run-id` file, which is then used by all subsequent stages. A cleanup step is executed at the end of the workflow.

The DAG includes PythonOperators and BashOperators, depending on the each stage. Sample data generation runs as a pure Python script, while tests and the main pipeline are executed via Spark. So, BashOperator is used to submit commands to the Spark, including those required for running the test suite.

<img width="1902" height="653" alt="image" src="https://github.com/user-attachments/assets/56669519-1d0c-4ef6-8a66-6b8460e32868" />

## Other details
Configuration files for Spark and the overall pipeline are included. The first defines parameters such as MinIO connection details and Spark memory settings, while the later stores configuration for storage/buckets, testing, logging, JARs etc. A separate .env file contains the credentials and secrets required for MinIO, Spark, Airflow, and other services included in the project.

# Cloning the project

After cloning/downloading the repo, note that a Makefile is provided for convenience.

Running the following commands will set up the project easily:
`make build` and `make up` 

Alternatively, you can achieve the same setup using:
`docker-compose build` and `docker-compose up -d` 

Web interfaces are accessible as follows:

Airflow: http://localhost:8080 | username: admin, password: admin

MinIO: http://localhost:9001 | username: minioadmin, password: minioadmin
