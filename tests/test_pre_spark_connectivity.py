import pytest
from pipeline.config_loader import get_spark_master_url

# 4th pre-pipeline test, uses spark fixture
# check if spark master url is valid, if sparksession can be created 

def test_spark_connectivity(spark):
    # check url
    spark_master = get_spark_master_url()
    assert spark_master, "Spark master url is not valid"
    assert spark_master.startswith("spark://"), f"Spark master URL must use spark:// protocol: {spark_master}"
    # sparksession
    assert spark is not None, "Spark session could not be created"
    