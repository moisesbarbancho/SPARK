import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session", autouse=True)
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # Use Java 11 for PySpark compatibility
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
    # Minimal Spark configuration for local testing
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    return spark

