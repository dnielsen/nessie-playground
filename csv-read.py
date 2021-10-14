"""
Use csv file as a Spark SQL data source.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/datasource.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row


def csv_read(spark):
    sc = spark.sparkContext

    # A CSV dataset is pointed to by path.
    # The path can be either a single CSV file or a directory of CSV files
    path = "/Users/davenielsen/code/nessie-playground/data/totals_stats.csv"

    df = spark.read.csv(path)
    df.show()

if __name__ == "__main2__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source") \
        .getOrCreate()
    csv_read(spark)

if __name__ == "__main__":
import os
import findspark
from pyspark.sql import *
from pyspark import SparkConf
import pynessie
findspark.init()
pynessie_version = pynessie.__version__

conf = SparkConf()
	# we need iceberg libraries and the nessie sql extensions
	conf.set("spark.jars.packages", f"org.apache.iceberg:iceberg-spark3-runtime:0.12.0,org.projectnessie:nessie-spark-extensions:{pynessie_version}")
	# ensure python <-> java interactions are w/ pyarrow
	conf.set("spark.sql.execution.pyarrow.enabled", "true")
	# create catalog dev_catalog as an iceberg catalog
	conf.set("spark.sql.catalog.dev_catalog", "org.apache.iceberg.spark.SparkCatalog")
	# tell the dev_catalog that its a Nessie catalog
	conf.set("spark.sql.catalog.dev_catalog.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
	# set the location for Nessie catalog to store data. Spark writes to this directory
	conf.set("spark.sql.catalog.dev_catalog.warehouse", 'file://' + os.getcwd() + '/spark_warehouse/iceberg')
	# set the location of the nessie server. In this demo its running locally. There are many ways to run it (see https://projectnessie.org/try/)
	conf.set("spark.sql.catalog.dev_catalog.uri", "http://localhost:19120/api/v1")
	# default branch for Nessie catalog to work on
	conf.set("spark.sql.catalog.dev_catalog.ref", "main")
	# use no authorization. Options are NONE AWS BASIC and aws implies running Nessie on a lambda
	conf.set("spark.sql.catalog.dev_catalog.auth_type", "NONE")
	# enable the extensions for both Nessie and Iceberg
	conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
	# finally, start up the Spark server
	spark = SparkSession.builder.config(conf=conf).getOrCreate()
	csv_read(spark)
    spark.stop()
