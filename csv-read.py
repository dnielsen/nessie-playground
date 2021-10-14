"""
Use csv file as a Spark SQL data source.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/datasource.py
"""
import os
from pyspark.sql import SparkSession
# from pyspark.sql import Row
# from pyspark.sql import *
from pyspark import SparkConf
import pynessie
# findspark.init()
pynessie_version = pynessie.__version__

conf = SparkConf()
# we need iceberg libraries and the nessie sql extensions
conf.set("spark.jars.packages", f"org.apache.iceberg:iceberg-spark3-runtime:0.11.1,org.projectnessie:nessie-spark-extensions:{pynessie_version}")
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
df = spark.read.csv("/Users/davenielsen/code/nessie-playground/data/totals_stats.csv")
df.show()

print("Current working dir : %s" % os.getcwd())
spark.sql("CREATE BRANCH dev IN dev_catalog AS main")
print("CREATED BRANCH dev")
spark.sql("""CREATE TABLE IF NOT EXISTS dev_catalog.warehouse.salaries (Season STRING, Team STRING, Salary STRING, Player STRING) USING iceberg""");
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW salaries_table USING csv
            OPTIONS (path "/Users/davenielsen/code/nessie-playground/data/salaries.csv", header true)""");
spark.sql('INSERT INTO dev_catalog.warehouse.salaries SELECT * FROM salaries_table');


spark.stop()
