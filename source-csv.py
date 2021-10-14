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
    path = "data/totals_stats.csv"

    df = spark.read.csv(path)
    df.show()
    # +------------------+
    # |               _c0|
    # +------------------+
    # |      name;age;job|
    # |Jorge;30;Developer|
    # |  Bob;32;Developer|
    # +------------------+
	

    # $example off:csv_dataset$


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source") \
        .getOrCreate()

    csv_read(spark)


    spark.stop()
