"""
SparkSession = Entry Point for DataFrame & DataSets
from pyspark.sql import SparkSession

SparkContext = Resilient Distributed DataSet
from pyspark import SparkContext

spark-submit /Users/luanmorenomaciel/GitHub/series-spark/src/pyspark/app.py
"""

# import libraries
# init a spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(spark)

# load data
df_device = spark.read.json("/Users/luanmorenomaciel/GitHub/series-spark/docs/files/device/device_2022_6_7_19_39_24.json")

# verify schema
df_device.printSchema()

# verify columns
print(df_device.columns)

# count rows
print(df_device.count())

# show data
df_device.show()

# [select] columns
df_device.select("manufacturer", "model", "platform").show()
df_device.select(df_device.manufacturer).show()

# [select expr] columns | run sql expressions
df_device.selectExpr("manufacturer", "model", "platform as type").show()

# filter data
df_device.filter(df_device.manufacturer == "Xiamomi").show()

# group data
df_device.groupBy("manufacturer").count().show()

# new dataframe = immutable
df_grouped_by_manufacturer = df_device.groupBy("manufacturer").count()
df_grouped_by_manufacturer.show()