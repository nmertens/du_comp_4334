# Databricks notebook source
# Lab 5
# Nickolas Mertens
sc = spark.sparkContext

# COMMAND ----------

playersRDD = sc.textFile('FileStore/tables/Master.csv').map(lambda l: l.split(","))
header = playersRDD.first() #extract header
# print(header)
playersRDD = playersRDD.filter(lambda row: row != header)
# print(playersRDD.first())
# print(playersRDD.count())
playersRDD = playersRDD.filter(lambda row: row[17] != '') # filter out rows with no height measurement
# print(playersRDD.count())

# COMMAND ----------

playersRowsRDD = playersRDD.map(lambda l: Row(l[0], l[4], l[5], int(l[17]))) # playerID, birthCountry, birthState, height
# print(playersRowsRDD.count())

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType, StringType
import pyspark.sql.functions as f

# COMMAND ----------

playersSchema = StructType( [\
                            StructField('playerID', StringType(), True), \
                            StructField('birthCountry', StringType(), True), \
                            StructField('birthState', StringType(), True), \
                            StructField('height', LongType(), True)
                           ])

playersDF = spark.createDataFrame(playersRowsRDD, playersSchema) 
playersDF.show()
playersDF.printSchema()

# COMMAND ----------

playersDF.createOrReplaceTempView("players")

# COMMAND ----------

# Colorado Count SQL
spark.sql('SELECT COUNT(playerID) AS count FROM players WHERE birthState = "CO"').show()

# COMMAND ----------

# Colorado Count DataFrame functions
playersDF.filter(playersDF['birthState'] == 'CO').select(f.count('playerID').alias("count")).show()

# COMMAND ----------

# Height By Country SQL
spark.sql("SELECT birthCountry, AVG(height) AS avgheight FROM players GROUP BY birthCountry ORDER BY avgheight DESC").show(playersDF.count())

# COMMAND ----------

# Height By Country DataFrame functions
playersDF.select(f.col('birthCountry'), f.col('height')).groupBy(playersDF['birthCountry']).agg(f.avg(playersDF['height']).alias('avgheight')).orderBy(f.desc('avgheight')).show(playersDF.count())
