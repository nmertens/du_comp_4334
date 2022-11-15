# Databricks notebook source
# Nickolas Mertens
# COMP 4334 Lab 8

from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
import pyspark.sql.functions as f

fifaSchema = StructType( \
                        [StructField('ID', LongType(), True), \
                         StructField('lang', StringType(), True), \
                         StructField('Date', TimestampType(), True), \
                         StructField('Source', StringType(), True), \
                         StructField('len', LongType(), True), \
                         StructField('Orig_Tweet', StringType(), True), \
                         StructField('Tweet', StringType(), True), \
                         StructField('Likes', LongType(), True), \
                         StructField('RTs', LongType(), True), \
                         StructField('Hashtags', StringType(), True), \
                         StructField('UserMentionNames', StringType(), True), \
                         StructField('UserMentionID', StringType(), True), \
                         StructField('Name', StringType(), True), \
                         StructField('Place', StringType(), True), \
                         StructField('Followers', LongType(), True), \
                         StructField('Friends', LongType(), True), \
                        ])

# COMMAND ----------

# Load in the full dataset
fifaPath = "dbfs:///FileStore/tables/FIFA.csv"
fifa = spark.read.format("csv").option("header", True).schema(fifaSchema).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(fifaPath)

# COMMAND ----------

# View dataset
fifa.show()

# COMMAND ----------

# Check number of partitions
print(fifa.rdd.getNumPartitions())

# COMMAND ----------

# Repartition
fifa = fifa.orderBy('Date').repartition(20).persist()
print(fifa.rdd.getNumPartitions())
# print(fifa.rdd.glom().collect())

# COMMAND ----------

# Save newly partitioned data to FileStore
dbutils.fs.rm("FileStore/tables/fifaTime/", True)
fifa.write.format("csv").option("header", True).save("FileStore/tables/fifaTime/")

# COMMAND ----------

# Static window testing
fifa = fifa.select(f.col('ID'), f.col('Date'), f.col('Hashtags')).filter(f.col('Hashtags').isNotNull()).persist()
fifa = fifa.withColumn('Hashtags', f.explode(f.split('Hashtags', ','))).persist()
fifaWin = fifa.groupBy(f.window("Date", "60 minutes", "30 minutes"), "HashTags").agg(f.count("ID").alias("count")).filter(f.col("count") > 100).orderBy(f.col("window").asc(), f.col("count").desc())

# COMMAND ----------

# Check output 
fifaWin.show(20, False)

# COMMAND ----------

# Setup the stream
sourceStream = spark.readStream.format("csv").option("header", True).schema(fifaSchema).option("maxFilesPerTrigger", 1).load("dbfs:///FileStore/tables/fifaTime/")

# COMMAND ----------

# Query for streaming information
fifaWin = sourceStream.withWatermark("Date", "24 hours")\
                      .select(f.col('ID'), f.col('Date'), f.col('Hashtags'))\
                      .filter(f.col('Hashtags').isNotNull())\
                      .withColumn('Hashtags', f.explode(f.split('Hashtags', ',')))\
                      .groupBy(f.window("Date", "60 minutes", "30 minutes"), "HashTags")\
                      .agg(f.count("ID").alias("count"))\
                      .filter(f.col("count") > 100)\
                      .orderBy(f.col("window").asc(), f.col("count").desc())

# COMMAND ----------

# Set up the sink
sinkStream = fifaWin.writeStream.outputMode("complete").format("memory").queryName("fifWin").trigger(processingTime='10 seconds'). start()

# COMMAND ----------

# Check the output while stream happening
spark.sql("SELECT * FROM fifWin").show(20, False)
