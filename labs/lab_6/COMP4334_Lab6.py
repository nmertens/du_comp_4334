# Databricks notebook source
# Nickolas Mertens
# Lab 6
sc = spark.sparkContext

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

masterFile = "dbfs:///FileStore/tables/Master.csv"
masterDF = spark.read.format("csv").option("header", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(masterFile)
# masterDF.show()

# COMMAND ----------

teamsFile = "dbfs:///FileStore/tables/Teams.csv"
teamsDF = spark.read.format("csv").option("header", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(teamsFile)
# teamsDF.show()

# COMMAND ----------

allstarFile = "dbfs:///FileStore/tables/AllstarFull.csv"
allstarDF = spark.read.format("csv").option("header", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(allstarFile)
# allstarDF.show()

# COMMAND ----------

masterDF = masterDF.select(f.col("playerID"), f.col("nameFirst"), f.col("nameLast"))
# masterDF.show()
teamsDF = teamsDF.select(f.col("teamID"), f.col("name").alias("teamName"))
# teamsDF.show()
allstarDF = allstarDF.select(f.col("playerID"), f.col("teamID"))
# allstarDF.show()

# COMMAND ----------

joinedDF = allstarDF.join(masterDF, "playerID").join(teamsDF, "teamID").distinct()
joinedDF.show()

# COMMAND ----------

parFilename = "dbfs:///FileStore/tables/allstars/"
dbutils.fs.rm(parFilename, True)
joinedDF.write.format("parquet").mode("overwrite").save(parFilename)

# COMMAND ----------

newDF = spark.read.format("parquet").load(parFilename)
newDF.show()

# COMMAND ----------

newDF.filter(newDF["teamName"] == "Colorado Rockies").agg(f.count("playerID").alias("CR Allstar Total")).show()
newDF.filter(newDF["teamName"] == "Colorado Rockies").select(f.col("nameFirst"), f.col("nameLast")).show(newDF.count())
