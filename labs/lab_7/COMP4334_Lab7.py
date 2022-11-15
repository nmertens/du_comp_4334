# Databricks notebook source
# Nickolas Mertens
# COMP 4334 Lab 7

# Load in necessary packages
from pyspark.sql.types import StructType, StructField, LongType, StringType
import pyspark.sql.functions as f
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, Bucketizer, VectorAssembler
from pyspark.ml.classification import LogisticRegression

# Create DataFrame schema
heartSchema = StructType( \
                          [StructField('id', LongType(), True), \
                           StructField('age', LongType(), True), \
                           StructField('sex', StringType(), True), \
                           StructField('chol', LongType(), True), \
                           StructField('pred', StringType(), True)
                          ])

# COMMAND ----------

heartTrainPath = "dbfs:///FileStore/tables/heartTraining.csv"
heartTestPath = "dbfs:///FileStore/tables/heartTesting.csv"

heartTrain = spark.read.format("csv").option("header", True).schema(heartSchema).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(heartTrainPath)
heartTest = spark.read.format("csv").option("header", True).schema(heartSchema).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(heartTestPath)

# COMMAND ----------

# heartTrain.show()

# COMMAND ----------

# heartTest.show()

# COMMAND ----------

# Break age up into categories
ageSplits = [-float("inf"), 40, 50, 60, 70, float("inf")]
ageBucketizer = Bucketizer(splits = ageSplits, inputCol = "age", outputCol = "ageBucket")

# Turn sex and pred into numbers
sexIndexer = StringIndexer(inputCol = "sex", outputCol = "sexIndex")
predIndexer = StringIndexer(inputCol = "pred", outputCol = "label")

# COMMAND ----------

# Create vectors for Logistic Regression
vecAssem = VectorAssembler(inputCols = ['ageBucket', 'sexIndex', 'chol'], outputCol = 'features')

# COMMAND ----------

# Create the Logistic Regression
lr = LogisticRegression(maxIter = 10, regParam = 0.01)

# COMMAND ----------

# Build the pipeline
myStages = [ageBucketizer, sexIndexer, predIndexer, vecAssem, lr]
p = Pipeline(stages = myStages)

# COMMAND ----------

# Fit the model
pModel = p.fit(heartTrain)

# COMMAND ----------

trainPredictions = pModel.transform(heartTrain)
trainPredictions.select(f.col("id"), f.col("label"), f.col("probability"), f.col("prediction")).show(20, False)
# trainPredictions.show()

# COMMAND ----------

testPredictions = pModel.transform(heartTest)
testPredictions.select(f.col("id"), f.col("label"), f.col("probability"), f.col("prediction")).show(20, False)
# testPredictions.show()
