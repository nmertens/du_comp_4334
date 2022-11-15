# Databricks notebook source
# COMP4334 Assignment 2
# Nickolas Mertens

from pyspark.sql.types import StructType, StructField, LongType, StringType, FloatType
import pyspark.sql.functions as f

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, Bucketizer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

starsSchema = StructType( \
                        [StructField('objID', StringType(), True), \
                         StructField('alpha', FloatType(), True), \
                         StructField('delta', FloatType(), True), \
                         StructField('ultra', FloatType(), True), \
                         StructField('green', FloatType(), True), \
                         StructField('red', FloatType(), True), \
                         StructField('nearInfra', FloatType(), True), \
                         StructField('infra', FloatType(), True), \
                         StructField('runID', LongType(), True), \
                         StructField('rerunID', LongType(), True), \
                         StructField('camCol', LongType(), True), \
                         StructField('fieldID', StringType(), True), \
                         StructField('specObjID', StringType(), True), \
                         StructField('objClass', StringType(), True), \
                         StructField('redshift', FloatType(), True), \
                         StructField('plate', LongType(), True), \
                         StructField('MJD', LongType(), True), \
                         StructField('fiberID', LongType(), True), \
                        ])

# COMMAND ----------

# Load in the full dataset
starsPath = "dbfs:///FileStore/tables/star_classification.csv"
# stars = spark.read.format("csv").option("header", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(starsPath)
stars = spark.read.format("csv").option("header", True).schema(starsSchema).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(starsPath)

# COMMAND ----------

stars.show(20)

# COMMAND ----------

# split the data into train and test
splits = stars.randomSplit(weights = [0.7, 0.3], seed = 1024)

# COMMAND ----------

# Get the Training and Testing data sets
starsTrain = splits[0]
starsTest = splits[1]

# COMMAND ----------

# Repartition test data and save to separate folder
print(starsTest.rdd.getNumPartitions())
starsTest = starsTest.repartition(100).persist()
print(starsTest.rdd.getNumPartitions())

# Save newly partitioned data to FileStore
dbutils.fs.rm("FileStore/tables/starsTestData/", True)
starsTest.write.format("csv").option("header", True).save("FileStore/tables/starsTestData/")

# COMMAND ----------

predIndexer = StringIndexer(inputCol = 'objClass', outputCol = 'label')
vecAssem = VectorAssembler(inputCols = ['alpha', 'delta', 'ultra', 'red', 'green', 'nearInfra', 'infra', 'redshift'], outputCol = 'features')
# vecAssem = VectorAssembler(inputCols = ['alpha', 'delta', 'ultra', 'red', 'green', 'nearInfra', 'infra'], outputCol = 'features')

# lr = LogisticRegression(maxIter = 10, regParam = 0.01)
lr = LogisticRegression()
dt = DecisionTreeClassifier()
rf = RandomForestClassifier()

# Build the pipeline
lrPipeline = Pipeline(stages = [predIndexer, vecAssem, lr])
dtPipeline = Pipeline(stages = [predIndexer, vecAssem, dt])
rfPipeline = Pipeline(stages = [predIndexer, vecAssem, rf])

# COMMAND ----------

# Fit the models
lrModel = lrPipeline.fit(starsTrain)
lrTrainPredictions = lrModel.transform(starsTrain)
# lrTrainPredictions.select(f.col("objID"), f.col("label"), f.col("probability"), f.col("prediction")).show(20, False)

dtModel = dtPipeline.fit(starsTrain)
dtTrainPredictions = dtModel.transform(starsTrain)

rfModel = rfPipeline.fit(starsTrain)
rfTrainPredictions = rfModel.transform(starsTrain)

# COMMAND ----------


evaluator = MulticlassClassificationEvaluator(predictionCol = "prediction")
print(f"Logistic Regression: {evaluator.evaluate(lrTrainPredictions)}")
print(f"Decision Tree: {evaluator.evaluate(dtTrainPredictions)}")
print(f"Random Forrest: {evaluator.evaluate(rfTrainPredictions)}")

# COMMAND ----------

def getConfusionMatrix(model, name = "Model Results"):
    '''
    Returns the Precision, Recall, and F1 from the Confusion Matrix of a model
        
        Inputs:
            model - model to evaluate
            name - name of the model (optional)
    '''
    results = model.select(['prediction', 'label'])
    predictionAndLabels = results.rdd
    metrics = MulticlassMetrics(predictionAndLabels)

    cm = metrics.confusionMatrix().toArray()
    precision = (cm[0][0])/(cm[0][0]+cm[1][0])
    recall = (cm[0][0])/(cm[0][0]+cm[0][1])
    f1 = (2*recall*precision)/(recall+precision)
    print(f"{name}: Precision - {precision}, Recall - {recall}, F1 - {f1}")

# COMMAND ----------

getConfusionMatrix(lrTrainPredictions, "Logistic Regression")
getConfusionMatrix(dtTrainPredictions, "Decision Tree")
getConfusionMatrix(rfTrainPredictions, "Random Forest")

# COMMAND ----------

# Setup the stream
sourceStream = spark.readStream.format("csv").option("header", True).schema(starsSchema).option("maxFilesPerTrigger", 1).load("dbfs:///FileStore/tables/starsTestData/")

# COMMAND ----------

starsStream = rfModel.transform(sourceStream).select(f.col("label"), f.col("probability"), f.col("prediction"))
display(starsStream)
