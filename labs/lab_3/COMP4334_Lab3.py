# Databricks notebook source
fileList = []
for i in range(len(dbutils.fs.ls("/FileStore/tables/")):
    fileList.append(dbutils.fs.ls("/FileStore/tables/")[i][1])

for i in fileList:
    if (i.startswith('full')) & (i.endswith('.txt')):
        dbutils.fs.mv(f"/FileStore/tables/{i}", f"/FileStore/tables/lab3full/{i}")
    elif (i.startswith('short')) & (i.endswith('.txt')):
        dbutils.fs.mv(f"/FileStore/tables/{i}", f"/FileStore/tables/lab3short/{i}")

# COMMAND ----------

sc = spark.sparkContext

# COMMAND ----------

# lab3short

txtFile = "/FileStore/tables/lab3short/"
file = sc.textFile(txtFile)
# print(file.collect())

urlList = file.map(lambda x: x.split(" "))
# print(urlList.collect())

def referenceTuples(x):
#     print(x)
    newX = []
    for i in range(1, len(x)):
        newX.append((x[i],x[0]))
    return newX

urlTuples = urlList.flatMap(referenceTuples)
# print(urlTuples.collect())

urlReferences = urlTuples.groupByKey()
urlReferences = urlReferences.mapValues(list).mapValues(lambda x: sorted(x))
print(urlReferences.sortByKey().collect())

# COMMAND ----------

# lab3full

txtFullFile = "/FileStore/tables/lab3full/"
fullFile = sc.textFile(txtFullFile)
# print(file.collect())

urlFullList = fullFile.map(lambda x: x.split(" "))
# print(urlList.collect())

urlFullTuples = urlFullList.flatMap(referenceTuples)
# print(urlTuples.collect())

urlFullReferences = urlFullTuples.groupByKey()
urlFullReferences = urlFullReferences.mapValues(list).mapValues(lambda x: sorted(x))
urlFullReferences = urlFullReferences.sortByKey()
print(urlFullReferences.count())
print(urlFullReferences.take(10))
