# Databricks notebook source
sc = spark.sparkContext

# COMMAND ----------

def getScores(x):
#     print(x)
    scores = []
    for i in range(len(x[0])):
        score = (x[0][i], x[1]/len(x[0]))
        scores.append(score)
    return scores

testList = (['b','c'], 0.25)
print(getScores(testList))

# COMMAND ----------

def sortScores(tup): 
    tup.sort(key = lambda x: x[1], reverse = True) 
    return tup 

# COMMAND ----------

# Test Set
iterations = 10

l1 = ["a b c", "b a a", "c b", "d a"]
test = sc.parallelize(l1)
split = test.map(lambda x: x.split(" "))
tuples = split.map(lambda x: (x[0], x[1:]))
tuples = tuples.flatMapValues(lambda x:x)
groups = tuples.groupByKey()
sortedGroups = groups.mapValues(lambda x: sorted(x))
links = sortedGroups.mapValues(lambda x: list(set(x)))
print("Initial links:", links.collect())

initialRankings = 1/links.count()
rankings = links.map(lambda x: (x[0], initialRankings))
print("Initial rankings:",rankings.collect())

for i in range(iterations):
    print("Iteration:", i)
    temp = links.join(rankings)
    print("Joined RDD", temp.collect())
    temp = temp.flatMapValues(getScores).map(lambda x: x[1])
    print("Neighbor contributions:", temp.collect())
    rankings = temp.reduceByKey(lambda x, y: x+y).persist()
    print("New rankings:", rankings.collect())

print("Final sorted rankings:")
finalRankings = rankings.collect()
sortedRankings = sortScores(finalRankings)
for i in range(len(sortedRankings)):
    print(sortedRankings[i][0], "has rank:", sortedRankings[i][1])

# COMMAND ----------

# lab3short

txtFile = "/FileStore/tables/lab3short/"
file = sc.textFile(txtFile)

iterations = 10

split = file.map(lambda x: x.split(" "))
tuples = split.map(lambda x: (x[0], x[1:]))
tuples = tuples.flatMapValues(lambda x:x)
groups = tuples.groupByKey()
sortedGroups = groups.mapValues(lambda x: sorted(x))
links = sortedGroups.mapValues(lambda x: list(set(x)))

initialRankings = 1/links.count()
rankings = links.map(lambda x: (x[0], initialRankings))

for i in range(iterations):
    temp = links.join(rankings)
    temp = temp.flatMapValues(getScores).map(lambda x: x[1])
    rankings = temp.reduceByKey(lambda x, y: x+y).persist()

print("Final sorted rankings:")
finalRankings = rankings.collect()
sortedRankings = sortScores(finalRankings)
for i in range(len(sortedRankings)):
    print(sortedRankings[i][0], "has rank:", sortedRankings[i][1])
