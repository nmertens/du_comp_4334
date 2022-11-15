# Databricks notebook source
# COMP 4334 Assignment 1
# Nickolas Mertens

from math import floor, sqrt

# COMMAND ----------

sc = spark.sparkContext

# COMMAND ----------

threshold = 0.75

# COMMAND ----------

files = sc.textFile("dbfs:///FileStore/tables/assignment1/").map(lambda x: x.split(","))
# print(files.collect())

# COMMAND ----------

tuples = files.map(lambda x: (x[0], x[1:]))
# print(tuples.collect())

# COMMAND ----------

def getGrid(x, threshold = threshold):
#     print(x)
    
    # initialize empty lists
    gridCell  = []
    gridCells = []
    
    # get the home gridCell
    for i in x[1]:
        cell = floor(float(i)/threshold)
        gridCell.append(cell)
    gridCells.append((tuple(gridCell), (x[0], x[1], 1))) # 1 identifies that this is the home cell for this point
    
    # only push in one direction
    adjCells = [(1,0), (1,-1), (0,-1), (-1,-1)] #right, lowerRight, lower, lowerLeft
    
    for element in adjCells:
        adjCell = tuple([gridCell[0] + element[0], gridCell[1] + element[1]])
        if not any(map(lambda ele: ele < 0, adjCell)):
            gridCells.append((adjCell, (x[0], x[1], 0))) # 0 identifies that this is a push cell
        
    return gridCells


# test = ('Pt11', ['0.68422', '0.734435'])
# res = getGrid(test, 0.75)
# print(res)

# COMMAND ----------

# grids = tuples.mapValues(getGrid)
grids = tuples.flatMap(lambda x: getGrid(x))
# print(grids.collect())
# test = grids.collect()
# for i in test:
#     print(i)

# COMMAND ----------

groupGrids = grids.groupByKey().mapValues(lambda x: list(x))
groupGrids = groupGrids.partitionBy(groupGrids.count())
# res = groupGrids.collect()
# for i in res:
#     print(i)

# print(groupGrids.glom().collect())
# print(groupGrids.getNumPartitions())

# COMMAND ----------

def getDistance(x, y):
#     print(x)
#     print(y)
    distance = sqrt((float(y[0]) - float(x[0]))**2 + (float(y[1]) - float(x[1]))**2)
    return distance

# testX = ['3.0474', '3.20142']
# testY = ['3.7311', '3.22135']

# print(getDistance(testX, testY))

# COMMAND ----------

def belowThreshold(x, threshold = threshold):
    return x <= threshold

# print(belowThreshold(dist, threshold = threshold))

# COMMAND ----------

def findPairs(x):
    closePairs = []
    if len(x[1]) > 1:
        for i in range(len(x[1])):
            for j in range(i+1, len(x[1])):
                a = x[1][i]
                b = x[1][j]
                if not((a[2] == 0) and (b[2] == 0)):
#                     print((a[2], b[2]))
                    dist = getDistance(a[1], b[1])
                    if belowThreshold(dist):
                        closePairs.append((a[0], b[0]))
    return closePairs

# COMMAND ----------

closePairs = groupGrids.flatMap(lambda x: findPairs(x))
print("Dist:", threshold)
print(closePairs.count())
print(closePairs.collect())
