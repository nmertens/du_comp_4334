# Databricks notebook source
## Prime Exercise ##
sc = spark.sparkContext
l1 = [x for x in range(100,10000)]
r1 = sc.parallelize(l1)

# COMMAND ----------

# define prime function
def isPrime(x):
    for y in range(2, x):
        if x%y == 0:
            return False
    return True

# print the total number of prime numbers
print(r1.filter(isPrime).count())

# COMMAND ----------

## Celcius Exercise ##
import random
l2 = [random.randrange(0, 100) for x in range(1000)]
r2 = sc.parallelize(l2)

# COMMAND ----------

# Example using function
# define the celcius conversion function
# def toCelcius(x):
#     return ((x-32)*5)/9

# r2Celcius = r2.map(toCelcius) # map the function

# Example using lambda
r2Celcius = r2.map(lambda x: ((x-32)*5)/9)
r2CelciusAF = r2Celcius.filter(lambda x: x > 0).persist() # AF: Above Freezing

# print the average temperature of all values above freezing
print(round(r2CelciusAF.reduce(lambda x, y: x+y)/r2CelciusAF.count(),2))

# COMMAND ----------

# Treating the problem with Pair RDDs
r2PairRDD = r2CelciusAF.map(lambda x: (x,1))
r2Sum = r2PairRDD.reduce(lambda x, y: (x[0]+y[0], x[1]+y[1]))
r2Avg = r2Sum[0]/r2Sum[1]
print(r2Avg)
