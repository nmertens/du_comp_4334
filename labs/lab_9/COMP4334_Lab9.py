# Databricks notebook source
# Nickolas Mertens
# COMP 4334 Lab 9

from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as f

from graphframes import *

# COMMAND ----------

# Load in routes data set
routesPath = "dbfs:///FileStore/tables/routes.csv"
routes = spark.read.format("csv").option("header", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(routesPath)

# COMMAND ----------

# modify data set to get important information
routes = routes.select(f.col("source airport").alias("sourceAirport"), f.col("destination apirport").alias("destinationAirport")).distinct().persist()
# print(routes.count())
# print(routes.show())

# COMMAND ----------

# Get the schema
airportsSchema = StructType( \
                        [StructField('airportID', StringType(), True), \
                         StructField('name', StringType(), True), \
                         StructField('city', StringType(), True), \
                         StructField('country', StringType(), True), \
                         StructField('IATA', StringType(), True), \
                         StructField('ICAO', StringType(), True), \
                         StructField('Lat', StringType(), True), \
                         StructField('Long', StringType(), True), \
                         StructField('Alt', StringType(), True), \
                         StructField('timeZone', StringType(), True), \
                         StructField('DST', StringType(), True), \
                         StructField('databaseTimeZone', StringType(), True), \
                         StructField('type', StringType(), True), \
                         StructField('source', StringType(), True), \
                        ])

# Load in airports data set
airportsPath = "dbfs:///FileStore/tables/airports.csv"
airports = spark.read.format("csv").option("header", False).schema(airportsSchema).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(airportsPath)

# COMMAND ----------

# Modify airports data set to get important information
airports = airports.select(f.col("country"), f.col("IATA")).persist()
# airports.show()

# COMMAND ----------

# Get the edges DataFrame
edges = routes.join(airports, routes.sourceAirport == airports.IATA, how = "left")\
              .filter(f.col("country") == "United States")\
              .select(f.col("sourceAirport"), f.col("destinationAirport"))\
              .join(airports, routes.destinationAirport == airports.IATA, how = "left")\
              .filter(f.col("country") == "United States")\
              .select(f.col("sourceAirport").alias("src"), f.col("destinationAirport").alias("dst"))\
              .persist()
# edges.show()

# COMMAND ----------

# Get the vertices DataFrame
srcVerts = edges.select(f.col("src").alias("id")).distinct().persist()
dstVerts = edges.select(f.col("dst").alias("id")).distinct().persist()
vertices = srcVerts.union(dstVerts).distinct().persist()

# COMMAND ----------

# Make the graphframe
g = GraphFrame(vertices, edges)

# COMMAND ----------

# Print out the number of vertices and edges
print("Number of US airport:", g.vertices.count())
print("Number of US to US routes:", g.edges.count())

# COMMAND ----------

# Get routes that fly to Denver but not from Denver
toDenver = g.find("(a)-[]->(b); !(b)-[]->(a)")\
            .filter("b.id = 'DEN'")\
            .select(f.col("a").alias("IATA"))\
            .persist()
# toDenver.show()

# COMMAND ----------

# Get routes that fly from Denver but not to Denver
fromDenver = g.find("(a)-[]->(b); !(b)-[]->(a)")\
              .filter("a.id = 'DEN'")\
              .select(f.col("b").alias("IATA"))\
              .persist()
# fromDenver.show()

# COMMAND ----------

# No round trip to Denver
Denver = fromDenver.union(toDenver)
print("Airports with no direct roundtrip to or from DEN:")
Denver.show()

# COMMAND ----------

# Get the shortest path and explode the distances column to separate out values
results = g.shortestPaths(landmarks = ["DEN"]).select(f.col("id"), f.explode(f.col("distances"))).persist()

# COMMAND ----------

# show the results
print("Airports that take 4 or more flights to get to DEN:")
results.filter(f.col("value") > 3).select(f.col("id").alias("IATA"), f.col("value").alias("Hops")).orderBy(f.col("Hops").asc(), f.col("IATA").asc()).show()
