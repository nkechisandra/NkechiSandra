# Databricks notebook source
# MAGIC %md
# MAGIC You should remove the `raise` exceptions below and insert your code in their place. The cells which say `DO NOT CHANGE THE CONTENT OF THIS CELL` are there to help you, if they fail, it's probably an indication of the fact that your code is wrong. You should not change their content - if you change them to make them correspond to what your program is producing, you will still not get the marks.
# MAGIC 
# MAGIC If you encounter an error while running your notebook that doesn't appear to be connected to RDDs (such as missing `imp`), you should check that you've run the initialization cells since you've started your latest cluster.
# MAGIC 
# MAGIC Before you turn your solution in, make sure everything runs as expected. With an attached cluster, you should **Clear State and Results** (under the **Clear** dropdown menu) and then click on the **Run all** icon. This runs all cells in the notebook from new. You should only submit this notebook if all cells run.
# MAGIC 
# MAGIC This homework is to be completed on your own. By the act of following these instructions and handing your work in, it is deemed that you have read and understand the rules on plagiarism as written in your student handbook.

# COMMAND ----------

# MAGIC %md
# MAGIC We will use the historical World cup player dataset (the second cell of this notebook downloads this for you) which is in JSON format. The first two cells set the environment up for you, including downloading the file. The initial dataframe is also created for you, so your work starts when you start exploring the data in the three ways we have seen in lectures.

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
import sys
if not 'dbruntime.dbutils' in sys.modules.keys():
    import pyspark
    sc = pyspark.SparkContext()
    from pyspark.sql import SQLContext
    spark = SQLContext(sc)
    print("Unless you're grading this homework, you should be running this on Databricks.")

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
import urllib.request

player_json = "/FileStore/all-world-cup-players.json"
if 'dbruntime.dbutils' in sys.modules.keys():
    try:
        dbutils.fs.ls(file1)
    except:
        # Download to local /tmp
        urllib.request.urlretrieve("https://github.com/jokecamp/FootballData/raw/master/World%20Cups/all-world-cup-players.json", "/tmp/all-world-cup-players.json")
        # Copy to DBFS
        dbutils.fs.cp("file:/tmp/all-world-cup-players.json", player_json)

# COMMAND ----------

dbutils.fs.cp("file:/tmp/all-world-cup-players.json", player_json)

# COMMAND ----------

dbutils.fs.mv("file:/tmp/all-world-cup-players.json", player_json, True)

# COMMAND ----------

dbutils.fs.ls(player_json)

# COMMAND ----------

# MAGIC %md
# MAGIC The cell below reads the data into a dataframe named `playersDF`. If you look at the file, you'll see that it's not formed quite as Spark expects: it doesn't have a single line per json record. We therefore use the `multiline` option.

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
playersDF = spark.read.option("multiline","true").json(player_json)
assert playersDF.count() == 9443, "Something has gone wrong with the reading process"

# COMMAND ----------

playersDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC We will now explore three different ways to extract the same information from the data. 
# MAGIC 1. Via DataFrames directly
# MAGIC 2. Via Views
# MAGIC 3. Via RDDs
# MAGIC 
# MAGIC Let's start with the DataFrames. Use DataFrame operations to create a `teamNamesFromDF` DataFrame which contains all the team names from 2014 (only). (You may want to look at the DataFrame you have read in first.) The team names should only appear once in your resulting dataframe.

# COMMAND ----------

# MAGIC %md #####Explore Dataset Direct

# COMMAND ----------

playersDF = spark.read.option("multiline","true").json(player_json)

# COMMAND ----------

playersDF . printSchema ()

# COMMAND ----------

playersDF .rdd

# COMMAND ----------

# MAGIC %md #####Explore Dataset Via Views

# COMMAND ----------

playersDF.show()

# COMMAND ----------

# MAGIC %md #####Explore Dataset Via RDDs

# COMMAND ----------

playerRDD=playersDF.rdd

# COMMAND ----------

playerRDD.take(2)

# COMMAND ----------

playerRDD = playersDF.rdd
for row in playerRDD.take(2):
    print (row)


# COMMAND ----------

# Your answer should have the format
# teamNamesFromDF = ...
# YOUR CODE HERE
teamNamesFromDF=playersDF.select("Team").where("Year==2014").dropDuplicates()
teamNamesFromDF.count()

# COMMAND ----------

teamNamesFromDF.show()

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
from pyspark.sql import DataFrame
assert isinstance(teamNamesFromDF, DataFrame), "Your answer should be a dataframe"
assert teamNamesFromDF.count() == 32, "Unexpected number of teams"

# COMMAND ----------

# MAGIC %md
# MAGIC Now do the same via constructing a temporary view called `players` from the data (remember that you'll need to ensure that the program doesn't fail if you run it twice - i.e. if the view already exists), using a Spark sql query to extract the 2014 team names (without repeats), and naming the resulting DataFrame `teamNamesFromTable`.

# COMMAND ----------

# MAGIC %md #####Create Temporary view called 'players'

# COMMAND ----------

playersDF. createOrReplaceTempView ("players")

# COMMAND ----------

# Your answer should have the format
# teamNamesFromTable =
# YOUR CODE HERE
teamNamesFromTable = spark.sql ("SELECT team FROM players WHERE year==2014").dropDuplicates()

# COMMAND ----------

teamNamesFromTable.count()

# COMMAND ----------

teamNamesFromTable.show()

# COMMAND ----------

teamNamesFromTable.count()

# COMMAND ----------

teamNamesFromTable.show()

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
# Check the table was created
if 'dbruntime.dbutils' in sys.modules.keys():
    tableList = [t.name for t in spark.catalog.listTables()]
    assert "players" in tableList, "You have either not created your table or you have named it something other than players"
else:
    assert ('players' in spark.tableNames()), "You have either not created your table or you have named it something other than players"
    
assert teamNamesFromTable.count() == 32, "Unexpected number of teams"

# COMMAND ----------

# MAGIC %md
# MAGIC Your third implementation should go via RDDs: i.e. you'll need to create a (Row) RDD from the DataFrame data, perform `.map`, `.filter` etc operations to obtain an RDD with the same result using RDDs. Your resulting RDD should be named `teamNamesFromRDD`.

# COMMAND ----------

# MAGIC %md #####Create Row(RDD)

# COMMAND ----------

RowRDD=playersDF.rdd
RowRDD.count()


# COMMAND ----------

# Your answer should have the format
# teamNamesFromRDD = ...
# YOUR CODE HERE
teamNamesFromRDD=RowRDD.filter(lambda x: x[9]==2014).map(lambda line: line[8]).distinct()

# COMMAND ----------

teamNamesFromRDD.count()

# COMMAND ----------

teamNamesFromRDD.toDebugString()

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
from pyspark.rdd import RDD
assert isinstance(teamNamesFromRDD, RDD), "Your result should be an RDD"
assert teamNamesFromRDD.count() == 32, "Unexpected number of teams"

lineage = teamNamesFromRDD.toDebugString()
assert 'MapPartitionsRDD' in lineage.decode(), "Did you really manage to answer this question via RDDs without a map?"
