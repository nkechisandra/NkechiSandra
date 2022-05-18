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
# MAGIC # Scrubbing data
# MAGIC 
# MAGIC A common part of the ETL process is data scrubbing. This homework asks you to process data in order to get it into a standardized format for later processing.
# MAGIC 
# MAGIC The file `devicestatus.txt.zip` is available from Blackboard. This file contains data collected from mobile devices on a network, including device ID, current status, location and so on. Because the company previously acquired other mobile provider's networks, the data from different subnetworks has a different format. Note that the records in this file have different field delimiters: some use commas, some use pipes (|), and so on. 
# MAGIC 
# MAGIC This notebook will guide you through the steps of scrubbing this dataset. Follow the instructions carefully. In general, every time you execute a step, there is a check that tests whether you carried out that step correctly. You should not move on until a step is passing the test. All your processing should be done in this notebook. It is assumed that you have uncompressed the `devicestatus.txt` file and have placed it in `/FileStore/` on DBFS using the steps described in the labs this week.
# MAGIC 
# MAGIC The first two cells perform some checks: first one checks that you are running this notebook on Databricks, and the second cell ensures that your file is starting off in `/FileStore/devicestatus.txt` You should not (try to) change either of these cells.

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

dbutils.fs.cp('dbfs:/FileStore/tables/devicestatus.zip', 'file:/tmp/')

# COMMAND ----------

ls /tmp

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -d /tmp/ /tmp/devicestatus.zip

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/devicestatus.txt")

# COMMAND ----------

dbutils.fs.mv("file:/tmp/devicestatus.txt", "/FileStore/devicestatus.txt")

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
import sys
if not 'dbruntime.dbutils' in sys.modules.keys():
    import pyspark
    sc = pyspark.SparkContext()
    print("Unless you're grading this homework, you should be running this on Databricks.")

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
if 'dbruntime.dbutils' in sys.modules.keys():
    try:
        dbutils.fs.ls("/FileStore/devicestatus.txt")
    except:
        assert False, "It is assumed that you've put your (unzipped) devicestatus.txt file in the DBFS /FileStore"
else:    
    import os
    assert os.path.exists("/FileStore/devicestatus.txt") == True, "It is assumed that you've put your (unziped) devicestatus.txt file in the DBFS /FileStore"

# COMMAND ----------

# MAGIC %md
# MAGIC Load the `devicestatus.txt` dataset into a variable called `myRDD`.

# COMMAND ----------

# Read the devicestatus.txt file into an RDD. Your answer should have the following format (without comment tag)
# myRDD = ...
# YOUR CODE HERE
myRDD = sc.textFile("dbfs:/FileStore/devicestatus.txt")
myRDD.count()

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
assert myRDD.count() == 459540, "It doesn't look like you've read in your data correctly"

# COMMAND ----------

# MAGIC %md
# MAGIC Determine which delimiter(s) to use and divide up each line of the RDD into a list of its fields. Your result should be in a variale called `splitRDD`.

# COMMAND ----------

# The format of your answer should be
# splitRDD = ...
# YOUR CODE HERE
SAN = myRDD.map(lambda x: x.replace('|',','))
SAN1 = SAN.map(lambda x: x.replace('/',','))
splitRDD = SAN1.map(lambda x: x.split(","))


# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
assert splitRDD.sortBy(lambda x: x[0]).take(2) == [[u'2014-03-15:10:10:20',
  u'Sorrento F41L',
  u'8cc3b47e-bd01-4482-b500-28f2342679af',
  u'7',
  u'24',
  u'39',
  u'enabled',
  u'disabled',
  u'connected',
  u'55',
  u'67',
  u'12',
  u'33.6894754264',
  u'-117.543308253'],
 [u'2014-03-15:10:10:20',
  u'MeeToo 1.0',
  u'ef8c7564-0a1a-4650-a655-c8bbd5f8f943',
  u'0',
  u'31',
  u'63',
  u'70',
  u'39',
  u'27',
  u'enabled',
  u'enabled',
  u'enabled',
  u'37.4321088904',
  u'-121.485029632']], "Unexpected entries in the first couple of lines"

# COMMAND ----------

# MAGIC %md
# MAGIC Filter out any records which do not parse correctly. The records which do have the correct number of fields should be placed in an RDD named `filteredRDD`.

# COMMAND ----------

# The format of your answer should be
# filteredRDD = ...
# YOUR CODE HERE
filteredRDD = splitRDD.filter(lambda f: f[0]) 

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
assert filteredRDD.count() == 459540, "You may have forgotten to account for some delimiters"

# COMMAND ----------

# MAGIC %md
# MAGIC Extract the date (first field), model (second field), device ID (third field), latitude and longitude (13th and 14th fields respectively). Each record's list should therefore reduced to a list of the 5 fields only. The output should be placed in a variable named `extractedRDD`.

# COMMAND ----------

# The format of your solution should be
# extractedRDD = ...
# YOUR CODE HERE
extractedRDD = filteredRDD.map(lambda field: [field[0],field[1],field[2],field[12],field[13]])

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
assert extractedRDD.sortBy(lambda x: x[0]).take(2) == [[u'2014-03-15:10:10:20',
  u'Sorrento F41L',
  u'8cc3b47e-bd01-4482-b500-28f2342679af',
  u'33.6894754264',
  u'-117.543308253'],
 [u'2014-03-15:10:10:20',
  u'MeeToo 1.0',
  u'ef8c7564-0a1a-4650-a655-c8bbd5f8f943',
  u'37.4321088904',
  u'-121.485029632']], "There seems to be some discrepancy between your answer and the expected answer"

# COMMAND ----------

# MAGIC %md
# MAGIC The second field contains the device manufacturer and model name (e.g. Ronin S2). Split this field by spaces to separate the manufacturer from the model (i.e. manufacturer Ronin, model S2). Each resulting record should therefore now contain a list containing: date, manufacturer, model, device ID, latitude and longitude. The new RDD should be placed in a variable named `separatedRDD`.

# COMMAND ----------

# The format of your solution should be
# separatedRDD = ...
# YOUR CODE HERE
SAN2 = SAN1.map(lambda field: field.replace(" ",","))
SAN3 = SAN2.map(lambda field: field.split(","))
separatedRDD = SAN3.map(lambda field: [field[0],field[1],field[2],field[3],field[13],field[14]])

# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
assert separatedRDD.sortBy(lambda x: x[0]).take(2) == [[u'2014-03-15:10:10:20',
  u'Sorrento',
  u'F41L',
  u'8cc3b47e-bd01-4482-b500-28f2342679af',
  u'33.6894754264',
  u'-117.543308253'],
 [u'2014-03-15:10:10:20',
  u'MeeToo',
  u'1.0',
  u'ef8c7564-0a1a-4650-a655-c8bbd5f8f943',
  u'37.4321088904',
  u'-121.485029632']], "There is some discrepancy between your answer and the expected answer"

# Remove the output directory for the following section if it already exists
if 'dbruntime.dbutils' in sys.modules.keys():
    try:
        dbutils.fs.ls("/FileStore")
        dbutils.fs.ls("/FileStore/devicestatus_etl/")
        dbutils.fs.rm("/FileStore/devicestatus_etl", True)
    except:
        # Directory is not there yet
        pass
else:
    if os.path.exists("/FileStore/devicestatus_etl/"):
        import shutil
        shutil.rmtree("/FileStore/devicestatus_etl/")

# COMMAND ----------

# MAGIC %md
# MAGIC Save the extracted data to comma delimited text files in the `/FileStore/devicestatus_etl` directory on DBFS.

# COMMAND ----------

# YOUR CODE HERE
SANdata = separatedRDD.map(lambda a: ",".join(str(a) for a in a))
SANdata.saveAsTextFile("/FileStore/devicestatus_etl/")


# COMMAND ----------

# DO NOT CHANGE THE CONTENT OF THIS CELL
if 'dbruntime.dbutils' in sys.modules.keys():
    assert dbutils.fs.ls("/FileStore/devicestatus_etl"), "You don't appear to have created the required output directory"
    assert dbutils.fs.ls("/FileStore/devicestatus_etl/part-00000"), "The output doesn't appear to be as expected"
    assert dbutils.fs.head("/FileStore/devicestatus_etl/part-00000", 99) == "2014-03-15:10:10:20,Sorrento,F41L,8cc3b47e-bd01-4482-b500-28f2342679af,33.6894754264,-117.543308253", "Expecting different output to that produced"
else:
    assert os.path.exists("/FileStore/devicestatus_etl"), "You don't appear to have created the required output directory"
    assert os.path.exists("/FileStore/devicestatus_etl/part-00000"), "The output doesn't appear to be as expected"
    
    # Check contents of the first line
    with open("/FileStore/devicestatus_etl/part-00000") as f:
        first_line = f.readline().strip()
        assert first_line == "2014-03-15:10:10:20,Sorrento,F41L,8cc3b47e-bd01-4482-b500-28f2342679af,33.6894754264,-117.543308253", "Expecting different output to that produced"
    
# Count the number of lines in the second part
myRDD = sc.textFile("/FileStore/devicestatus_etl/part-00001")
assert myRDD.count() == 229802, "Unexpected number of lines in part-00001"
