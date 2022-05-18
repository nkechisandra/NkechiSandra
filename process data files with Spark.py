# Databricks notebook source
# MAGIC %md #####In this exercise you will parse a set of activation records in JSON format to extract the account numbers and model names.
# MAGIC Spark is commonly used for ETL (Extract/Transform/Load) operations. Sometimes data is stored in line-oriented records,
# MAGIC like the web logs in the previous exercise, but sometimes the data is in a multi-line format that must be processed as a whole
# MAGIC file. In this exercise you will practice working with file-based instead of line-based formats.

# COMMAND ----------

# MAGIC %md #####Put your data on Databricks
# MAGIC Download the activations.tgz file from Blackboard and unpack with tar xvzf activations.tgz. Review the data. Each
# MAGIC JSON file contains data for all the devices activated by customers during a specific month.
# MAGIC 1. Download the json activations.zip file from Blackboard. This will download it to the machine you’re using. To get
# MAGIC the zip file onto Databricks, we’ll use the Databricks user interface (UI).
# MAGIC 2. Log into Databricks and ensure you are on the home screen – this is the page which says “Data Science & Engineering”.
# MAGIC (You can get to this screen by clicking the topmost icon on the left.)
# MAGIC 3. There should be three columns, the middle of which is titled “Data Import”. Click on “Browse files” below this section
# MAGIC description and select the json activations.zip file you downloaded to your system. Wait for the file to upload, but
# MAGIC do not click on anything else on that page after the upload finishes.

# COMMAND ----------

# MAGIC %md #####By default, the UI uploads to /FileStore/tables/. Back in a notebook, use the dbutils.fs.ls command to check
# MAGIC that the file has been uploaded.

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

# MAGIC %md #####We need to extract this zip archive. Since the dbutils toolkit doesn’t provide an unzip command, you need to copy the
# MAGIC file to the driver node, extract there using a shell command, and put the extracted contents back into DBFS. So, first
# MAGIC copy the json activations.zip from DBFS to the /tmp directory on your driver node using dbutils and verify the
# MAGIC file has been copied.

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/json_activations.zip","file:/tmp/")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/

# COMMAND ----------

# MAGIC %md #####Use the UNIX command unzip to extract the contents. I.e. assuming success in Point 5, you should be able to run

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -d /tmp/ /tmp/json_activations.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/

# COMMAND ----------

ls /tmp/activations

# COMMAND ----------

# MAGIC %md #####Create a DBFS directory activations in the /FileStore/ directory using the dbutils.fs.mkdirs command.

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/activations")

# COMMAND ----------

# MAGIC %md #####Now you need to move (using the dbutils.fs.mv command) the contents of the local /tmp/activations directory
# MAGIC into your newly created DBFS /FileStore/activations directory so you can perform some Spark processing. If you
# MAGIC get an error when trying to use this command, you should look up its details so you can address the problem. Check
# MAGIC the contents of /FileStore/activations and confirm it contains the expected files.

# COMMAND ----------

dbutils.fs.mv("file:/tmp/activations",  "/FileStore/activations", True)

# COMMAND ----------

dbutils.fs.ls("/FileStore/activations/")


# COMMAND ----------

# MAGIC %md #####Use the dbutils.fs.head command to view the format of one of the files. Although perhaps not formatted as well, it
# MAGIC should look something like this:

# COMMAND ----------

dbutils.fs.head("/FileStore/activations/2008-10.json")

# COMMAND ----------

# MAGIC %md ##### Use wholeTextFiles to create an RDD from the activations dataset. The resulting RDD will consist of tuples, in
# MAGIC which the first value is the name of the file, and the second value is the contents of the file (JSON) as a string.

# COMMAND ----------

myrdd1 = sc.wholeTextFiles("/FileStore/activations/")

# COMMAND ----------

myrdd1.take(1) 

# COMMAND ----------

# MAGIC %md ##### Each JSON file can contain many activation records; map the contents of each file to a collection of JSON records.
# MAGIC Take each JSON string, parse it, and return a collection of JSON records; map each record to a separate RDD element. 

# COMMAND ----------

import json
myrdd2 = myrdd1.map(lambda s: json.loads(s[1]))
myrdd3 = myrdd2.flatMap(lambda x: x["activations"]["activation"])

# COMMAND ----------

myrdd2.take(1)

# COMMAND ----------

myrdd3.take(2)

# COMMAND ----------

# MAGIC %md ##### Map each activation record to a string in the format account-number:model

# COMMAND ----------

myrdd4 = myrdd3.map(lambda x: x["account-number"] + ":" + x["model"])

# COMMAND ----------

myrdd4.take(1)

# COMMAND ----------

myrdd4.saveAsTextFile("/FileStore/account-models")

# COMMAND ----------

myrdd4.saveAsTextFile("/FileStore/account-models")

# COMMAND ----------

myrdd4.take(1)

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/accounts.zip","file:/tmp/")

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -d /tmp/ /tmp/accounts.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/

# COMMAND ----------

ls /tmp/accounts

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/accounts")

# COMMAND ----------

dbutils.fs.mv("file:/tmp/accounts",  "/FileStore/accounts", True)

# COMMAND ----------

dbutils.fs.ls("/FileStore/accounts/")

# COMMAND ----------

dbutils.fs.head("/FileStore/accounts/part-m-00000")
