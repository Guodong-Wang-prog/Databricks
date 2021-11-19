# Databricks notebook source
# MAGIC %md # Run Sprak Example

# COMMAND ----------

# MAGIC %python
# MAGIC blob_account_name = "azureopendatastorage"
# MAGIC blob_container_name = "citydatacontainer"
# MAGIC blob_relative_path = "Safety/Release/city=Seattle"
# MAGIC blob_sas_token = r""

# COMMAND ----------

# MAGIC %python
# MAGIC wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
# MAGIC spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
# MAGIC print('Remote blob path: ' + wasbs_path)

# COMMAND ----------

# MAGIC %python
# MAGIC df = spark.read.parquet(wasbs_path)
# MAGIC print('Register the DataFrame as a SQL temporary view: source')
# MAGIC df.createOrReplaceTempView('source')

# COMMAND ----------

# MAGIC %python
# MAGIC print('Displaying top 10 rows: ')
# MAGIC display(spark.sql('SELECT * FROM source LIMIT 10'))

# COMMAND ----------

# MAGIC %md # DBFS Trying

# COMMAND ----------

# MAGIC %fs ls /tmp

# COMMAND ----------

# MAGIC %sh ls

# COMMAND ----------

# MAGIC %fs mkdirs /tmp/my_cloud_dir

# COMMAND ----------

# MAGIC %fs ls /tmp/

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.put("/tmp/my_new_file", "This is a file in cloud storage.")

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/

# COMMAND ----------

# MAGIC %python
# MAGIC import os
# MAGIC os.listdir('/dbfs/tmp')

# COMMAND ----------

# MAGIC %sh ls conf

# COMMAND ----------

# MAGIC %fs ls file:/tmp

# COMMAND ----------

# MAGIC %sh ls /tmp

# COMMAND ----------

# MAGIC %python
# MAGIC df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/guodongwang@microsoft.com/export.csv")

# COMMAND ----------

# MAGIC %python
# MAGIC df1.show()

# COMMAND ----------

# MAGIC %fs ls /FileStore

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.mkdirs("/foobar/")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.put("/foobar/baz.txt", "Hello, World!")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.head("/foobar/baz.txt")

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls("/foobar"))

# COMMAND ----------

# MAGIC %python
# MAGIC #write a file to DBFS using Python file system APIs
# MAGIC with open("/dbfs/tmp/test_dbfs.txt", 'w') as f:
# MAGIC   f.write("Apache Spark is awesome!\n")
# MAGIC   f.write("End of example!")
# MAGIC 
# MAGIC # read the file
# MAGIC with open("/dbfs/tmp/test_dbfs.txt", "r") as f_read:
# MAGIC   for line in f_read:
# MAGIC     print(line)

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.io.Source
# MAGIC 
# MAGIC val filename = "/dbfs/tmp/test_dbfs.txt"
# MAGIC for (line <- Source.fromFile(filename).getLines()) {
# MAGIC   println(line)
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC df = spark.read.csv('dbfs:/export.csv')
# MAGIC df.show()

# COMMAND ----------

# MAGIC %python
# MAGIC displayHTML("<img src ='files/test.jpg/'>")

# COMMAND ----------

# MAGIC %md # Notebook Trying

# COMMAND ----------

# MAGIC %python
# MAGIC spark.version

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

# COMMAND ----------

# MAGIC %md
# MAGIC ![Databricks_logo](files/dababricks-logo-mobile.png)

# COMMAND ----------

# MAGIC %md
# MAGIC \\(c = \\pm\\sqrt{a^2 + b^2} \\)
# MAGIC 
# MAGIC \\(A{_i}{_j}=B{_i}{_j}\\)
# MAGIC 
# MAGIC $$c = \\pm\\sqrt{a^2 + b^2}$$
# MAGIC 
# MAGIC \\[A{_i}{_j}=B{_i}{_j}\\]

# COMMAND ----------

import pandas as pd
pdf = pd.DataFrame({'A':range(1000)})

# COMMAND ----------

pdf.apply

# COMMAND ----------

# MAGIC %md # Version control

# COMMAND ----------

# MAGIC %md this is a test