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

# MAGIC %sh mkdir /dbfs/tmp2

# COMMAND ----------

# MAGIC %fs mkdirs /tmp/my_cloud_dir

# COMMAND ----------

# MAGIC %fs mv /test_dbfs.txt /tmp2

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

# MAGIC %md this is a branch test

# COMMAND ----------

# MAGIC %md #Mount an Azure blob storage

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://dbs-mount@dbsmount.blob.core.windows.net",
  mount_point = "/mnt/mounttest",
  extra_configs = {"fs.azure.account.key.dbsmount.blob.core.windows.net":dbutils.secrets.get(scope = "databricks-scope", key = "mykey")})

# COMMAND ----------

df = spark.read.text("/mnt/mounttest/Az Region.txt")

# COMMAND ----------

df.head(5)

# COMMAND ----------

# MAGIC %fs ls /mnt/mounttest/

# COMMAND ----------

# MAGIC %md #adls-gen2-access-key

# COMMAND ----------

#Configure a storage account access key for authentication
spark.conf.set(
  "fs.azure.account.key.darabrickslake.dfs.core.windows.net", 
  dbutils.secrets.get(scope="keyvault-secrets", key="example-adls2-secret"))

# COMMAND ----------

#Create a storage container
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://createdby-databricksnotebook@darabrickslake.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

#Read Databricks Dataset IoT Devices JSON
df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")

# COMMAND ----------

#Write IoT Devices JSON
df.write.json("abfss://createdby-databricksnotebook@darabrickslake.dfs.core.windows.net/iot_devices.json")

# COMMAND ----------

#List filesystem
dbutils.fs.ls("abfss://createdby-databricksnotebook@darabrickslake.dfs.core.windows.net/")

# COMMAND ----------

#Read IoT Devices JSON from ADLS Gen2 filesystem
df2 = spark.read.json("abfss://createdby-databricksnotebook@darabrickslake.dfs.core.windows.net/iot_devices.json")
display(df2)

# COMMAND ----------

# MAGIC %md #Access ADLS using OAuth2.0

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "8ca022f0-1560-4dd2-b1f1-8317eab6ab8d",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="keyvault-secrets",key="client-secret"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://createdby-databricksnotebook@darabrickslake.dfs.core.windows.net/",
  mount_point = "/mnt/adlsoauth",
  extra_configs = configs)

# COMMAND ----------

df2 = spark.read.json("/mnt/adlsoauth/iot_devices.json")
display(df2)

# COMMAND ----------

dbutils.fs.ls("/mnt/adlsoauth")

# COMMAND ----------

# MAGIC %md #Drive node local file system trying

# COMMAND ----------

# MAGIC %sh ls logs

# COMMAND ----------

# MAGIC %sh cat logs/stderr

# COMMAND ----------

# MAGIC %sh ls /databricks/python/bin

# COMMAND ----------

# MAGIC %md #Cluster init scripts

# COMMAND ----------

#Create a DBFS directory you want to store the init script in. This example uses dbfs:/databricks/scripts.
dbutils.fs.mkdirs("dbfs:/databricks/scripts/")

# COMMAND ----------

#Create a script named postgresql-install.sh in that directory:
dbutils.fs.put("/databricks/scripts/postgresql-install.sh","""
#!/bin/bash
wget --quiet -O /mnt/driver-daemon/jars/postgresql-42.2.2.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.2/postgresql-42.2.2.jar""", True)

# COMMAND ----------

#Check that the script exists.
display(dbutils.fs.ls("dbfs:/databricks/scripts/postgresql-install.sh"))

# COMMAND ----------

# MAGIC %fs ls cluster-logs/1111-133045-rx27q9ov/init_scripts/1111-133045-rx27q9ov_10_139_64_6/

# COMMAND ----------

# read the file
with open("/dbfs/cluster-logs/1111-133045-rx27q9ov/init_scripts/1111-133045-rx27q9ov_10_139_64_6/20211120_092728_00_postgresql-install.sh.stderr.log", "r") as f_read:
  for line in f_read:
    print(line)

# COMMAND ----------

# MAGIC %md #External Hive Metastore

# COMMAND ----------

# MAGIC %sh
# MAGIC nc -vz mysqlserver13501649220.database.windows.net 1433

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.typesafe.config.ConfigFactory
# MAGIC val path = ConfigFactory.load().getString("java.io.tmpdir")
# MAGIC 
# MAGIC println(s"\nHive JARs are downloaded to the path: $path \n")

# COMMAND ----------

# MAGIC %sh cp /local_disk0/tmp /dbfs/hive_metastore_jar

# COMMAND ----------

# MAGIC %fs ls /hive_metastore_jar

# COMMAND ----------

# MAGIC %fs ls /cluster-logs/1111-133045-rx27q9ov/init_scripts/

# COMMAND ----------

#Create a script named postgresql-install.sh in that directory:
dbutils.fs.put("/databricks/scripts/copy_hive_meta_jar.sh","""
#!/bin/bash
%sh cp /dbfs/hive_metastore_jar /databricks/hive_metastore_jars/""", True)

# COMMAND ----------

# MAGIC %fs ls /databricks/scripts/

# COMMAND ----------

# MAGIC %md ## Init Script执行错误，查找init_log排故

# COMMAND ----------

# MAGIC %fs ls /cluster-logs/1123-073853-udks5dap/init_scripts/

# COMMAND ----------

# MAGIC %fs ls /cluster-logs/1123-073853-udks5dap/init_scripts/1123-073853-udks5dap_10_139_64_7/

# COMMAND ----------

# MAGIC %md ## Root Cause for the init script failure

# COMMAND ----------

with open ("/dbfs/cluster-logs/1123-073853-udks5dap/init_scripts/1123-073853-udks5dap_10_139_64_7/20211123_081615_00_copy_hive_meta_jar.sh.stderr.log", "r") as f_read:
    for line in f_read:
        print(line)

# COMMAND ----------

# MAGIC %md Root cause为：“cp: -r not specified; omitting directory '/dbfs/hive_metastore_jar'，shell脚本写错了，应该是cp -r ... ...，r待变recursive复制，不能遗漏-r
# MAGIC ”

# COMMAND ----------

# MAGIC %fs rm -r /databricks/scripts/copy_hive_meta_jar.sh

# COMMAND ----------

# MAGIC %fs ls /databricks/scripts

# COMMAND ----------

# MAGIC %md ##Solution

# COMMAND ----------

#Create a script named postgresql-install.sh in that directory:
dbutils.fs.put("/databricks/scripts/copy_hive_meta_jar.sh","""
#!/bin/bash
cp -r /dbfs/hive_metastore_jar /databricks/hive_metastore_jars/""", True)

# COMMAND ----------

# MAGIC %fs ls /databricks/scripts/

# COMMAND ----------

# MAGIC %md 然后再重新跑一边init script即可，发现Cluster可以成功Running了！

# COMMAND ----------

