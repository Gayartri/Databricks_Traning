# Databricks notebook source
select * from gayatri.circuit where location ='Abu Dhabi'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from gayatri.circuit

# COMMAND ----------

df1.write.saveAsTable("Gayatri.circuit")

# COMMAND ----------

df1.write.mode("overwrite").parquet("dbfs:/FileStore/tables/output/Gayatri/Circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists Gayatri

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.parquet("dbfs:/FileStore/tables/output/Gayatri/Circuit")


# COMMAND ----------

df.write.parquet("dbfs:/FileStore/tables/output/Gayatri/Circuit")

# COMMAND ----------

df.withColumn("name",upper("name")).display()

# COMMAND ----------

df.withColumn("Current_time",current_date()).display()

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.drop("url")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1=df.toDF(*new_coulmns)

# COMMAND ----------

new_coulmns=['circuit_Id',
 'circuit_Ref',
 'name',
 'location',
 'country',
 'latitude',
 'longtitude',
 'altitude',
 'url']

# COMMAND ----------

df.columns

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_Id").withColumnRenamed("circuitRef","circuit_Ref").display()

# COMMAND ----------

df.select(concat("location",lit(" "),"country").alias("loc&country")).display()


# COMMAND ----------

df.select(concat("Location"," ","Country")),display()

# COMMAND ----------

df.select("circuitId",col("circuitRef"),df.name,df["location"]).display()

# COMMAND ----------

df.select(col('circuitId').alias("circuit_Id"),'circuitRef').show()

# COMMAND ----------

df.select('circuitId','circuitRef','name').show()

# COMMAND ----------

df.select('*').display()''

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

help(df.select)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Extract Data 

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv",inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.show()

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")
