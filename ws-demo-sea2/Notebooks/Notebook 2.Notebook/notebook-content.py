# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7c807647-f014-431d-aeb1-c995d23c4826",
# META       "default_lakehouse_name": "demo_lh",
# META       "default_lakehouse_workspace_id": "f0bae9f7-35d7-42ad-b24d-31286c19623c",
# META       "known_lakehouses": [
# META         {
# META           "id": "7c807647-f014-431d-aeb1-c995d23c4826"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/PoC_Dataset_for_Machine_Learning_Platform_2025.csv")
# df now is a Spark DataFrame containing CSV data from "Files/PoC_Dataset_for_Machine_Learning_Platform_2025.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM demo_lh.poc_data LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM demo_lh.poc_data LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
