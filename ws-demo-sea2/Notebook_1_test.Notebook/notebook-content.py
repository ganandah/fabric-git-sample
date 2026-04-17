# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0d4a4bf0-50e6-4483-9b9b-37bbaff6e5dc",
# META       "default_lakehouse_name": "fabric_demolh",
# META       "default_lakehouse_workspace_id": "f0bae9f7-35d7-42ad-b24d-31286c19623c",
# META       "known_lakehouses": [
# META         {
# META           "id": "0d4a4bf0-50e6-4483-9b9b-37bbaff6e5dc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM fabric_demolh.dim_city LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
