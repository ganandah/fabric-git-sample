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

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Load the CSV file into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/Sales Customer.csv")

# Optionally display first few rows
df.show(5)

# Save the DataFrame as a Delta table named customer_sales
df.write.format("delta").mode("overwrite").saveAsTable("dim_sales_customer")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Load the dim_city table into a Spark DataFrame
spark_df = spark.read.table("dim_city")

# Show summary statistics for numerical columns
spark_df.describe().show()

# Show additional descriptive statistics for all columns
from pyspark.sql.functions import countDistinct, col

summary = spark_df.summary()
summary.show()

# Show data types and null counts for each column
print("Schema:")
spark_df.printSchema()

print("Null Counts:")
spark_df.select([countDistinct(col(c)).alias(c + '_distinct') for c in spark_df.columns]).show()

spark_df.select([(col(c).isNull().cast("int").alias(c + "_nulls")) for c in spark_df.columns]).groupBy().sum().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%chat
# MAGIC Find any duplicates in dimension_customer table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

from pyspark.sql import functions as F

# Read the dimension_customer table
df = spark.read.table("dimension_customer")

# Define columns to check for duplicates (excluding technical columns)
dup_columns = [
    'WWICustomerID', 'Customer', 'BillToCustomer', 'Category',
    'BuyingGroup', 'PrimaryContact', 'PostalCode'
]

# Find duplicate rows based on these columns
duplicates = (
    df.groupBy(dup_columns)
      .count()
      .filter("count > 1")
)

print("Duplicate groups based on business columns:")
duplicates.show(truncate=False)

# If you want to see all duplicated records, join back:
df_dup = df.join(
    duplicates,
    on=dup_columns,
    how='inner'
).orderBy(dup_columns)
print("All duplicate rows:")
df_dup.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Load necessary tables
sales_df = spark.read.table("fact_sale")
city_df = spark.read.table("dim_city")

# Join fact_sale and dim_city on CityKey
# Ensure that 'CityKey' exists in both tables as a join key

joined_df = sales_df.join(city_df, sales_df.CityKey == city_df.CityKey, how="inner")

# Group by city name and sum the profit
profit_by_city = (
    joined_df.groupBy("City")
    .agg(F.sum("Profit").alias("total_profit"))
    .orderBy("City")
)

profit_by_city.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%chat
# MAGIC Suggest how we can build a machine learning model from fabric_demolh lakehouse.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%chat
# MAGIC 
# MAGIC Suggest how we can build machine learning model to Predict sales for next month (Regression)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df = spark.read.table("fact_sale")
date_df = spark.read.table("dim_date")

# Join to get calendar info
df = sales_df.join(date_df, sales_df.InvoiceDateKey == date_df.Date)

# Aggregate sales by year and month
from pyspark.sql.functions import year, month, sum as _sum

monthly_sales = (
    df.groupBy(year("Date").alias("year"), month("Date").alias("month"))
      .agg(_sum("TotalExcludingTax").alias("monthly_sales"))
      .orderBy("year", "month")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

monthly_sales.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
import pyspark.sql.functions as F

w = Window.orderBy("year", "month")
monthly_sales = monthly_sales.withColumn("prev_month_sales", F.lag("monthly_sales").over(w))
# The target is actual sales for the next month
monthly_sales = monthly_sales.withColumn("target_next_month_sales", F.lead("monthly_sales").over(w))
# Drop the last row where target is null
final_df = monthly_sales.dropna(subset=["target_next_month_sales"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Load fact_sale and dim_date tables
fact_df = spark.read.table("fact_sale")
date_df = spark.read.table("dim_date")

# Join fact_sale with dim_date to get calendar info
joined_df = fact_df.join(date_df, fact_df.InvoiceDateKey == date_df.Date, how="inner")

from pyspark.sql.functions import year, month, sum as _sum

# Group by year and month, sum the Profit
profit_by_month = (
    joined_df.groupBy(year("Date").alias("year"), month("Date").alias("month"))
    .agg(_sum("Profit").alias("total_profit"))
    .orderBy("year", "month")
)

profit_by_month.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%chat
# MAGIC troublehoot below code and fix
# MAGIC 
# MAGIC train_df = final_df.filter(final_df.year < 2001) & (final_df.month < 6) 
# MAGIC train_df.show()
# MAGIC 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

train_df = final_df.filter((final_df.year < 2001) & (final_df.month < 6))
train_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(labelCol="target_next_month_sales", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Test RMSE: {rmse}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%chat
# MAGIC 
# MAGIC how to use dataframe filter function to filter by 2 different column


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
