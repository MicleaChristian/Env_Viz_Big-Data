# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/MtCO2_Emissions_over_time-1.csv"
file_type = "csv"

# CSV options
delimiter = ","

# Load the raw CSV as an RDD and skip the first row
raw_rdd = spark.read.text(file_location).rdd.zipWithIndex().filter(lambda row_index: row_index[1] > 0).map(lambda row_index: row_index[0].value)

# Convert the cleaned RDD back to a DataFrame
raw_df = spark.read.csv(raw_rdd, header=True, inferSchema=True, sep=delimiter)

# Rename `_c0` to `Year`
renamed_df = raw_df.withColumnRenamed("_c0", "Year")

# Display the raw data to verify
display(renamed_df)

# COMMAND ----------

from pyspark.sql.functions import col
import re

# Standardize column names
standardized_columns = [re.sub(r"[^\w]", "_", col_name) for col_name in renamed_df.columns]
df_with_headers = renamed_df.toDF(*standardized_columns)

# Display the updated DataFrame
display(df_with_headers)

# COMMAND ----------

from pyspark.sql.functions import col

# Ensure consistent column types
cleaned_df = df_with_headers.select(
    col("Year").cast("STRING"),  # Keep Year as string
    *[col(c).cast("DOUBLE").alias(c) for c in df_with_headers.columns if c != "Year"]  # Cast emissions to DOUBLE
)

# Filter out rows where Year or all emissions are NULL (if needed)
cleaned_df = cleaned_df.filter("Year IS NOT NULL")

# Display the cleaned wide-format DataFrame
display(cleaned_df)

# COMMAND ----------

# Save the cleaned data to the silver layer
cleaned_emissions.write.format("parquet").mode("overwrite").save("/mnt/silver/emissions/")

# (Optional) Register the cleaned data as a SQL table for further queries
cleaned_emissions.createOrReplaceTempView("silver_emissions")
