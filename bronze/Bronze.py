# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Bronze : Ingestion des Données
# MAGIC ## Objectif :
# MAGIC Collecter les données brutes depuis les sources externes (fichiers, API, bases de données, etc.).
# MAGIC Stocker les données dans un format brut sans transformation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration et Montage de la Source de Données
# MAGIC Définir l'emplacement des fichiers dans le conteneur Bronze.

# COMMAND ----------

storage_account_name = "miclea"
container_name = "bronze"
access_key = "xMG8bxTSXMF7Qb3S6AaVzINxitBCSOhYaPiHvmG3ToDbk2ajZc2UQ46VbuafW8StD41jSInil9SN+AStqRygfQ=="

mount_point = f"/mnt/{container_name}"

# Unmount if already mounted
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

# Mount the Azure Blob Storage container
dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point=mount_point,
    extra_configs={
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key
    }
)

# Verify the mount
print(f"Container '{container_name}' mounted at {mount_point}")
dbutils.fs.ls(mount_point)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lecture des Données : MtCO2_Emissions_over_time.csv
# MAGIC Charger et nettoyer les données sur les émissions de CO2.

# COMMAND ----------

# File location and type
file_location = "/mnt/bronze/MtCO2_Emissions_over_time.csv"
file_type = "csv"
delimiter = ","

# Read the raw CSV as an RDD and skip the first row
raw_rdd = spark.read.text(file_location).rdd.zipWithIndex() \
    .filter(lambda row_index: row_index[1] > 0) \
    .map(lambda row_index: row_index[0].value)

# Convert the cleaned RDD back to a DataFrame
raw_df = spark.read.csv(raw_rdd, header=True, inferSchema=True, sep=delimiter)

# Rename `_c0` to `Year`
renamed_df = raw_df.withColumnRenamed("_c0", "Year")

# Standardize column names
import re
standardized_columns = [re.sub(r"[^\w]", "_", col_name) for col_name in renamed_df.columns]
df_with_headers = renamed_df.toDF(*standardized_columns)

# Ensure consistent column types
from pyspark.sql.functions import col
cleaned_df = df_with_headers.select(
    col("Year").cast("STRING"),
    *[col(c).cast("DOUBLE").alias(c) for c in df_with_headers.columns if c != "Year"]
)

# Filter out rows where Year or all emissions are NULL
cleaned_df = cleaned_df.filter("Year IS NOT NULL")

# Save the cleaned data to the Silver layer
cleaned_df.write.format("parquet").mode("overwrite").save("/mnt/silver/emissions/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lecture des Données : Global_temperature_anomalies.csv
# MAGIC Charger et nettoyer les données sur les anomalies de température.

# COMMAND ----------

# File location and type
file_location = "/mnt/bronze/Gta.csv"
file_type = "csv"
delimiter = ","

# Read the raw CSV, skipping the first 4 lines
raw_rdd = spark.read.text(file_location).rdd.zipWithIndex() \
    .filter(lambda row_index: row_index[1] > 4) \
    .map(lambda row_index: row_index[0].value)

# Convert the cleaned RDD back to a DataFrame
raw_df = spark.read.csv(raw_rdd, header=True, inferSchema=True, sep=delimiter)

# Rename columns
renamed_df = raw_df.toDF("Date", "Anomaly")

# Extract Year and Month from `Date` (e.g., 198003)
from pyspark.sql.functions import substring
cleaned_temperature_df = renamed_df.withColumn("Year", substring("Date", 1, 4).cast("INT")) \
    .withColumn("Month", substring("Date", 5, 2).cast("INT")) \
    .drop("Date")

# Rename `Anomaly` for clarity
cleaned_temperature_df = cleaned_temperature_df.withColumnRenamed("Anomaly", "Temperature_Anomaly")

# Save the cleaned data to the Silver layer
cleaned_temperature_df.write.format("parquet").mode("overwrite").save("/mnt/silver/temperature_anomalies/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification des Données Nettoyées
# MAGIC Assurez-vous que les données sont correctement nettoyées et enregistrées.

# COMMAND ----------

# Verify CO2 emissions data
print("CO2 Emissions Data:")
display(spark.read.parquet("/mnt/silver/emissions/"))

# Verify temperature anomalies data
print("Temperature Anomalies Data:")
display(spark.read.parquet("/mnt/silver/temperature_anomalies/"))
