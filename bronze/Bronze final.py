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
container_name = "silver"
access_key = "xMG8bxTSXMF7Qb3S6AaVzINxitBCSOhYaPiHvmG3ToDbk2ajZc2UQ46VbuafW8StD41jSInil9SN+AStqRygfQ=="

mount_point = f"/mnt/{container_name}"

if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point=mount_point,
    extra_configs={
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key
    }
)

# Verify the mount
dbutils.fs.ls(mount_point)

# Storage account details
storage_account_name = "miclea"  # Your storage account name
container_name = "bronze"       # Container name to mount (e.g., bronze, silver, or gold)
access_key = "xMG8bxTSXMF7Qb3S6AaVzINxitBCSOhYaPiHvmG3ToDbk2ajZc2UQ46VbuafW8StD41jSInil9SN+AStqRygfQ=="  # Your storage access key

# Mount point in Databricks
mount_point = f"/mnt/{container_name}"  # This will be the accessible path in Databricks

# Unmount the container if it's already mounted (to avoid conflicts)
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

# List the contents of the mounted container to verify successful access
dbutils.fs.ls(mount_point)
# File location and type
file_location = "/mnt/bronze/MtCO2_Emissions_over_time.csv"  # Path to the file in the bronze layer
file_type = "csv"

# CSV options
delimiter = ","

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lecture des Données Brutes
# MAGIC Charger les données brutes et vérifier la structure.

# COMMAND ----------

# Load the raw CSV as an RDD and skip the first row
raw_rdd = spark.read.text(file_location).rdd.zipWithIndex() \
    .filter(lambda row_index: row_index[1] > 0) \
    .map(lambda row_index: row_index[0].value)

# Convert the cleaned RDD back to a DataFrame
raw_df = spark.read.csv(raw_rdd, header=True, inferSchema=True, sep=delimiter)

# Rename `_c0` to `Year`
renamed_df = raw_df.withColumnRenamed("_c0", "Year")

# Display the raw data to verify
display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardisation des Noms de Colonnes
# MAGIC Nettoyer et standardiser les noms des colonnes pour éviter les conflits.

# COMMAND ----------

from pyspark.sql.functions import col
import re

# Standardize column names
standardized_columns = [re.sub(r"[^\w]", "_", col_name) for col_name in renamed_df.columns]
df_with_headers = renamed_df.toDF(*standardized_columns)

# Display the updated DataFrame
display(df_with_headers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nettoyage et Vérification des Types de Données
# MAGIC Assurer la cohérence des types de données.

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Sauvegarde des Données dans la Couche Silver
# MAGIC Sauvegarder les données nettoyées pour les transformations futures.

# COMMAND ----------

# Save the cleaned data to the silver layer
cleaned_df.write.format("parquet").mode("overwrite").save("/mnt/silver/emissions/")

# (Optional) Register the cleaned data as a SQL table for further queries
cleaned_df.createOrReplaceTempView("silver_emissions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification des Résultats
# MAGIC Assurez-vous que les données sont correctement enregistrées dans la couche Silver.

# COMMAND ----------

# Verify saved data
display(spark.read.parquet("/mnt/silver/emissions/"))
