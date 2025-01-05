# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Silver: Transformation des Données
# MAGIC ## Objectif :
# MAGIC Nettoyer, structurer et transformer les données extraites de la couche Bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration et Montage de la Source de Données
# MAGIC Définir l'emplacement des fichiers dans le conteneur Silver.

# COMMAND ----------

# Azure Storage account details

storage_account_name = "miclea"
container_name = "gold"
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

storage_account_name = "miclea"
container_name = "silver"
access_key = "xMG8bxTSXMF7Qb3S6AaVzINxitBCSOhYaPiHvmG3ToDbk2ajZc2UQ46VbuafW8StD41jSInil9SN+AStqRygfQ=="

mount_point = f"/mnt/{container_name}"

# Unmount the container if already mounted
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
print(f"Container '{container_name}' mounted at {mount_point}")
dbutils.fs.ls(mount_point)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lecture des Données de la Couche Silver
# MAGIC Charger les fichiers Parquet de la couche Silver.

# COMMAND ----------

# Load the cleaned Parquet data from the silver layer
silver_df = spark.read.parquet("/mnt/silver/emissions/")

# Display the loaded data
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification et Nettoyage des Données
# MAGIC Assurez-vous que les données sont complètes, propres, et valides.

# COMMAND ----------

from pyspark.sql.functions import col, count, when

# Check for missing or null values
missing_values = silver_df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in silver_df.columns]
)
print("Missing values per column:")
display(missing_values)

# Fill null values with 0 before summation
cleaned_silver_df = silver_df.fillna(0)

# Remove rows where all emissions are zero (optional but recommended)
non_zero_columns = [c for c in cleaned_silver_df.columns if c != "Year"]
cleaned_silver_df = cleaned_silver_df.filter(
    reduce(lambda x, y: x | y, [col(c) != 0 for c in non_zero_columns])
)

# Display the cleaned DataFrame
display(cleaned_silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structuration et Transformation des Données
# MAGIC Ajoutez des transformations ou enrichissez les données, si nécessaire.

# COMMAND ----------

from pyspark.sql.functions import col
from functools import reduce

# Calculate Total Emissions Per Year
columns_to_sum = [col(c) for c in cleaned_silver_df.columns if c != "Year"]
transformed_df = cleaned_silver_df.withColumn(
    "Total_Emissions",
    reduce(lambda x, y: x + y, columns_to_sum)  # Sum all columns except 'Year'
)

# Display the transformed DataFrame
display(transformed_df)

# Verify if Total_Emissions contains invalid data
transformed_df.filter(col("Total_Emissions") == 0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sauvegarde des Données dans la Couche Gold
# MAGIC Sauvegarder les données transformées pour l'analyse finale.

# COMMAND ----------

# Save the transformed data to the gold layer
transformed_df.write.format("parquet").mode("overwrite").save("/mnt/gold/emissions_transformed/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification des Résultats
# MAGIC Assurez-vous que les données sont correctement enregistrées dans la couche Gold.

# COMMAND ----------

# Verify the saved data in the gold layer
gold_df = spark.read.parquet("/mnt/gold/emissions_transformed/")
display(gold_df)
