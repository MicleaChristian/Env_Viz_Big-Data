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

# Load CO2 emissions data
emissions_df = spark.read.parquet("/mnt/silver/emissions/")

# Load temperature anomalies data
temperature_df = spark.read.parquet("/mnt/silver/temperature_anomalies/")

# Display the loaded data
print("Emissions Data:")
display(emissions_df)

print("Temperature Anomalies Data:")
display(temperature_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification et Nettoyage des Données : CO2 Emissions
# MAGIC Assurez-vous que les données sur les émissions de CO2 sont propres et valides.

# COMMAND ----------

from pyspark.sql.functions import col, count, when
from functools import reduce  # Ensure reduce is imported

# Check for missing or null values
missing_values_emissions = emissions_df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in emissions_df.columns]
)
print("Missing values in emissions data:")
display(missing_values_emissions)

# Fill null values with 0
cleaned_emissions_df = emissions_df.fillna(0)

# Remove rows where all emissions are zero
non_zero_columns = [c for c in cleaned_emissions_df.columns if c != "Year"]
cleaned_emissions_df = cleaned_emissions_df.filter(
    reduce(lambda x, y: x | y, [col(c) != 0 for c in non_zero_columns])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification et Nettoyage des Données : Temperature Anomalies
# MAGIC Assurez-vous que les données sur les anomalies de température sont propres et valides.

# COMMAND ----------

# Check for missing or null values
missing_values_temperature = temperature_df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in temperature_df.columns]
)
print("Missing values in temperature anomalies data:")
display(missing_values_temperature)

# Fill null values (if any) with 0
cleaned_temperature_df = temperature_df.fillna(0)

# Display the cleaned temperature anomalies data
display(cleaned_temperature_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structuration et Transformation des Données
# MAGIC Ajoutez des transformations ou enrichissez les données, si nécessaire.

# COMMAND ----------

from pyspark.sql.functions import col

# CO2 Emissions: Calculate Total Emissions Per Year
columns_to_sum = [col(c) for c in cleaned_emissions_df.columns if c != "Year"]
transformed_emissions_df = cleaned_emissions_df.withColumn(
    "Total_Emissions",
    reduce(lambda x, y: x + y, columns_to_sum)  # Sum all columns except 'Year'
)

# Verify if Total_Emissions contains invalid data
print("Transformed CO2 Emissions Data:")
display(transformed_emissions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sauvegarde des Données dans la Couche Gold
# MAGIC Sauvegarder les données transformées pour l'analyse finale.

# COMMAND ----------

# Save transformed CO2 emissions data to the gold layer
transformed_emissions_df.write.format("parquet").mode("overwrite").save("/mnt/gold/emissions_transformed/")

# Save cleaned temperature anomalies data to the gold layer
cleaned_temperature_df.write.format("parquet").mode("overwrite").save("/mnt/gold/temperature_anomalies_transformed/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification des Résultats
# MAGIC Assurez-vous que les données sont correctement enregistrées dans la couche Gold.

# COMMAND ----------

# Verify saved CO2 emissions data
gold_emissions_df = spark.read.parquet("/mnt/gold/emissions_transformed/")
print("Transformed CO2 Emissions Data in Gold Layer:")
display(gold_emissions_df)

# Verify saved temperature anomalies data
gold_temperature_df = spark.read.parquet("/mnt/gold/temperature_anomalies_transformed/")
print("Transformed Temperature Anomalies Data in Gold Layer:")
display(gold_temperature_df)
