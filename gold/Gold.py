# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Gold: Analyse des Données
# MAGIC ## Objectif :
# MAGIC Analyser et visualiser les données transformées dans la couche Gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration et Montage de la Source de Données
# MAGIC Définir l'emplacement des fichiers dans le conteneur Gold.

# COMMAND ----------

# Azure Storage account details
storage_account_name = "miclea"
container_name = "gold"
access_key = "xMG8bxTSXMF7Qb3S6AaVzINxitBCSOhYaPiHvmG3ToDbk2ajZc2UQ46VbuafW8StD41jSInil9SN+AStqRygfQ=="

mount_point = f"/mnt/{container_name}"

# Unmount the container if already mounted
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
# MAGIC ## Lecture des Données de la Couche Gold
# MAGIC Charger les fichiers Parquet de la couche Gold.

# COMMAND ----------

# Load the transformed Parquet data from the gold layer
gold_df = spark.read.parquet("/mnt/gold/emissions_transformed/")

# Ensure all years are included (1960-2023) with 0 emissions if missing
from pyspark.sql.functions import lit
all_years_df = spark.createDataFrame([(str(year),) for year in range(1960, 2024)], ["Year"])
from pyspark.sql.functions import coalesce

# Ensure existing Total_Emissions are retained, filling only missing years
gold_complete_df = all_years_df.join(gold_df, "Year", "left_outer") \
    .withColumn("Total_Emissions", coalesce(gold_df["Total_Emissions"], lit(0)))

# Display the completed DataFrame
display(gold_complete_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyse des Données
# MAGIC Effectuez des analyses approfondies pour extraire des insights utiles.

# COMMAND ----------

from pyspark.sql.functions import col, avg, max, min

# Example 1: Average Total Emissions Per Year
avg_emissions = gold_complete_df.select(avg("Total_Emissions").alias("Average_Emissions"))
print("Average Total Emissions:")
display(avg_emissions)

# Example 2: Year with Maximum Emissions
max_emissions = gold_complete_df.select("Year", "Total_Emissions").orderBy(col("Total_Emissions").desc()).limit(1)
print("Year with Maximum Total Emissions:")
display(max_emissions)

# Example 3: Year with Minimum Emissions
min_emissions = gold_complete_df.select("Year", "Total_Emissions").orderBy(col("Total_Emissions").asc()).limit(1)
print("Year with Minimum Total Emissions:")
display(min_emissions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisation des Données
# MAGIC Créez des graphiques pour visualiser les tendances et les insights.

# COMMAND ----------

import matplotlib.pyplot as plt

# Convert the completed DataFrame to Pandas for visualization
gold_pdf = gold_complete_df.toPandas()

# Ensure 'Year' is sorted as integers for proper plotting
gold_pdf["Year"] = gold_pdf["Year"].astype(int)
gold_pdf = gold_pdf.sort_values("Year")

# Line Chart: Total Emissions Over Years
plt.figure(figsize=(12, 6))
plt.plot(gold_pdf["Year"], gold_pdf["Total_Emissions"], marker="o")
plt.title("Total Emissions Over Years (1960-2023)", fontsize=16)
plt.xlabel("Year", fontsize=14)
plt.ylabel("Total Emissions", fontsize=14)
plt.grid(True)
plt.xticks(range(1960, 2024, 5))  # Show ticks every 5 years
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exportation des Résultats
# MAGIC Sauvegardez les résultats analysés ou visualisés si nécessaire.

# COMMAND ----------

# Save the analyzed data (e.g., summary statistics) back to the Gold layer
analysis_result = gold_complete_df.select("Year", "Total_Emissions")
analysis_result.write.format("parquet").mode("overwrite").save("/mnt/gold/analysis_results/")

print("Analysis results saved to /mnt/gold/analysis_results/")
