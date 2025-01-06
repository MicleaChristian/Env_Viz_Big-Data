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

# Load CO2 emissions data
gold_emissions_df = spark.read.parquet("/mnt/gold/emissions_transformed/")

# Load temperature anomalies data
gold_temperature_df = spark.read.parquet("/mnt/gold/temperature_anomalies_transformed/")

# Display both datasets
print("Gold Layer - CO2 Emissions Data:")
display(gold_emissions_df)

print("Gold Layer - Temperature Anomalies Data:")
display(gold_temperature_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyse des Données
# MAGIC Effectuez des analyses approfondies pour extraire des insights utiles.

# COMMAND ----------

from pyspark.sql.functions import col, avg, max, min, sum

# 1. Average CO2 Emissions per Year
avg_emissions = gold_emissions_df.select(avg("Total_Emissions").alias("Average_Emissions"))
print("Average Total Emissions:")
display(avg_emissions)

# 2. Year with Maximum Total Emissions
max_emissions = gold_emissions_df.select("Year", "Total_Emissions").orderBy(col("Total_Emissions").desc()).limit(1)
print("Year with Maximum Total Emissions:")
display(max_emissions)

# 3. Year with Minimum Total Emissions
min_emissions = gold_emissions_df.select("Year", "Total_Emissions").orderBy(col("Total_Emissions").asc()).limit(1)
print("Year with Minimum Total Emissions:")
display(min_emissions)

# 4. Average Temperature Anomaly
avg_anomaly = gold_temperature_df.select(avg("Anomaly").alias("Average_Anomaly"))
print("Average Temperature Anomaly:")
display(avg_anomaly)

# 5. Year with Maximum Anomaly
max_anomaly = gold_temperature_df.select("Year", "Anomaly").orderBy(col("Anomaly").desc()).limit(1)
print("Year with Maximum Anomaly:")
display(max_anomaly)

# 6. Year with Minimum Anomaly
min_anomaly = gold_temperature_df.select("Year", "Anomaly").orderBy(col("Anomaly").asc()).limit(1)
print("Year with Minimum Anomaly:")
display(min_anomaly)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Emissions Per Country
# MAGIC Calculez les émissions totales par pays sur toute la période.

# COMMAND ----------

# Calculate total emissions per country
country_columns = [c for c in gold_emissions_df.columns if c not in ["Year", "Total_Emissions"]]
total_emissions_per_country = gold_emissions_df.select(
    *[sum(col(c)).alias(c) for c in country_columns]
).toPandas().T.reset_index()
total_emissions_per_country.columns = ["Country", "Total_Emissions"]

# Display total emissions per country
total_emissions_per_country = total_emissions_per_country.sort_values("Total_Emissions", ascending=False)
print("Total Emissions Per Country:")
display(total_emissions_per_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisation des Données
# MAGIC Créez des graphiques pour visualiser les tendances et les insights.

# COMMAND ----------

import matplotlib.pyplot as plt

# CO2 Emissions Over Years
gold_emissions_pdf = gold_emissions_df.toPandas()
gold_emissions_pdf["Year"] = gold_emissions_pdf["Year"].astype(int)

plt.figure(figsize=(12, 6))
plt.plot(gold_emissions_pdf["Year"], gold_emissions_pdf["Total_Emissions"], marker="o")
plt.title("Total Emissions Over Years (1960-2023)", fontsize=16)
plt.xlabel("Year", fontsize=14)
plt.ylabel("Total Emissions (MtCO2)", fontsize=14)
plt.grid(True)
plt.xticks(range(1960, 2024, 5))  # Show ticks every 5 years
plt.show()

# Temperature Anomalies Over Years
gold_temperature_pdf = gold_temperature_df.toPandas()
gold_temperature_pdf["Year"] = gold_temperature_pdf["Year"].astype(int)

plt.figure(figsize=(12, 6))
plt.plot(gold_temperature_pdf["Year"], gold_temperature_pdf["Anomaly"], marker="o", color="orange")
plt.title("Temperature Anomalies Over Years (1960-2023)", fontsize=16)
plt.xlabel("Year", fontsize=14)
plt.ylabel("Temperature Anomaly (°C)", fontsize=14)
plt.grid(True)
plt.xticks(range(1960, 2024, 5))  # Show ticks every 5 years
plt.show()

# Top 10 Emitting Countries
plt.figure(figsize=(12, 6))
top_countries = total_emissions_per_country.head(10)
plt.bar(top_countries["Country"], top_countries["Total_Emissions"])
plt.title("Top 10 Countries by Total Emissions", fontsize=16)
plt.xlabel("Country", fontsize=14)
plt.ylabel("Total Emissions (MtCO2)", fontsize=14)
plt.xticks(rotation=45, ha="right")
plt.grid(axis="y")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exportation des Résultats
# MAGIC Sauvegardez les résultats analysés ou visualisés si nécessaire.

# COMMAND ----------

# Save analyzed CO2 emissions data
gold_emissions_df.write.format("parquet").mode("overwrite").save("/mnt/gold/analysis_emissions/")

# Save analyzed temperature anomalies data
gold_temperature_df.write.format("parquet").mode("overwrite").save("/mnt/gold/analysis_temperature_anomalies/")

# Save total emissions per country
total_emissions_per_country_spark = spark.createDataFrame(total_emissions_per_country)
total_emissions_per_country_spark.write.format("parquet").mode("overwrite").save("/mnt/gold/total_emissions_by_country/")

print("Analysis results saved to Gold Layer.")
