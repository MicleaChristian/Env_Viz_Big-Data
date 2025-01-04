# Env_Viz_Big-Data
Impact of Climate Change Indicators on Global Trends

Description

This project analyzes the impact of key climate change indicators (e.g., temperature trends, CO2 emissions, and natural disaster frequency) on global and regional patterns. It employs a Lakehouse architecture using Databricks Community and Microsoft Fabric for ETL and Power BI for visualization.

Objectives
	1.	Data Ingestion and Preparation:
	•	Import datasets (CSV, JSON) from open sources like Kaggle, NOAA, and NASA.
	2.	Data Transformation:
	•	Clean and structure the data in a star schema.
	3.	Lakehouse Architecture Implementation:
	•	Use Apache Iceberg for storage and ensure scalability.
	4.	Data Visualization:
	•	Provide actionable insights using Power BI.

Technologies and Tools

Platforms:
	•	Airbyte: For ingesting datasets from NOAA, NASA, or Kaggle.
	•	Databricks Community: For cleaning, transforming, and analyzing data.
	•	Microsoft Fabric Synapse: For ETL workflows.
	•	Power BI: For creating and sharing visualizations.

Formats and Frameworks:
	•	Apache Iceberg: For open table format storage.
	•	Redpanda or Kafka: For real-time data streaming (optional if time permits).
	•	PySpark: For scalable data transformations and analysis.