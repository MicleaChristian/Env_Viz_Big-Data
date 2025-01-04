# Env_Viz_Big-Data
# **Impact of Climate Change Indicators on Global Trends**

## **Description**
This project analyzes the impact of key climate change indicators (e.g., temperature trends, CO2 emissions, and natural disaster frequency) on global and regional patterns. It employs a Lakehouse architecture using Databricks Community and Microsoft Fabric for ETL and Power BI for visualization.

---

## **Objectives**
1. **Data Ingestion and Preparation**:
   - Import datasets (CSV, JSON) from open sources like Kaggle, NOAA, and NASA.
2. **Data Transformation**:
   - Clean and structure the data in a star schema.
3. **Lakehouse Architecture Implementation**:
   - Use Apache Iceberg for storage and ensure scalability.
4. **Data Visualization**:
   - Provide actionable insights using Power BI.

---

## **Technologies and Tools**

### **Platforms**
- **Airbyte**: For ingesting datasets from NOAA, NASA, or Kaggle.
- **Databricks Community**: For cleaning, transforming, and analyzing data.
- **Microsoft Fabric Synapse**: For ETL workflows.
- **Power BI**: For creating and sharing visualizations.

### **Formats and Frameworks**
- **Apache Iceberg**: For open table format storage.
- **Redpanda or Kafka**: For real-time data streaming (optional if time permits).
- **PySpark**: For scalable data transformations and analysis.

---

## **Getting Started**

### **Prerequisites**
- Install or set up:
  - Databricks Community Edition
  - Airbyte for data ingestion
  - Power BI for visualization
  - Python environment with PySpark

### **Installation**
1. Clone this repository:
   ```bash
   git clone https://MicleaChristian/Env_Viz_Big-Data.git
2. Navigate to the project repository:
   ```bash
   cd Env_Viz_Big-Data
3. Install required Python libraries
   ```bash
   pip install -r requirements.txt
4. Configure Airbyte to connect to your data sources.
---

## Project Workflow

### Data Ingestion

- **Sources**:
  - [NOAA](https://www.ncei.noaa.gov/): Climate and weather data.
  - [NASA Earth Data](https://earthdata.nasa.gov/): Atmospheric and temperature trends.
  - [Kaggle](https://www.kaggle.com/): Various datasets on climate change.
  - [Global Carbon Atlas](http://www.globalcarbonatlas.org/): CO2 emissions data by sector and region.
- Use **Airbyte** to ingest datasets into your Lakehouse architecture:
  - Connect Airbyte to your data sources and configure destinations.
  - Automate the ingestion process to move raw data into the **bronze layer**.

---

### Data Transformation

- Use **PySpark** within **Databricks Community** to:
  - **Clean Data**:
    - Remove missing or duplicate values.
    - Normalize column formats (e.g., date and unit standardization).
  - **Transform Data**:
    - Create structured datasets for the **silver layer** by filtering and joining raw data.
    - Aggregate metrics like average temperature by year and CO2 emissions by sector.
  - **Prepare Gold Layer**:
    - Generate final structured tables optimized for analysis.

---

### Lakehouse Architecture

- Implement a Lakehouse model:
  - **Bronze Layer**: Store raw ingested datasets.
  - **Silver Layer**: Store cleaned and structured datasets.
  - **Gold Layer**: Store aggregated data ready for visualization and reporting.
- Use **Apache Iceberg**:
  - Manage datasets across all layers efficiently.
  - Enable schema evolution and time travel for version control.

---

### Data Modeling

- Design a **star schema** for the **gold layer**:
  - **Fact Table**:
    - Climate metrics such as temperature trends, CO2 emissions, and natural disaster counts.
  - **Dimension Tables**:
    - **Time**: Year, month, and day.
    - **Region**: Country, continent, or other geographic divisions.
    - **Sector**: Transport, energy, industry, etc.

---

### Data Visualization

- Create interactive dashboards using **Power BI**:
  - Visualize global and regional trends:
    - **Line Charts**: Global temperature changes over decades.
    - **Bar Charts**: CO2 emissions by country or sector.
    - **Maps**: Geographic distribution of natural disaster occurrences.
  - Provide actionable insights and recommendations.

---

## Deliverables

1. **PDF Report**:
   - Overview of the project objectives and data sources.
   - Detailed description of the data processing pipeline.
   - Star schema design and insights from data visualization.
2. **GitHub Repository**:
   - Include all code for ingestion, transformation, and visualization.
3. **Power BI Dashboards**:
   - Share insightful and interactive visualizations.

---

## Optional Enhancements

- **Real-Time Data Streaming**:
  - Integrate **Redpanda** or **Kafka** for processing live data feeds.
- **Predictive Analytics**:
  - Use **PySpark MLlib** to predict future climate trends based on historical data.

---

## Contributing

Contributions are welcome! To contribute:
1. **Fork** the repository.
2. Create a new branch:
   ```bash
   git checkout -b feature-name
3. Commit your changes
   ```bash
   git commit -m "Add feature-name"
4. Push your branch:
   ```bash
   git push origin feature-name
5. Open a Pull request for review.
---

## License

This project is licensed under the [MIT License](LICENSE).  
You are free to use, modify, and distribute this project as per the terms of the license.

---

## Acknowledgments

- **Data Sources**:
  - [NOAA](https://www.ncei.noaa.gov/): Comprehensive climate and weather datasets.
  - [NASA Earth Data](https://earthdata.nasa.gov/): Atmospheric and temperature trends datasets.
  - [Kaggle](https://www.kaggle.com/): Open datasets on climate change.
  - [Global Carbon Atlas](http://www.globalcarbonatlas.org/): CO2 emissions data by region and sector.

- **Technologies**:
  - **Apache Iceberg**: For managing datasets in the Lakehouse architecture.
  - **Databricks Community Edition**: For scalable data transformation.
  - **Airbyte**: For automating data ingestion.
  - **Power BI**: For creating interactive dashboards.

- Special thanks to the open-source community and contributors for the tools and frameworks used in this project.