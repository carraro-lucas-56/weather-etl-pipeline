# Weather ETL Pipeline

## Project Overview

This project implements a **data engineering pipeline** that collects, processes, and stores large-scale weather data for Brazil.

The pipeline retrieves **hourly weather data for 5,000 Brazilian cities** from **two different weather APIs**, combines the datasets, and loads the results into **Google BigQuery**. The pipeline is orchestrated using **Apache Airflow**, which schedules the ingestion and loading process to run **daily**.

Each execution processes large volumes of API data and produces approximately **120,000 rows of new weather observations per day**, which are stored in a **partitioned BigQuery table** optimized for analytical queries.

---

# Project Architecture

The pipeline follows a traditional **ETL architecture**:

1. **Extract**

   * Collect hourly weather data from two external APIs
   * Handle API rate limits and different endpoint structures

2. **Transform**

   * Normalize the schemas from both APIs
   * Merge datasets
   * Clean and structure the data

3. **Load**

   * Upload the processed data to **Google BigQuery**
   * Store results in a **partitioned table by date**

4. **Orchestration**

   * **Apache Airflow** schedules the pipeline to run daily and manages task execution.

---

# Installation

Clone the repository:

```
git clone https://github.com/carraro-lucas-56/weather-etl-pipeline.git
cd weather-etl-pipeline
```

Create and activate a virtual environment:

```
python -m venv venv
source venv/bin/activate
```

Install the project and its dependencies:

```
pip install -e .
```

If you want to run the Airflow scheduler locally, install Airflow as well:

```
pip install apache-airflow
```

---

# Environment Variables

Create a `.env` file in the root directory with the required variables.

Example:

```
DATA_ROOT=/path/to/data
EMAIL=your-email@example.com
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json
CITIES_URL="https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/municipios.csv"
PROJECT_ID=project_id_from_google
```

---

# Running Airflow

Initialize and start Airflow:

```
airflow standalone
```

Then open the Airflow UI in your browser:

```
http://localhost:8080
```

Enable the DAG:

```
weather_pipeline
```

Airflow will schedule the pipeline to run daily.

---

# Data Output

The pipeline generates structured weather datasets and loads them into **Google BigQuery**.

Dataset characteristics:

* Hourly weather observations
* Coverage of **5,000 cities across Brazil**
* Approximately **120,000 new rows per day**
* Stored in a **partitioned BigQuery table**
* Optimized for analytical queries

---

# Main Challenges and Learnings

This project involved several technical challenges that were important learning experiences.

## Integrating Multiple APIs

The pipeline extracts weather data from **two different APIs**, each with different characteristics:

* Different endpoints
* Different response formats
* Different request limits
* Different rate limit policies

Understanding how both APIs work and designing a reliable extraction system that respects those limits required experimentation, testing, and adjustments to the pipeline logic.

---

## Working with the Google BigQuery API

Another major challenge was learning how to interact with **Google Cloud BigQuery programmatically**.

This involved:

* Authenticating using **Google service accounts**
* Designing the **table schema**
* Uploading large datasets using the **BigQuery Python client**
* Structuring data in a format compatible with BigQuery ingestion
* Implementing **date-based partitioning** for efficient queries

---

## Learning Cloud-Based Data Engineering

This was my **first project involving cloud infrastructure in a data engineering workflow**.

Through this project I learned:

* How **cloud data warehouses** like BigQuery operate
* How to integrate Python pipelines with **cloud APIs**
* How to design pipelines suitable for **cloud analytics environments**
* Practical aspects of **cloud-based data ingestion and storage**

