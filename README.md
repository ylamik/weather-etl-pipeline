# 🌤️ Automated NCR Regional Weather ETL Pipeline

An end-to-end, containerized data pipeline that extracts live weather data across all 17 municipalities of the National Capital Region (NCR), transforms it via Apache Airflow, and loads it into a PostgreSQL data warehouse for visualization in Metabase.

## 🏗️ Project Architecture

* **Extract:** Python hits the Open-Meteo REST API to fetch daily temperature and geospatial data for all 17 NCR cities.
* **Transform:** Python parses the nested JSON payload and isolates the required metrics (Max/Min Celsius, Dates, Latitude, and Longitude).
* **Load:** The cleaned data is loaded into a strictly typed PostgreSQL database.
* **Orchestrate:** Apache Airflow schedules and monitors the ETL process to run automatically on a daily schedule.
* **Visualize:** Metabase connects directly to the warehouse to serve a live, auto-updating dashboard mapping the entire region.

## 💻 Tech Stack

* **Language:** Python 3
* **Orchestration:** Apache Airflow
* **Database:** PostgreSQL
* **Containerization:** Docker & Docker Compose
* **Business Intelligence:** Metabase

## 🚀 How to Run This Project Locally

**1. Clone the repository**
```bash
git clone [https://github.com/ylamik/weather-etl-pipeline.git](https://github.com/ylamik/weather-etl-pipeline.git)
cd weather-etl-pipeline
```

**2. Set up your environment variables**
Create a `.env` file in the root directory and add your database credentials:
```env
DB_USER=data_engineer
DB_PASSWORD=your_password
DB_NAME=data_engineer
```

**3. Spin up the containers**
```bash
docker-compose up -d --build
```

**4. Access the UI**
* **Airflow:** http://localhost:8080
* **Metabase:** http://localhost:3000

## 📊 Dashboard Snapshot
![Metabase Dashboard 1.1](assets/dashboard_snapshot_1.png)
![Metabase Dashboard 1.2](assets/dashboard_snapshot_2.png)
![Metabase Dashboard 1.3](assets/dashboard_snapshot_3.png)