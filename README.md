# Highway Traffic Data Pipeline with SQLite Integration

This project aims to analyze and decongest national highways by processing road traffic data from various toll plazas. Data from different formats are consolidated into a SQLite database for efficient querying and analysis.

<p align="center">
  <img width="600" src="https://github.com/mohamedmagdy841/highway-traffic-data-pipeline/assets/64127744/01bf912a-1b44-458d-bc2f-ab08b8519adc">
</p>

## Project Overview

As a data engineer at a data analytics consulting company, the goal is to create an Apache Airflow DAG that performs the following tasks:

1. **Extract Data**
   - From a CSV file
   - From a TSV file
   - From a fixed-width file

2. **Transform Data**
   - Process and clean the extracted data for consistency

3. **Load Data**
   - Load the transformed data into a SQLite database for storage and querying

4. **Query Data**
   - Perform SQL queries on the SQLite database to extract insights and analytics

## Project Structure

- **dags/**: Contains the Airflow DAG definition.
- **data/**: Includes sample data files in CSV, TSV, and fixed-width formats.
  
## Getting Started

### Prerequisites

- Python 3.7+
- Apache Airflow 2.x
- Pandas
- SQLite3

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/highway-traffic-data-pipeline.git
   cd highway-traffic-data-pipeline
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start Airflow:**
   ```bash
   airflow db init
   airflow webserver --port 8080
   airflow scheduler
   ```

### Running the Pipeline

1. Place your data files in the `data/` directory.
2. Access the Airflow UI at `http://localhost:8080`.
3. Trigger the DAG named `ETL_toll_data`.

## SQLite Database Integration

- Transformed data is loaded into a SQLite database for structured storage.
- SQLite provides a lightweight, file-based database ideal for analytical queries.

## Querying Data

- Use SQL queries to extract insights and perform analytics directly from the SQLite database.

## Graph
<p align="center">
  <img width="800" src="https://github.com/mohamedmagdy841/highway-traffic-data-pipeline/assets/64127744/60c35230-9b14-4f0b-af82-5ed4717037c7">
</p>

## Query Logs
<p align="center">
  <img width="800" src="https://github.com/mohamedmagdy841/highway-traffic-data-pipeline/assets/64127744/a1541f37-5ff4-4c2c-a88f-f5dabca848fc">
</p>
