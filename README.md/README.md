
# Marketflow Analytics: A Real-time Cryptocurrency Data Pipeline

## Project Goal
The primary objective of the Marketflow Analytics project is to establish a robust data pipeline that ingests, processes, and analyses real-time cryptocurrency price data. This project simulates a system used by fintech platforms or financial analysts to monitor short-term market trends and volatility, providing actionable insights for trading decisions.

---

## Core Concepts
This pipeline leverages a series of modern data engineering technologies to achieve near real-time analytics:

- **Data Ingestion**: A Python script continuously pulls market data from the CoinGecko API.  
- **Message Queue**: Apache Kafka is used as a message broker to handle the high-volume stream of price updates.  
- **Processing & Transformation**: Apache Spark Structured Streaming consumes the data from Kafka, performs complex calculations, and enriches the data.  
- **Persistence**: Processed data is stored in Parquet files on the local filesystem for efficient querying and in a PostgreSQL database for downstream consumption (e.g., dashboards).  

---

## Architecture
The project follows a modular ETL (Extract, Transform, Load) pipeline:

1. **Extract**:  
   - A Python script (`crypto_dag.py`) fetches cryptocurrency market data from the CoinGecko API every 30 seconds.  

2. **Queue**:  
   - Raw JSON data is pushed into a Kafka topic named `crypto_prices`.  

3. **Transform**:  
   - A Spark streaming job (`kafka_streaming.py`) consumes the Kafka topic.  
   - It parses, flattens, and transforms the raw data into a structured DataFrame.  

4. **Enrichment**:  
   - A Spark script (`analytics.py`) reads the Parquet files.  
   - Performs advanced analytics: price changes, moving averages (SMA/EMA), volatility.  
   - Identifies top gainers and losers.  

5. **Load**:  
   - Enriched data is written back to a PostgreSQL database for persistent storage and reporting.  

---

## Data Schema

### Initial Data (from CoinGecko API)
- `id`: Unique identifier for the cryptocurrency (e.g., "bitcoin")  
- `symbol`: Trading symbol (e.g., "btc")  
- `current_price`: Price in USD  
- `market_cap`: Market capitalization  
- `total_volume`: 24-hour trading volume  
- `high_24h`: Highest price in the last 24 hours  
- `low_24h`: Lowest price in the last 24 hours  
- `last_updated`: Timestamp of the last update  

### Enriched Data (after Spark Processing)
- `timestamp`: Timestamp of data ingestion  
- `price`: Current price  
- `change_1min`: Percentage price change over the last 1 minute  
- `change_5min`: Percentage price change over the last 5 minutes  
- `SMA`: Simple Moving Average over a 5-minute window  
- `EMA`: Exponential Moving Average over a 3-minute window  
- `volatility`: Rolling standard deviation of price over a 5-minute window  
- `top_5_gainers`: Snapshot of the top 5 cryptocurrencies with the highest 5-minute price change  
- `top_5_losers`: Snapshot of the top 5 cryptocurrencies with the lowest 5-minute price change  

---

## Project Structure

```

Marketflow-Analysis/
│
├── dags/                 # Python scripts for scheduling and data ingestion
│   └── crypto\_dag.py
│
├── spark\_streaming/       # Spark jobs for data processing
│   ├── kafka\_streaming.py
│   └── analytics.py
│
├── env/                  # Python virtual environment
├── dataframes/           # Incoming Parquet files from Spark
├── checkpoints/          # Metadata for Spark Structured Streaming
├── tests/                # Unit tests and validation scripts
│
├── requirements.txt      # Python dependencies
├── README.md             # Project documentation
└── .env                  # Environment variables and secrets (local development)

````

---

## Technical Constraints & Decisions

- **Data Storage**: Parquet format for raw data, PostgreSQL for processed tables.  
- **Processing**: Spark jobs run on a local machine using Structured Streaming.  
- **Orchestration**: No distributed orchestrator (like Airflow); pipeline scripts handle execution loops.  

---

## Getting Started

### 1. Setup Environment
- Install Python, Java (for Spark), and PostgreSQL.  

### 2. Install Dependencies
```bash
pip install -r requirements.txt
````

### 3. Start Services

* Start Kafka (or Redpanda)
* Start PostgreSQL

### 4. Run Pipeline

* Run `crypto_dag.py` to begin data ingestion
* Run `kafka_streaming.py` to consume data from Kafka
* Run `analytics.py` to process and store enriched data in PostgreSQL

## Running the Project

### 1. Kafka Streaming Job
To start the Kafka streaming process with Spark:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.kafka:kafka-clients:3.7.0 \
  spark_streaming/kafka_streaming.py
````

### 2. Analytics Job

To run the analytics script and connect with PostgreSQL (make sure `postgresql-42.7.7.jar` is in your working directory):

```bash
spark-submit --jars postgresql-42.7.7.jar analytics.py
```

--- 

## Future Improvements

* Integrate with a dashboard (Streamlit, PowerBI, or Tableau).
* Deploy pipeline on cloud infrastructure (AWS/GCP).
* Add monitoring and alerting for Kafka/Spark failures.
* Extend analytics with ML-based trend forecasting.

