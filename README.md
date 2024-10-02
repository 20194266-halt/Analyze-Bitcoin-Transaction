# Bitcoin Transaction Data Processing Pipeline

This project aims to fetch Bitcoin transaction data from an API, load it into Kafka using Airbyte, process it with Apache Spark Streaming, and finally save the processed data into Delta Lake. The processed data is then utilized for a dashboard and anomaly detection.

## Project Overview
![Data Pipeline](/dashboard/btc_data_pipeline.png)
1. **Data Ingestion**:
   - Fetch Bitcoin transaction data from a specified API.
   - Use **Airbyte** to load the data into **Kafka**.

2. **Data Processing**:
   - Use **Apache Spark Streaming** to read data from Kafka.
   - Process the incoming data and save it into **Delta Lake** for efficient storage and querying.

3. **Data Utilization**:
   - The processed data can be visualized on a dashboard.
   - Implement anomaly detection techniques to identify unusual patterns in Bitcoin transactions.

## Technologies Used

- **Airbyte**: For data integration and loading into Kafka.
- **Kafka**: For streaming data ingestion.
- **Apache Spark**: For processing streaming data.
- **Delta Lake**: For storing processed data efficiently.
- **Plotly Dash**: To visualize the processed data.
- **SparkML**: For identifying unusual transactions.


