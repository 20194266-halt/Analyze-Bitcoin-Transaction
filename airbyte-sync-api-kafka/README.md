# Data Ingestion using Airbyte and Kafka

## Overview

This project demonstrates how to set up a data ingestion pipeline that uses **Airbyte** to ingest data from a Bitcoin transaction API and syncs the ingested data with **Kafka** for further processing and analysis.

## Requirements

Before getting started, ensure you have the following:

- **Docker**: Airbyte runs in a Docker container.
- **Apache Kafka**: A running Kafka instance for data synchronization.
- **Airbyte**: The latest version of Airbyte installed. 
- **JDK**: Java Development Kit installed (for Kafka).

## Airbyte Configuration

1. **Access Airbyte UI**

   Open your browser and navigate to `http://localhost:8000` to access the Airbyte UI.

2. **Create a Source**

   - Click on "Sources" in the left navigation panel.
   - Select "Custom connector"
   - Fill in the necessary configuration details
   - Test the connection to ensure it's working.

3. **Create a Destination**

   - Click on "Destinations" in the left navigation panel.
   - Select "Add Destination."
   - Choose the **Kafka** destination connector.
   - Fill in the configuration details.
   - Test the connection.

4. **Create a Sync**

   - Click on "Connections" in the left navigation panel.
   - Select "Add Connection."
   - Choose the source and destination you just created.
   - Set the sync frequency (e.g., manual, every 5 minutes).
   - Save the connection.

## Kafka Configuration

Ensure that your Kafka broker is set up and running correctly. Create a topic where the data will be sent:

```bash
bin/kafka-topics.sh --create --topic Test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Running the Pipeline

Once the Airbyte sync connection is set up, you can manually trigger the sync process:

- Go to the Airbyte UI.
- Click on the connection you created.
- Click on the "Sync" button.

Alternatively, you can configure Airbyte to run the sync automatically based on the frequency you set.
