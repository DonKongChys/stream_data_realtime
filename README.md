# Introduction
This project is a thorough tutorial on creating a complete data engineering pipeline. It details each phase, from data ingestion and processing to storage, using a powerful tech stack comprising Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. The entire setup is containerized with Docker, ensuring easy deployment and scalability.
# Architechture
![image](https://github.com/user-attachments/assets/eca9c88e-c85b-4400-a144-b02b40b521c6)

- Data Source: We use randomuser.me API to generate random user data for our pipeline.
- Apache Airflow: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- Apache Kafka and Zookeeper: Used for streaming data from PostgreSQL to the processing engine.
- Control Center and Schema Registry: Helps in monitoring and schema management of our Kafka streams.
- Apache Spark: For data processing with its master and worker nodes.
- Cassandra: Where the processed data will be stored.
# Technology
- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker

