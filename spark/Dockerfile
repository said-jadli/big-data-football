# Base image with Jupyter and PySpark
FROM jupyter/pyspark-notebook

# Switch to root to install system-level dependencies
USER root

# Install Python dependencies
RUN pip install --no-cache-dir kafka-python requests
RUN pip install --no-cache-dir pyspark==3.5.0
# Copy the MongoDB Spark connector JAR
# Ensure this path matches your directory structure
COPY spark/jars/mongo-spark-connector_2.12-10.3.0-all.jar /usr/local/spark/jars/

COPY spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar /usr/local/spark/jars/

# Ajout du connecteur Spark Kafka
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar /usr/local/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /usr/local/spark/jars/



# Switch back to the default user
USER ${NB_UID}
