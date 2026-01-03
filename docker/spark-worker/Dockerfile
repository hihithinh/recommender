FROM apache/spark:python3

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    wget \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /opt/spark-data /opt/raw-data /opt/spark-apps /opt/scripts

WORKDIR /opt/spark

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

EXPOSE 8081 7000-7100

# Use shell form to allow environment variable expansion
CMD /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker ${SPARK_MASTER_URL}
