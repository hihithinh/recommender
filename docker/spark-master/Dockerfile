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

ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

EXPOSE 8080 7077 6066

CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
