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

WORKDIR /opt/spark-apps

ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt/spark-apps:/opt/spark/python:$PYTHONPATH

EXPOSE 5000

CMD ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "/opt/spark-apps/api/app.py"]
