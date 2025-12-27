FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

RUN apt-get update && apt-get install -y \
    wget \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /opt/spark-data /opt/raw-data /opt/spark-apps /opt/scripts /opt/notebooks

WORKDIR /opt/notebooks

USER ${NB_UID}

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS='lab'

EXPOSE 8888 4040
