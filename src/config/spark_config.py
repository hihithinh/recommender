from pyspark.sql import SparkSession
from typing import Optional
import os


class SparkConfig:
    
    @staticmethod
    def create_spark_session(
        app_name: str = "BookCrossing-Recommender",
        master: Optional[str] = None,
        executor_memory: str = "4g",
        executor_cores: int = 2,
        driver_memory: str = "2g"
    ) -> SparkSession:
        
        if master is None:
            master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        
        # Get driver host for distributed setup
        driver_host = os.getenv("SPARK_DRIVER_HOST", "spark-master")
        
        spark = (SparkSession.builder
                .appName(app_name)
                .master(master)
                .config("spark.executor.memory", executor_memory)
                .config("spark.executor.cores", executor_cores)
                .config("spark.driver.memory", driver_memory)
                .config("spark.driver.host", driver_host)
                .config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.driver.port", "35000")
                .config("spark.driver.blockManager.port", "35001")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.default.parallelism", "200")
                .config("spark.network.timeout", "600s")
                .config("spark.executor.heartbeatInterval", "60s")
                .getOrCreate())
        
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    @staticmethod
    def get_data_paths():
        return {
            "raw_data": "/opt/raw-data",
            "processed_data": "/opt/spark-data/processed",
            "embeddings": "/opt/spark-data/embeddings",
            "models": "/opt/spark-data/models"
        }
