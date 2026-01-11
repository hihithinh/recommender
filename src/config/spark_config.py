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
        master_ip = os.getenv("MASTER_TAILSCALE_IP", "namenode")
        # Driver host: advertise Tailscale IP to workers, but bind on all interfaces
        driver_host = os.getenv("SPARK_DRIVER_HOST", master_ip)
        
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
                .config("spark.hadoop.fs.defaultFS", f"hdfs://{master_ip}:9000")
                .config("spark.hadoop.dfs.replication", "1")
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.default.parallelism", "200")
                .config("spark.network.timeout", "800s")
                .config("spark.rpc.askTimeout", "600s")
                .config("spark.executor.heartbeatInterval", "60s")
                .getOrCreate())
        
        # Show WARN level but with minimal stack traces
        spark.sparkContext.setLogLevel("WARN")
        
        # Configure log pattern to show only essential info (no full stack traces)
        log4j = spark.sparkContext._jvm.org.apache.log4j
        
        # Suppress verbose DataStreamer/HDFS internal logs (too noisy)
        log4j.Logger.getLogger("org.apache.hadoop.hdfs.DataStreamer").setLevel(log4j.Level.ERROR)
        log4j.Logger.getLogger("org.apache.hadoop.hdfs.DFSClient").setLevel(log4j.Level.ERROR)
        log4j.Logger.getLogger("org.apache.hadoop.ipc").setLevel(log4j.Level.ERROR)
        log4j.Logger.getLogger("org.apache.hadoop.hdfs.protocol.datatransfer").setLevel(log4j.Level.ERROR)
        
        return spark
    
    @staticmethod
    def get_data_paths():
        master_ip = os.getenv("MASTER_TAILSCALE_IP", "namenode")
        return {
            "raw_data": f"hdfs://{master_ip}:9000/data/raw",
            "processed_data": f"hdfs://{master_ip}:9000/data/processed",
            "embeddings": f"hdfs://{master_ip}:9000/data/embeddings",
            "models": f"hdfs://{master_ip}:9000/data/models"
        }
