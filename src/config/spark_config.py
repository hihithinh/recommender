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
        
        master_ip = os.getenv("MASTER_TAILSCALE_IP", "namenode")
        driver_host = os.getenv("SPARK_DRIVER_HOST", master_ip)
        
        spark = (SparkSession.builder
                .appName(app_name)
                .master(master)
                
                # Memory configs
                .config("spark.executor.memory", executor_memory)
                .config("spark.executor.cores", executor_cores)
                .config("spark.driver.memory", driver_memory)
                .config("spark.executor.memoryOverhead", "1g")
                .config("spark.driver.memoryOverhead", "512m")
                
                # Network configs
                .config("spark.driver.host", driver_host)
                .config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.driver.port", "35000")
                .config("spark.driver.blockManager.port", "35001")
                
                # HDFS configs
                .config("spark.hadoop.fs.defaultFS", f"hdfs://{master_ip}:9000")
                .config("spark.hadoop.dfs.replication", "1")
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                
                # Timeout configs - INCREASED to prevent executor loss
                .config("spark.network.timeout", "1200s")
                .config("spark.rpc.askTimeout", "1200s")
                .config("spark.executor.heartbeatInterval", "30s")
                .config("spark.storage.blockManagerSlaveTimeoutMs", "600000")
                
                # Retry configs
                .config("spark.task.maxFailures", "4")
                .config("spark.stage.maxConsecutiveAttempts", "4")
                
                # Shuffle configs - REDUCED for smaller datasets
                .config("spark.sql.shuffle.partitions", "100")
                .config("spark.default.parallelism", "100")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                
                # Serialization
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryoserializer.buffer.max", "512m")
                
                # Compression
                .config("spark.sql.parquet.compression.codec", "snappy")
                
                # Speculation - retry slow tasks
                .config("spark.speculation", "true")
                .config("spark.speculation.multiplier", "2")
                
                .getOrCreate())
        
        spark.sparkContext.setLogLevel("WARN")
        
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
