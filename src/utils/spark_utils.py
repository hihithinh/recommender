from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, avg, stddev
from typing import Dict, Any
import logging


def get_dataframe_stats(df: DataFrame, name: str = "DataFrame") -> Dict[str, Any]:
    
    stats = {
        "name": name,
        "count": df.count(),
        "columns": df.columns,
        "num_columns": len(df.columns),
        "num_partitions": df.rdd.getNumPartitions()
    }
    
    return stats


def print_dataframe_info(df: DataFrame, name: str = "DataFrame"):
    
    print(f"\n{'='*50}")
    print(f"DataFrame: {name}")
    print(f"{'='*50}")
    print(f"Number of rows: {df.count():,}")
    print(f"Number of columns: {len(df.columns)}")
    print(f"Number of partitions: {df.rdd.getNumPartitions()}")
    print(f"\nSchema:")
    df.printSchema()
    print(f"\nSample data:")
    df.show(5, truncate=False)


def repartition_dataframe(df: DataFrame, num_partitions: int = None) -> DataFrame:
    
    if num_partitions is None:
        row_count = df.count()
        num_partitions = max(1, row_count // 100000)
    
    return df.repartition(num_partitions)


def save_parquet(df: DataFrame, path: str, mode: str = "overwrite", partition_by: list = None):
    
    writer = df.write.mode(mode).format("parquet")
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.save(path)
    print(f"Saved DataFrame to {path}")


def load_parquet(spark, path: str) -> DataFrame:
    
    df = spark.read.parquet(path)
    print(f"Loaded DataFrame from {path}")
    return df
