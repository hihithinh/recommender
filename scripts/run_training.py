import sys
sys.path.append('/opt/spark-apps')

from pyspark.sql import SparkSession
from config.spark_config import SparkConfig
from training.train_als import train_als_model
from utils.logger import get_default_logger


def main():
    logger = get_default_logger("run_training")
    logger.info("Starting model training pipeline...")
    
    spark = SparkConfig.create_spark_session(app_name="Model-Training")
    
    try:
        logger.info("Training ALS model...")
        als_results = train_als_model(spark)
        
        logger.info("\n" + "="*50)
        logger.info("Training Pipeline Completed!")
        logger.info("="*50)
        logger.info("\nALS Results:")
        for key, value in als_results.items():
            logger.info(f"  {key}: {value:.4f}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
