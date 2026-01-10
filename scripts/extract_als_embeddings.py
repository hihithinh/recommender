import sys
sys.path.append('/opt/spark-apps')

from pyspark.sql import SparkSession
from config.spark_config import SparkConfig
from models.als_recommender import ALSRecommender
from utils.spark_utils import save_parquet
from utils.logger import get_default_logger


def extract_als_embeddings():
    """
    Extract user and item embeddings from trained ALS model.
    This should be run after train_als.py completes successfully.
    """
    logger = get_default_logger("extract_embeddings")
    logger.info("Starting ALS embeddings extraction...")
    
    spark = SparkConfig.create_spark_session(app_name="Extract-ALS-Embeddings")
    paths = SparkConfig.get_data_paths()
    
    try:
        logger.info("Loading trained ALS model...")
        als_recommender = ALSRecommender()
        als_recommender.load_model(f"{paths['models']}/als_model")
        
        logger.info("Extracting user and item embeddings...")
        user_embeddings = als_recommender.get_user_embeddings()
        item_embeddings = als_recommender.get_item_embeddings()
        
        user_count = user_embeddings.count()
        item_count = item_embeddings.count()
        
        logger.info(f"User embeddings count: {user_count:,}")
        logger.info(f"Item embeddings count: {item_count:,}")
        
        logger.info("Saving embeddings to HDFS...")
        save_parquet(user_embeddings, f"{paths['embeddings']}/als_user_embeddings.parquet")
        save_parquet(item_embeddings, f"{paths['embeddings']}/als_item_embeddings.parquet")
        
        logger.info("Embeddings extraction completed successfully!")
        logger.info("Next step: Run train_lightgbm.py to train LightGBM model")
        
        return {
            "user_embeddings_count": user_count,
            "item_embeddings_count": item_count
        }
        
    finally:
        spark.stop()


if __name__ == "__main__":
    results = extract_als_embeddings()
    print("\n" + "="*50)
    print("Embeddings Extraction Summary:")
    print("="*50)
    print(f"User embeddings: {results['user_embeddings_count']:,}")
    print(f"Item embeddings: {results['item_embeddings_count']:,}")
