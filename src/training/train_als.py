from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import os

sys.path.append('/opt/spark-apps')

from config.spark_config import SparkConfig
from config.model_config import ALSConfig, DataConfig
from models.als_recommender import ALSRecommender
from utils.spark_utils import save_parquet, print_dataframe_info
from utils.logger import get_default_logger


def train_als_model(spark: SparkSession):
    
    logger = get_default_logger("train_als")
    logger.info("Starting ALS model training...")
    
    paths = SparkConfig.get_data_paths()
    
    logger.info("Loading processed ratings data...")
    ratings_df = spark.read.parquet(f"{paths['processed_data']}/ratings.parquet")
    
    logger.info(f"Total ratings: {ratings_df.count()}")
    
    train_df, validation_df, test_df = ratings_df.randomSplit(
        [DataConfig.TRAIN_RATIO, DataConfig.VALIDATION_RATIO, DataConfig.TEST_RATIO],
        seed=DataConfig.RANDOM_SEED
    )
    
    logger.info(f"Train set: {train_df.count()}")
    logger.info(f"Validation set: {validation_df.count()}")
    logger.info(f"Test set: {test_df.count()}")
    
    save_parquet(train_df, f"{paths['processed_data']}/train_ratings.parquet")
    save_parquet(validation_df, f"{paths['processed_data']}/validation_ratings.parquet")
    save_parquet(test_df, f"{paths['processed_data']}/test_ratings.parquet")
    
    logger.info("Initializing ALS model...")
    als_recommender = ALSRecommender(
        rank=ALSConfig.RANK,
        max_iter=ALSConfig.MAX_ITER,
        reg_param=ALSConfig.REG_PARAM,
        alpha=ALSConfig.ALPHA,
        implicit_prefs=ALSConfig.IMPLICIT_PREFS,
        cold_start_strategy=ALSConfig.COLD_START_STRATEGY,
        num_user_blocks=ALSConfig.NUM_USER_BLOCKS,
        num_item_blocks=ALSConfig.NUM_ITEM_BLOCKS,
        checkpoint_interval=ALSConfig.CHECKPOINT_INTERVAL
    )
    
    logger.info("Training ALS model...")
    model = als_recommender.train(train_df)
    
    logger.info("Evaluating on validation set...")
    val_predictions = als_recommender.predict(validation_df)
    val_metrics = als_recommender.evaluate_comprehensive(validation_df, val_predictions)
    
    logger.info("Validation Metrics:")
    logger.info(f"  RMSE: {val_metrics['rmse']:.4f}")
    logger.info(f"  MAE: {val_metrics['mae']:.4f}")
    logger.info(f"  R2: {val_metrics['r2']:.4f}")
    logger.info(f"  Explained Variance: {val_metrics['var']:.4f}")
    
    logger.info("Evaluating on test set...")
    test_predictions = als_recommender.predict(test_df)
    test_metrics = als_recommender.evaluate_comprehensive(test_df, test_predictions)
    
    logger.info("Test Metrics:")
    logger.info(f"  RMSE: {test_metrics['rmse']:.4f}")
    logger.info(f"  MAE: {test_metrics['mae']:.4f}")
    logger.info(f"  R2: {test_metrics['r2']:.4f}")
    logger.info(f"  Explained Variance: {test_metrics['var']:.4f}")
    
    logger.info("Saving ALS model...")
    als_recommender.save_model(f"{paths['models']}/als_model")
    
    logger.info("ALS training completed successfully!")
    logger.info("Next step: Run extract_als_embeddings.py to create embeddings for LightGBM")
    
    return {
        "validation": val_metrics,
        "test": test_metrics
    }


if __name__ == "__main__":
    spark = SparkConfig.create_spark_session(app_name="ALS-Training")
    
    try:
        results = train_als_model(spark)
        print("\n" + "="*50)
        print("Training Results Summary:")
        print("="*50)
        
        print("\nValidation Metrics:")
        for key, value in results['validation'].items():
            print(f"  {key}: {value:.4f}")
        
        print("\nTest Metrics:")
        for key, value in results['test'].items():
            print(f"  {key}: {value:.4f}")
    finally:
        spark.stop()
