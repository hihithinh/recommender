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
    val_rmse = als_recommender.evaluate(val_predictions, metric="rmse")
    val_mae = als_recommender.evaluate(val_predictions, metric="mae")
    
    logger.info(f"Validation RMSE: {val_rmse:.4f}")
    logger.info(f"Validation MAE: {val_mae:.4f}")
    
    logger.info("Evaluating on test set...")
    test_predictions = als_recommender.predict(test_df)
    test_rmse = als_recommender.evaluate(test_predictions, metric="rmse")
    test_mae = als_recommender.evaluate(test_predictions, metric="mae")
    
    logger.info(f"Test RMSE: {test_rmse:.4f}")
    logger.info(f"Test MAE: {test_mae:.4f}")
    
    logger.info("Extracting user and item embeddings...")
    user_embeddings = als_recommender.get_user_embeddings()
    item_embeddings = als_recommender.get_item_embeddings()
    
    logger.info(f"User embeddings count: {user_embeddings.count()}")
    logger.info(f"Item embeddings count: {item_embeddings.count()}")
    
    save_parquet(user_embeddings, f"{paths['embeddings']}/als_user_embeddings.parquet")
    save_parquet(item_embeddings, f"{paths['embeddings']}/als_item_embeddings.parquet")
    
    logger.info("Saving ALS model...")
    als_recommender.save_model(f"{paths['models']}/als_model")
    
    logger.info("ALS training completed successfully!")
    
    return {
        "val_rmse": val_rmse,
        "val_mae": val_mae,
        "test_rmse": test_rmse,
        "test_mae": test_mae
    }


if __name__ == "__main__":
    spark = SparkConfig.create_spark_session(app_name="ALS-Training")
    
    try:
        results = train_als_model(spark)
        print("\n" + "="*50)
        print("Training Results:")
        print("="*50)
        for key, value in results.items():
            print(f"{key}: {value:.4f}")
    finally:
        spark.stop()
