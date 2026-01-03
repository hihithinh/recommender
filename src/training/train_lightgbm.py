from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array, lit
import sys
import os

sys.path.append('/opt/spark-apps')

from config.spark_config import SparkConfig
from config.model_config import LightGBMConfig, DataConfig
from models.lightgbm_recommender import LightGBMRecommender
from utils.spark_utils import save_parquet, print_dataframe_info
from utils.logger import get_default_logger


def train_lightgbm_model(spark: SparkSession):
    
    logger = get_default_logger("train_lightgbm")
    logger.info("Starting LightGBM model training...")
    
    paths = SparkConfig.get_data_paths()
    
    logger.info("Loading processed data...")
    train_df = spark.read.parquet(f"{paths['processed_data']}/train_ratings.parquet")
    validation_df = spark.read.parquet(f"{paths['processed_data']}/validation_ratings.parquet")
    test_df = spark.read.parquet(f"{paths['processed_data']}/test_ratings.parquet")
    
    logger.info("Loading ALS embeddings...")
    user_embeddings = spark.read.parquet(f"{paths['embeddings']}/als_user_embeddings.parquet")
    item_embeddings = spark.read.parquet(f"{paths['embeddings']}/als_item_embeddings.parquet")
    
    user_embeddings = user_embeddings.withColumnRenamed("id", "user_index").withColumnRenamed("features", "user_features")
    item_embeddings = item_embeddings.withColumnRenamed("id", "item_index").withColumnRenamed("features", "item_features")
    
    logger.info("Joining embeddings with ratings...")
    train_with_features = train_df \
        .join(user_embeddings, on="user_index", how="inner") \
        .join(item_embeddings, on="item_index", how="inner")
    
    validation_with_features = validation_df \
        .join(user_embeddings, on="user_index", how="inner") \
        .join(item_embeddings, on="item_index", how="inner")
    
    test_with_features = test_df \
        .join(user_embeddings, on="user_index", how="inner") \
        .join(item_embeddings, on="item_index", how="inner")
    
    logger.info(f"Train set with features: {train_with_features.count()}")
    logger.info(f"Validation set with features: {validation_with_features.count()}")
    logger.info(f"Test set with features: {test_with_features.count()}")
    
    logger.info("Initializing LightGBM model...")
    lgbm_recommender = LightGBMRecommender(
        num_leaves=LightGBMConfig.NUM_LEAVES,
        max_depth=LightGBMConfig.MAX_DEPTH,
        learning_rate=LightGBMConfig.LEARNING_RATE,
        num_iterations=LightGBMConfig.NUM_ITERATIONS,
        min_data_in_leaf=LightGBMConfig.MIN_DATA_IN_LEAF,
        bagging_fraction=LightGBMConfig.BAGGING_FRACTION,
        feature_fraction=LightGBMConfig.FEATURE_FRACTION,
        objective=LightGBMConfig.OBJECTIVE
    )
    
    logger.info("Training LightGBM model...")
    model = lgbm_recommender.train(train_with_features, label_col="rating")
    
    logger.info("Evaluating on validation set...")
    val_predictions = lgbm_recommender.predict(validation_with_features)
    val_rmse = lgbm_recommender.evaluate(val_predictions, metric="rmse")
    val_mae = lgbm_recommender.evaluate(val_predictions, metric="mae")
    val_r2 = lgbm_recommender.evaluate(val_predictions, metric="r2")
    
    logger.info(f"Validation RMSE: {val_rmse:.4f}")
    logger.info(f"Validation MAE: {val_mae:.4f}")
    logger.info(f"Validation R2: {val_r2:.4f}")
    
    logger.info("Evaluating on test set...")
    test_predictions = lgbm_recommender.predict(test_with_features)
    test_rmse = lgbm_recommender.evaluate(test_predictions, metric="rmse")
    test_mae = lgbm_recommender.evaluate(test_predictions, metric="mae")
    test_r2 = lgbm_recommender.evaluate(test_predictions, metric="r2")
    
    logger.info(f"Test RMSE: {test_rmse:.4f}")
    logger.info(f"Test MAE: {test_mae:.4f}")
    logger.info(f"Test R2: {test_r2:.4f}")
    
    logger.info("Saving predictions...")
    save_parquet(val_predictions, f"{paths['processed_data']}/lgbm_val_predictions.parquet")
    save_parquet(test_predictions, f"{paths['processed_data']}/lgbm_test_predictions.parquet")
    
    logger.info("Saving LightGBM model...")
    lgbm_recommender.save_model(f"{paths['models']}/lightgbm_model")
    
    logger.info("LightGBM training completed successfully!")
    
    return {
        "val_rmse": val_rmse,
        "val_mae": val_mae,
        "val_r2": val_r2,
        "test_rmse": test_rmse,
        "test_mae": test_mae,
        "test_r2": test_r2
    }


if __name__ == "__main__":
    spark = SparkConfig.create_spark_session(app_name="LightGBM-Training")
    
    try:
        results = train_lightgbm_model(spark)
        print("\n" + "="*50)
        print("Training Results:")
        print("="*50)
        for key, value in results.items():
            print(f"{key}: {value:.4f}")
    finally:
        spark.stop()
