import sys
sys.path.append('/opt/spark-apps')

from pyspark.sql import SparkSession
from config.spark_config import SparkConfig
from config.model_config import DataConfig
from data_processing.data_loader import BookCrossingDataLoader
from data_processing.data_cleaner import DataCleaner
from data_processing.feature_engineering import FeatureEngineering
from utils.spark_utils import save_parquet, print_dataframe_info
from utils.logger import get_default_logger


def main():
    logger = get_default_logger("preprocessing")
    logger.info("Starting data preprocessing pipeline...")
    
    spark = SparkConfig.create_spark_session(app_name="Data-Preprocessing")
    paths = SparkConfig.get_data_paths()
    
    try:
        logger.info("Step 1: Loading raw data...")
        loader = BookCrossingDataLoader(spark, paths['raw_data'])
        data = loader.load_all_data()
        
        ratings_df = data['ratings']
        users_df = data['users']
        books_df = data['books']
        
        print_dataframe_info(ratings_df, "Raw Ratings")
        print_dataframe_info(users_df, "Raw Users")
        print_dataframe_info(books_df, "Raw Books")
        
        logger.info("Step 2: Cleaning data...")
        ratings_clean = DataCleaner.clean_ratings(ratings_df)
        users_clean = DataCleaner.clean_users(users_df)
        books_clean = DataCleaner.clean_books(books_df)
        
        logger.info("Step 3: Filtering by minimum interactions...")
        ratings_filtered = DataCleaner.filter_by_min_interactions(
            ratings_clean,
            min_user_ratings=DataConfig.MIN_RATINGS_PER_USER,
            min_book_ratings=DataConfig.MIN_RATINGS_PER_BOOK
        )
        
        print_dataframe_info(ratings_filtered, "Filtered Ratings")
        
        logger.info("Step 4: Creating user-item indices...")
        ratings_indexed, user_indexer, item_indexer = FeatureEngineering.create_user_item_indices(
            ratings_filtered
        )
        
        logger.info("Step 5: Creating features...")
        user_features = FeatureEngineering.create_user_features(ratings_filtered, users_clean)
        item_features = FeatureEngineering.create_item_features(ratings_filtered, books_clean)
        
        logger.info("Step 6: Saving processed data...")
        save_parquet(ratings_indexed, f"{paths['processed_data']}/ratings.parquet")
        save_parquet(user_features, f"{paths['processed_data']}/users.parquet")
        save_parquet(item_features, f"{paths['processed_data']}/books.parquet")
        
        user_indexer.write().overwrite().save(f"{paths['models']}/user_indexer")
        item_indexer.write().overwrite().save(f"{paths['models']}/item_indexer")
        
        logger.info("Preprocessing completed successfully!")
        
        logger.info("\nFinal Statistics:")
        logger.info(f"Total ratings: {ratings_indexed.count():,}")
        logger.info(f"Unique users: {ratings_indexed.select('user_id').distinct().count():,}")
        logger.info(f"Unique books: {ratings_indexed.select('isbn').distinct().count():,}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
