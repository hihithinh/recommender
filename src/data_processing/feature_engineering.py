from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, avg, count, stddev
from pyspark.ml.feature import StringIndexer, VectorAssembler
from typing import Tuple


class FeatureEngineering:
    
    @staticmethod
    def create_user_item_indices(ratings_df: DataFrame) -> Tuple[DataFrame, object, object]:
        
        user_indexer = StringIndexer(inputCol="user_id", outputCol="user_index", handleInvalid="keep")
        user_indexer_model = user_indexer.fit(ratings_df)
        ratings_indexed = user_indexer_model.transform(ratings_df)
        
        item_indexer = StringIndexer(inputCol="isbn", outputCol="item_index", handleInvalid="keep")
        item_indexer_model = item_indexer.fit(ratings_indexed)
        ratings_indexed = item_indexer_model.transform(ratings_indexed)
        
        return ratings_indexed, user_indexer_model, item_indexer_model
    
    @staticmethod
    def create_user_features(ratings_df: DataFrame, users_df: DataFrame) -> DataFrame:
        
        user_stats = ratings_df.groupBy("user_id").agg(
            count("rating").alias("num_ratings"),
            avg("rating").alias("avg_rating"),
            stddev("rating").alias("stddev_rating")
        )
        
        user_features = users_df.join(user_stats, "user_id", "left")
        
        user_features = user_features.fillna({
            "num_ratings": 0,
            "avg_rating": 0.0,
            "stddev_rating": 0.0,
            "age": 0.0
        })
        
        return user_features
    
    @staticmethod
    def create_item_features(ratings_df: DataFrame, books_df: DataFrame) -> DataFrame:
        
        item_stats = ratings_df.groupBy("isbn").agg(
            count("rating").alias("num_ratings"),
            avg("rating").alias("avg_rating"),
            stddev("rating").alias("stddev_rating")
        )
        
        item_features = books_df.join(item_stats, "isbn", "left")
        
        item_features = item_features.fillna({
            "num_ratings": 0,
            "avg_rating": 0.0,
            "stddev_rating": 0.0,
            "year_of_publication": 2000
        })
        
        return item_features
    
    @staticmethod
    def create_interaction_features(
        ratings_df: DataFrame,
        user_features: DataFrame,
        item_features: DataFrame
    ) -> DataFrame:
        
        interaction_df = (ratings_df
                         .join(user_features.select("user_id", "avg_rating").withColumnRenamed("avg_rating", "user_avg_rating"), "user_id")
                         .join(item_features.select("isbn", "avg_rating").withColumnRenamed("avg_rating", "item_avg_rating"), "isbn"))
        
        interaction_df = interaction_df.withColumn(
            "rating_diff_user",
            col("rating") - col("user_avg_rating")
        )
        
        interaction_df = interaction_df.withColumn(
            "rating_diff_item",
            col("rating") - col("item_avg_rating")
        )
        
        return interaction_df
