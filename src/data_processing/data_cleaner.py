from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, regexp_replace, isnan, isnull
from typing import Tuple


class DataCleaner:
    
    @staticmethod
    def clean_ratings(df: DataFrame, min_rating: int = 0, max_rating: int = 10) -> DataFrame:
        
        df_cleaned = df.filter(
            (col("rating") >= min_rating) & 
            (col("rating") <= max_rating) &
            col("user_id").isNotNull() &
            col("isbn").isNotNull()
        )
        
        df_cleaned = df_cleaned.dropDuplicates(["user_id", "isbn"])
        
        return df_cleaned
    
    @staticmethod
    def clean_users(df: DataFrame) -> DataFrame:
        
        df_cleaned = df.filter(col("user_id").isNotNull())
        
        df_cleaned = df_cleaned.withColumn(
            "age",
            when(
                (col("age").isNull()) | 
                (col("age") < 5) | 
                (col("age") > 120),
                None
            ).otherwise(col("age"))
        )
        
        df_cleaned = df_cleaned.dropDuplicates(["user_id"])
        
        return df_cleaned
    
    @staticmethod
    def clean_books(df: DataFrame) -> DataFrame:
        
        df_cleaned = df.filter(
            col("isbn").isNotNull() &
            col("book_title").isNotNull()
        )
        
        df_cleaned = df_cleaned.withColumn(
            "book_title",
            trim(col("book_title"))
        )
        
        df_cleaned = df_cleaned.withColumn(
            "book_author",
            trim(col("book_author"))
        )
        
        df_cleaned = df_cleaned.withColumn(
            "year_of_publication",
            when(
                regexp_replace(col("year_of_publication"), "[^0-9]", "").cast("int").between(1800, 2030),
                regexp_replace(col("year_of_publication"), "[^0-9]", "").cast("int")
            ).otherwise(None)
        )
        
        df_cleaned = df_cleaned.dropDuplicates(["isbn"])
        
        return df_cleaned
    
    @staticmethod
    def filter_by_min_interactions(
        ratings_df: DataFrame,
        min_user_ratings: int = 5,
        min_book_ratings: int = 5
    ) -> DataFrame:
        
        from pyspark.sql.functions import count
        from pyspark.sql.window import Window
        
        user_counts = ratings_df.groupBy("user_id").agg(count("*").alias("user_rating_count"))
        book_counts = ratings_df.groupBy("isbn").agg(count("*").alias("book_rating_count"))
        
        filtered_df = (ratings_df
                      .join(user_counts, "user_id")
                      .join(book_counts, "isbn")
                      .filter(
                          (col("user_rating_count") >= min_user_ratings) &
                          (col("book_rating_count") >= min_book_ratings)
                      )
                      .drop("user_rating_count", "book_rating_count"))
        
        return filtered_df
