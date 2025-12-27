from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os


class BookCrossingDataLoader:
    
    def __init__(self, spark: SparkSession, raw_data_path: str = "/opt/raw-data"):
        self.spark = spark
        self.raw_data_path = raw_data_path
    
    def load_ratings(self) -> DataFrame:
        
        ratings_path = os.path.join(self.raw_data_path, "BX-Book-Ratings.csv")
        
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("isbn", StringType(), True),
            StructField("rating", IntegerType(), True)
        ])
        
        df = (self.spark.read
              .option("header", "true")
              .option("delimiter", ";")
              .option("quote", '"')
              .option("escape", '"')
              .schema(schema)
              .csv(ratings_path))
        
        return df
    
    def load_users(self) -> DataFrame:
        
        users_path = os.path.join(self.raw_data_path, "BX-Users.csv")
        
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("location", StringType(), True),
            StructField("age", FloatType(), True)
        ])
        
        df = (self.spark.read
              .option("header", "true")
              .option("delimiter", ";")
              .option("quote", '"')
              .option("escape", '"')
              .schema(schema)
              .csv(users_path))
        
        return df
    
    def load_books(self) -> DataFrame:
        
        books_path = os.path.join(self.raw_data_path, "BX_Books.csv")
        
        schema = StructType([
            StructField("isbn", StringType(), True),
            StructField("book_title", StringType(), True),
            StructField("book_author", StringType(), True),
            StructField("year_of_publication", StringType(), True),
            StructField("publisher", StringType(), True),
            StructField("image_url_s", StringType(), True),
            StructField("image_url_m", StringType(), True),
            StructField("image_url_l", StringType(), True)
        ])
        
        df = (self.spark.read
              .option("header", "true")
              .option("delimiter", ";")
              .option("quote", '"')
              .option("escape", '"')
              .schema(schema)
              .csv(books_path))
        
        return df
    
    def load_all_data(self):
        
        return {
            "ratings": self.load_ratings(),
            "users": self.load_users(),
            "books": self.load_books()
        }
