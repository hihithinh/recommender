from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, array
from pyspark.ml.recommendation import ALSModel
import lightgbm as lgb
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationService:
    
    def __init__(self, spark: SparkSession, data_path: str, model_path: str):
        self.spark = spark
        self.data_path = data_path
        self.model_path = model_path
        
        logger.info("Loading models and data...")
        self.als_model = ALSModel.load(f"{model_path}/als_model")
        self.lgbm_model = lgb.Booster(model_file=f"{model_path}/lightgbm_model")
        
        # Load books from CSV since parquet doesn't exist
        self.books_df = spark.read.csv(
            "/opt/raw-data/BX_Books.csv",
            header=True,
            inferSchema=True,
            sep=";",
            quote='"',
            escape='"'
        )
        # Rename columns to match expected schema
        self.books_df = self.books_df.withColumnRenamed("ISBN", "isbn") \
            .withColumnRenamed("Book-Title", "book_title") \
            .withColumnRenamed("Book-Author", "book_author") \
            .withColumnRenamed("Year-Of-Publication", "year_of_publication") \
            .withColumnRenamed("Publisher", "publisher") \
            .withColumnRenamed("Image-URL-M", "image_url_m")
        
        self.ratings_df = spark.read.parquet(f"{data_path}/processed/ratings.parquet")
        
        self.user_indexer = None
        self.item_indexer = None
        self._load_indexers()
        
        logger.info("Service initialized successfully")
    
    def _load_indexers(self):
        from pyspark.ml.feature import StringIndexerModel
        try:
            self.user_indexer = StringIndexerModel.load(f"{self.model_path}/user_indexer")
            self.item_indexer = StringIndexerModel.load(f"{self.model_path}/item_indexer")
        except Exception as e:
            logger.warning(f"Could not load indexers: {e}")
    
    def get_user_recommendations(
        self, 
        user_id: str, 
        top_n: int = 10,
        use_lgbm: bool = True
    ) -> List[Dict[str, Any]]:
        
        try:
            logger.info(f"Starting recommendations for user_id={user_id}, top_n={top_n}, use_lgbm={use_lgbm}")
            user_index_df = self.spark.createDataFrame([(user_id,)], ["user_id"])
            logger.info("Created user_index_df")
            
            if self.user_indexer:
                logger.info("Using user_indexer to transform user_id")
                user_index_df = self.user_indexer.transform(user_index_df)
                user_index = user_index_df.select("user_index").first()[0]
                logger.info(f"Got user_index={user_index}")
            else:
                logger.info("No user_indexer, looking up user_index from ratings_df")
                user_stats = self.ratings_df.filter(col("user_id") == user_id)
                count = user_stats.count()
                logger.info(f"Found {count} ratings for user {user_id}")
                if count == 0:
                    logger.warning(f"User {user_id} not found, returning popular books")
                    return self._get_popular_books(top_n)
                user_index = user_stats.select("user_index").first()[0]
                logger.info(f"Got user_index={user_index}")
            
            logger.info(f"Getting ALS recommendations for user_index={user_index}")
            user_recs = self.als_model.recommendForUserSubset(
                self.spark.createDataFrame([(int(user_index),)], ["user_index"]),
                top_n * 3
            )
            logger.info("Got ALS recommendations")
            
            recommendations = user_recs.select(explode("recommendations").alias("rec")) \
                .select(col("rec.item_index").alias("item_index"), 
                       col("rec.rating").alias("als_score"))
            rec_count = recommendations.count()
            logger.info(f"Exploded recommendations, count={rec_count}")
            
            if rec_count == 0:
                logger.warning(f"No ALS recommendations for user {user_id}, returning popular books")
                return self._get_popular_books(top_n)
            
            if use_lgbm:
                logger.info("Applying LightGBM re-ranking")
                recommendations = self._apply_lgbm_reranking(
                    user_id, 
                    int(user_index), 
                    recommendations
                )
                logger.info("LightGBM re-ranking complete")
            
            logger.info("Joining with ratings_df to get ISBN")
            recommendations = recommendations.join(
                self.ratings_df.select("item_index", "isbn").distinct(),
                "item_index",
                "left"
            )
            logger.info("Joined with ratings_df")
            
            logger.info("Joining with books_df to get book details")
            recommendations = recommendations.join(
                self.books_df,
                "isbn",
                "left"
            )
            logger.info("Joined with books_df")
            
            recommendations = recommendations.limit(top_n)
            logger.info(f"Limited to top {top_n} recommendations")
            
            results = []
            rows = recommendations.collect()
            logger.info(f"Collected {len(rows)} recommendation rows")
            for row in rows:
                results.append({
                    "isbn": row.isbn,
                    "title": row.book_title if hasattr(row, 'book_title') else "Unknown",
                    "author": row.book_author if hasattr(row, 'book_author') else "Unknown",
                    "year": int(row.year_of_publication) if hasattr(row, 'year_of_publication') and row.year_of_publication else None,
                    "publisher": row.publisher if hasattr(row, 'publisher') else "Unknown",
                    "als_score": float(row.als_score) if hasattr(row, 'als_score') else 0.0,
                    "lgbm_score": float(row.lgbm_score) if hasattr(row, 'lgbm_score') else None,
                    "image_url": row.image_url_m if hasattr(row, 'image_url_m') else None
                })
            
            logger.info(f"Returning {len(results)} recommendations")
            return results
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}", exc_info=True)
            logger.info("Falling back to popular books")
            return self._get_popular_books(top_n)
    
    def _apply_lgbm_reranking(self, user_id: str, user_index: int, recommendations):
        logger.info(f"Starting LightGBM re-ranking for user_index={user_index}")
        
        user_embeddings = self.als_model.userFactors.filter(col("id") == user_index)
        logger.info("Got user embeddings")
        item_embeddings = self.als_model.itemFactors
        logger.info("Got item embeddings")
        
        logger.info("Joining recommendations with embeddings")
        recs_with_embeddings = recommendations \
            .crossJoin(user_embeddings.select(col("features").alias("user_features"))) \
            .join(
                item_embeddings.select(
                    col("id").alias("item_index"),
                    col("features").alias("item_features")
                ),
                "item_index"
            )
        logger.info("Joined embeddings")
        
        logger.info("Converting to pandas DataFrame")
        recs_pd = recs_with_embeddings.select(
            "item_index", "als_score", "user_features", "item_features"
        ).toPandas()
        logger.info(f"Converted to pandas, shape={recs_pd.shape}")
        
        from pyspark.ml.linalg import DenseVector, SparseVector
        
        def convert_vector(v):
            if isinstance(v, (DenseVector, SparseVector)):
                return v.toArray().tolist()
            elif isinstance(v, list):
                return v
            else:
                return list(v)
        
        user_features = [convert_vector(v) for v in recs_pd['user_features']]
        item_features = [convert_vector(v) for v in recs_pd['item_features']]
        
        X = []
        for uf, if_ in zip(user_features, item_features):
            X.append(uf + if_)
        
        X_df = pd.DataFrame(X).astype(np.float64)
        logger.info(f"Created feature matrix, shape={X_df.shape}")
        
        logger.info("Predicting with LightGBM model")
        lgbm_scores = self.lgbm_model.predict(X_df)
        logger.info(f"Got LightGBM predictions, shape={lgbm_scores.shape}")
        
        recs_pd['lgbm_score'] = lgbm_scores
        logger.info("Added LightGBM scores to DataFrame")
        
        recs_pd = recs_pd.sort_values('lgbm_score', ascending=False)
        logger.info("Sorted by LightGBM score")
        
        logger.info("Converting back to Spark DataFrame")
        result_df = self.spark.createDataFrame(
            recs_pd[['item_index', 'als_score', 'lgbm_score']]
        )
        logger.info("Converted back to Spark DataFrame")
        
        return result_df
    
    def _get_popular_books(self, top_n: int) -> List[Dict[str, Any]]:
        
        popular = self.ratings_df.groupBy("isbn") \
            .agg(
                {"rating": "avg", "isbn": "count"}
            ) \
            .withColumnRenamed("avg(rating)", "avg_rating") \
            .withColumnRenamed("count(isbn)", "rating_count") \
            .filter(col("rating_count") >= 10) \
            .orderBy(col("avg_rating").desc()) \
            .limit(top_n)
        
        popular = popular.join(self.books_df, "isbn", "left")
        
        results = []
        for row in popular.collect():
            results.append({
                "isbn": row.isbn,
                "title": row.book_title if hasattr(row, 'book_title') else "Unknown",
                "author": row.book_author if hasattr(row, 'book_author') else "Unknown",
                "year": int(row.year_of_publication) if hasattr(row, 'year_of_publication') and row.year_of_publication else None,
                "publisher": row.publisher if hasattr(row, 'publisher') else "Unknown",
                "avg_rating": float(row.avg_rating),
                "rating_count": int(row.rating_count),
                "image_url": row.image_url_m if hasattr(row, 'image_url_m') else None,
                "als_score": None,
                "lgbm_score": None
            })
        
        return results
    
    def get_user_history(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        
        user_ratings = self.ratings_df.filter(col("user_id") == user_id) \
            .orderBy(col("rating").desc()) \
            .limit(limit)
        
        user_ratings = user_ratings.join(self.books_df, "isbn", "left")
        
        results = []
        for row in user_ratings.collect():
            results.append({
                "isbn": row.isbn,
                "title": row.book_title if hasattr(row, 'book_title') else "Unknown",
                "author": row.book_author if hasattr(row, 'book_author') else "Unknown",
                "rating": float(row.rating),
                "image_url": row.image_url_m if hasattr(row, 'image_url_m') else None
            })
        
        return results
    
    def get_recommendations_for_new_user(
        self,
        age: int = None,
        location: str = None,
        favorite_genres: List[str] = None,
        favorite_authors: List[str] = None,
        top_n: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get recommendations for a new user based on their profile.
        Uses content-based filtering and popular books in their demographic.
        """
        logger.info(f"Getting recommendations for new user: age={age}, location={location}")
        
        try:
            # Start with popular books
            recommendations = self._get_popular_books(top_n * 3)
            
            # Filter by favorite authors if provided
            if favorite_authors and len(favorite_authors) > 0:
                logger.info(f"Filtering by favorite authors: {favorite_authors}")
                author_books = []
                for author in favorite_authors:
                    author_results = self.books_df.filter(
                        col("book_author").contains(author)
                    ).limit(5)
                    
                    for row in author_results.collect():
                        # Get average rating for this book
                        book_ratings = self.ratings_df.filter(col("isbn") == row.isbn)
                        if book_ratings.count() > 0:
                            avg_rating = book_ratings.agg({"rating": "avg"}).first()[0]
                            rating_count = book_ratings.count()
                        else:
                            avg_rating = 0
                            rating_count = 0
                        
                        author_books.append({
                            "isbn": row.isbn,
                            "title": row.book_title if hasattr(row, 'book_title') else "Unknown",
                            "author": row.book_author if hasattr(row, 'book_author') else "Unknown",
                            "year": int(row.year_of_publication) if hasattr(row, 'year_of_publication') and row.year_of_publication else None,
                            "publisher": row.publisher if hasattr(row, 'publisher') else "Unknown",
                            "avg_rating": float(avg_rating) if avg_rating else 0.0,
                            "rating_count": int(rating_count),
                            "image_url": row.image_url_m if hasattr(row, 'image_url_m') else None,
                            "reason": f"By your favorite author: {row.book_author if hasattr(row, 'book_author') else 'Unknown'}",
                            "als_score": None,
                            "lgbm_score": None
                        })
                
                # Sort by rating and add to recommendations
                author_books.sort(key=lambda x: (x['avg_rating'], x['rating_count']), reverse=True)
                recommendations = author_books[:top_n] + recommendations
            
            # Add demographic-based recommendations if age provided
            if age:
                logger.info(f"Adding age-based recommendations for age {age}")
                # Simple age-based heuristics
                if age < 18:
                    # Young adult books
                    ya_keywords = ["Harry Potter", "Twilight", "Hunger Games", "Percy Jackson"]
                elif age < 30:
                    # Contemporary fiction
                    ya_keywords = ["Fiction", "Romance", "Mystery"]
                elif age < 50:
                    # Mature fiction
                    ya_keywords = ["Thriller", "Biography", "History"]
                else:
                    # Classic literature
                    ya_keywords = ["Classic", "Literature", "Historical"]
            
            # Remove duplicates by ISBN
            seen_isbns = set()
            unique_recommendations = []
            for rec in recommendations:
                if rec['isbn'] not in seen_isbns:
                    seen_isbns.add(rec['isbn'])
                    unique_recommendations.append(rec)
                    if len(unique_recommendations) >= top_n:
                        break
            
            logger.info(f"Returning {len(unique_recommendations)} recommendations for new user")
            return unique_recommendations
            
        except Exception as e:
            logger.error(f"Error getting new user recommendations: {e}", exc_info=True)
            return self._get_popular_books(top_n)
    
    def search_books(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        
        query_lower = query.lower()
        
        results_df = self.books_df.filter(
            col("book_title").contains(query) | 
            col("book_author").contains(query)
        ).limit(limit)
        
        results = []
        for row in results_df.collect():
            results.append({
                "isbn": row.isbn,
                "title": row.book_title if hasattr(row, 'book_title') else "Unknown",
                "author": row.book_author if hasattr(row, 'book_author') else "Unknown",
                "year": int(row.year_of_publication) if hasattr(row, 'year_of_publication') and row.year_of_publication else None,
                "publisher": row.publisher if hasattr(row, 'publisher') else "Unknown",
                "image_url": row.image_url_m if hasattr(row, 'image_url_m') else None
            })
        
        return results
