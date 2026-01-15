from pyspark.sql import DataFrame
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator
from typing import Dict, List
import os


class ALSRecommender:
    
    def __init__(
        self,
        rank: int = 50,
        max_iter: int = 20,
        reg_param: float = 0.1,
        alpha: float = 1.0,
        implicit_prefs: bool = False,
        cold_start_strategy: str = "drop",
        num_user_blocks: int = 10,
        num_item_blocks: int = 10,
        checkpoint_interval: int = 10
    ):
        self.rank = rank
        self.max_iter = max_iter
        self.reg_param = reg_param
        self.alpha = alpha
        self.implicit_prefs = implicit_prefs
        self.cold_start_strategy = cold_start_strategy
        self.num_user_blocks = num_user_blocks
        self.num_item_blocks = num_item_blocks
        self.checkpoint_interval = checkpoint_interval
        self.model = None
    
    def train(self, train_df: DataFrame, user_col: str = "user_index", 
              item_col: str = "item_index", rating_col: str = "rating") -> ALSModel:
        
        als = ALS(
            rank=self.rank,
            maxIter=self.max_iter,
            regParam=self.reg_param,
            alpha=self.alpha,
            implicitPrefs=self.implicit_prefs,
            coldStartStrategy=self.cold_start_strategy,
            numUserBlocks=self.num_user_blocks,
            numItemBlocks=self.num_item_blocks,
            checkpointInterval=self.checkpoint_interval,
            userCol=user_col,
            itemCol=item_col,
            ratingCol=rating_col
        )
        
        self.model = als.fit(train_df)
        
        return self.model
    
    def predict(self, test_df: DataFrame) -> DataFrame:
        
        if self.model is None:
            raise ValueError("Model has not been trained yet. Call train() first.")
        
        predictions = self.model.transform(test_df)
        
        return predictions
    
    def evaluate(self, predictions: DataFrame, metric: str = "rmse") -> float:
        
        evaluator = RegressionEvaluator(
            metricName=metric,
            labelCol="rating",
            predictionCol="prediction"
        )
        
        score = evaluator.evaluate(predictions)
        
        return score
    
    def evaluate_ranking_metrics(
        self,
        predictions: DataFrame,
        user_col: str = "user_id",
        item_col: str = "item_id",
        rating_col: str = "rating",
        prediction_col: str = "prediction",
        k_values: List[int] = [5, 10, 20]
    ) -> Dict[str, float]:
        """
        Evaluate ranking metrics: Precision@K, Recall@K, NDCG@K
        """
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number, col, when, sum as spark_sum, count, log2
        
        relevant_threshold = 4.0
        metrics = {}
        
        for k in k_values:
            # Rank predictions per user
            window_spec = Window.partitionBy(user_col).orderBy(col(prediction_col).desc())
            ranked_predictions = predictions.withColumn("rank", row_number().over(window_spec))
            
            # Filter top-K predictions
            top_k = ranked_predictions.filter(col("rank") <= k)
            
            # Mark relevant items
            top_k = top_k.withColumn(
                "is_relevant",
                when(col(rating_col) >= relevant_threshold, 1).otherwise(0)
            )
            
            # Calculate metrics per user
            user_metrics = top_k.groupBy(user_col).agg(
                spark_sum("is_relevant").alias("relevant_in_top_k"),
                count("*").alias("k_count")
            )
            
            # Get total relevant items per user
            total_relevant = predictions.filter(col(rating_col) >= relevant_threshold) \
                .groupBy(user_col) \
                .agg(count("*").alias("total_relevant"))
            
            user_metrics = user_metrics.join(total_relevant, user_col, "left")
            
            # Precision@K and Recall@K
            user_metrics = user_metrics.withColumn(
                "precision", col("relevant_in_top_k") / k
            ).withColumn(
                "recall",
                when(col("total_relevant") > 0, col("relevant_in_top_k") / col("total_relevant")).otherwise(0)
            )
            
            avg_metrics = user_metrics.agg(
                {"precision": "avg", "recall": "avg"}
            ).collect()[0]
            
            metrics[f'precision@{k}'] = float(avg_metrics['avg(precision)'] or 0.0)
            metrics[f'recall@{k}'] = float(avg_metrics['avg(recall)'] or 0.0)
            
            # Calculate NDCG@K
            top_k_ndcg = top_k.withColumn(
                "relevance",
                when(col(rating_col) >= relevant_threshold, col(rating_col)).otherwise(0)
            )
            
            top_k_ndcg = top_k_ndcg.withColumn(
                "dcg_component",
                col("relevance") / log2(col("rank") + 1)
            )
            
            dcg_per_user = top_k_ndcg.groupBy(user_col).agg(
                spark_sum("dcg_component").alias("dcg")
            )
            
            # IDCG
            window_ideal = Window.partitionBy(user_col).orderBy(col(rating_col).desc())
            ideal_ranked = predictions.withColumn("ideal_rank", row_number().over(window_ideal))
            ideal_top_k = ideal_ranked.filter(col("ideal_rank") <= k)
            
            ideal_top_k = ideal_top_k.withColumn(
                "relevance",
                when(col(rating_col) >= relevant_threshold, col(rating_col)).otherwise(0)
            ).withColumn(
                "idcg_component",
                col("relevance") / log2(col("ideal_rank") + 1)
            )
            
            idcg_per_user = ideal_top_k.groupBy(user_col).agg(
                spark_sum("idcg_component").alias("idcg")
            )
            
            # NDCG = DCG / IDCG
            ndcg_df = dcg_per_user.join(idcg_per_user, user_col, "left")
            ndcg_df = ndcg_df.withColumn(
                "ndcg",
                when(col("idcg") > 0, col("dcg") / col("idcg")).otherwise(0)
            )
            
            avg_ndcg = ndcg_df.agg({"ndcg": "avg"}).collect()[0]['avg(ndcg)']
            metrics[f'ndcg@{k}'] = float(avg_ndcg or 0.0)
        
        return metrics
    
    def evaluate_comprehensive(self, test_df: DataFrame, predictions: DataFrame) -> Dict[str, float]:
        """
        Comprehensive evaluation with multiple metrics:
        - RMSE, MAE, R2, Explained Variance: Regression metrics
        - Precision@K, Recall@K, NDCG@K: Ranking metrics (K=5,10,20)
        
        Reference: Notebook achieved RMSE=0.964, MAE=0.751, R2=0.266, Var=0.272
        """
        metrics = {}
        
        # Regression metrics
        metrics['rmse'] = self.evaluate(predictions, metric='rmse')
        metrics['mae'] = self.evaluate(predictions, metric='mae')
        metrics['r2'] = self.evaluate(predictions, metric='r2')
        metrics['var'] = self.evaluate(predictions, metric='var')
        
        # Ranking metrics
        ranking_metrics = self.evaluate_ranking_metrics(predictions, k_values=[5, 10, 20])
        metrics.update(ranking_metrics)
        
        return metrics
    
    def get_user_embeddings(self) -> DataFrame:
        
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
        
        return self.model.userFactors
    
    def get_item_embeddings(self) -> DataFrame:
        
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
        
        return self.model.itemFactors
    
    def recommend_for_users(self, num_recommendations: int = 10) -> DataFrame:
        
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
        
        return self.model.recommendForAllUsers(num_recommendations)
    
    def recommend_for_items(self, num_recommendations: int = 10) -> DataFrame:
        
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
        
        return self.model.recommendForAllItems(num_recommendations)
    
    def save_model(self, path: str):
        
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
        
        self.model.write().overwrite().save(path)
        print(f"Model saved to {path}")
    
    def load_model(self, path: str):
        
        self.model = ALSModel.load(path)
        print(f"Model loaded from {path}")
