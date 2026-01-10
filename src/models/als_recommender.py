from pyspark.sql import DataFrame
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator
from typing import Dict
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
    
    def evaluate_comprehensive(self, test_df: DataFrame, predictions: DataFrame) -> Dict[str, float]:
        """
        Comprehensive evaluation with multiple metrics from als_deep_dive.ipynb:
        - RMSE: Root Mean Squared Error
        - MAE: Mean Absolute Error  
        - R2: R-squared score
        - Explained Variance: Explained variance score
        
        Reference: Notebook achieved RMSE=0.964, MAE=0.751, R2=0.266, Var=0.272
        """
        metrics = {}
        
        metrics['rmse'] = self.evaluate(predictions, metric='rmse')
        metrics['mae'] = self.evaluate(predictions, metric='mae')
        metrics['r2'] = self.evaluate(predictions, metric='r2')
        metrics['var'] = self.evaluate(predictions, metric='var')
        
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
