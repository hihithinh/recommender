from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, count, expr
from pyspark.ml.evaluation import RegressionEvaluator
from typing import Dict, List
import math


class RecommenderMetrics:
    
    @staticmethod
    def calculate_rmse(predictions_df: DataFrame, label_col: str = "rating", 
                       prediction_col: str = "prediction") -> float:
        
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol=label_col,
            predictionCol=prediction_col
        )
        return evaluator.evaluate(predictions_df)
    
    @staticmethod
    def calculate_mae(predictions_df: DataFrame, label_col: str = "rating",
                      prediction_col: str = "prediction") -> float:
        
        evaluator = RegressionEvaluator(
            metricName="mae",
            labelCol=label_col,
            predictionCol=prediction_col
        )
        return evaluator.evaluate(predictions_df)
    
    @staticmethod
    def calculate_r2(predictions_df: DataFrame, label_col: str = "rating",
                     prediction_col: str = "prediction") -> float:
        
        evaluator = RegressionEvaluator(
            metricName="r2",
            labelCol=label_col,
            predictionCol=prediction_col
        )
        return evaluator.evaluate(predictions_df)
    
    @staticmethod
    def calculate_coverage(recommendations_df: DataFrame, total_items: int) -> float:
        
        unique_items = recommendations_df.select("isbn").distinct().count()
        coverage = unique_items / total_items
        return coverage
    
    @staticmethod
    def calculate_diversity(recommendations_df: DataFrame) -> float:
        
        total_recs = recommendations_df.count()
        unique_recs = recommendations_df.select("isbn").distinct().count()
        diversity = unique_recs / total_recs if total_recs > 0 else 0.0
        return diversity
    
    @staticmethod
    def calculate_all_metrics(
        predictions_df: DataFrame,
        label_col: str = "rating",
        prediction_col: str = "prediction"
    ) -> Dict[str, float]:
        
        metrics = {
            "rmse": RecommenderMetrics.calculate_rmse(predictions_df, label_col, prediction_col),
            "mae": RecommenderMetrics.calculate_mae(predictions_df, label_col, prediction_col),
            "r2": RecommenderMetrics.calculate_r2(predictions_df, label_col, prediction_col)
        }
        
        return metrics
