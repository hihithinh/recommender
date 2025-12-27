from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler
from typing import List, Dict, Any
import os


class LightGBMRecommender:
    
    def __init__(
        self,
        num_leaves: int = 31,
        max_depth: int = -1,
        learning_rate: float = 0.05,
        num_iterations: int = 100,
        objective: str = "regression",
        metric: str = "rmse",
        feature_fraction: float = 0.8,
        bagging_fraction: float = 0.8,
        bagging_freq: int = 5,
        min_data_in_leaf: int = 20
    ):
        self.num_leaves = num_leaves
        self.max_depth = max_depth
        self.learning_rate = learning_rate
        self.num_iterations = num_iterations
        self.objective = objective
        self.metric = metric
        self.feature_fraction = feature_fraction
        self.bagging_fraction = bagging_fraction
        self.bagging_freq = bagging_freq
        self.min_data_in_leaf = min_data_in_leaf
        self.model = None
        self.feature_cols = None
    
    def prepare_features(
        self,
        df: DataFrame,
        feature_cols: List[str],
        label_col: str = "rating"
    ) -> DataFrame:
        
        self.feature_cols = feature_cols
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        df_features = assembler.transform(df)
        
        return df_features.select("features", label_col)
    
    def train(self, train_df: DataFrame, feature_cols: List[str], label_col: str = "rating"):
        
        try:
            from synapse.ml.lightgbm import LightGBMRegressor
        except ImportError:
            print("Warning: SynapseML LightGBM not available. Using placeholder.")
            print("Install with: pip install synapseml")
            return None
        
        train_features = self.prepare_features(train_df, feature_cols, label_col)
        
        lgbm = LightGBMRegressor(
            numLeaves=self.num_leaves,
            maxDepth=self.max_depth,
            learningRate=self.learning_rate,
            numIterations=self.num_iterations,
            objective=self.objective,
            metric=self.metric,
            featureFraction=self.feature_fraction,
            baggingFraction=self.bagging_fraction,
            baggingFreq=self.bagging_freq,
            minDataInLeaf=self.min_data_in_leaf,
            labelCol=label_col,
            featuresCol="features",
            predictionCol="prediction"
        )
        
        self.model = lgbm.fit(train_features)
        
        return self.model
    
    def predict(self, test_df: DataFrame) -> DataFrame:
        
        if self.model is None:
            raise ValueError("Model has not been trained yet. Call train() first.")
        
        if self.feature_cols is None:
            raise ValueError("Feature columns not set. Train the model first.")
        
        test_features = self.prepare_features(test_df, self.feature_cols)
        
        predictions = self.model.transform(test_features)
        
        return predictions
    
    def save_model(self, path: str):
        
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
        
        self.model.write().overwrite().save(path)
        print(f"Model saved to {path}")
    
    def load_model(self, path: str):
        
        try:
            from synapse.ml.lightgbm import LightGBMRegressionModel
            self.model = LightGBMRegressionModel.load(path)
            print(f"Model loaded from {path}")
        except ImportError:
            print("Error: SynapseML LightGBM not available.")
