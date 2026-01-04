from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf, col, concat
from pyspark.sql.types import ArrayType, DoubleType
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
    
    def prepare_features(
        self,
        df: DataFrame,
        label_col: str = "rating"
    ) -> DataFrame:
        
        from pyspark.ml.linalg import DenseVector, SparseVector
        
        def vector_to_array_func(v):
            if v is None:
                return None
            elif isinstance(v, (DenseVector, SparseVector)):
                return v.toArray().tolist()
            elif isinstance(v, list):
                return v
            else:
                return list(v)
        
        vector_to_array = udf(vector_to_array_func, ArrayType(DoubleType()))
        
        df_with_arrays = df \
            .withColumn("user_features_array", vector_to_array(col("user_features"))) \
            .withColumn("item_features_array", vector_to_array(col("item_features")))
        
        # Use all 50 dimensions from ALS embeddings
        for i in range(50):
            df_with_arrays = df_with_arrays.withColumn(f"user_f{i}", col("user_features_array")[i])
        
        for i in range(50):
            df_with_arrays = df_with_arrays.withColumn(f"item_f{i}", col("item_features_array")[i])
        
        feature_cols = [f"user_f{i}" for i in range(50)] + [f"item_f{i}" for i in range(50)]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        df_features = assembler.transform(df_with_arrays)
        
        return df_features.select("features", label_col)
    
    def train(self, train_df: DataFrame, label_col: str = "rating"):
        
        import lightgbm as lgb
        import pandas as pd
        import numpy as np
        from pyspark.ml.linalg import DenseVector, SparseVector
        
        train_features = self.prepare_features(train_df, label_col)
        
        train_pd = train_features.select("features", label_col).toPandas()
        
        def convert_vector(v):
            if isinstance(v, (DenseVector, SparseVector)):
                return v.toArray().tolist()
            elif isinstance(v, list):
                return v
            else:
                return list(v)
        
        X_train = pd.DataFrame([convert_vector(v) for v in train_pd['features']]).astype(np.float64)
        y_train = train_pd[label_col].astype(np.float64)
        
        params = {
            'objective': self.objective,
            'metric': self.metric,
            'num_leaves': self.num_leaves,
            'max_depth': self.max_depth,
            'learning_rate': self.learning_rate,
            'feature_fraction': self.feature_fraction,
            'bagging_fraction': self.bagging_fraction,
            'bagging_freq': self.bagging_freq,
            'min_data_in_leaf': self.min_data_in_leaf,
            'verbose': -1
        }
        
        train_data = lgb.Dataset(X_train, label=y_train)
        
        self.model = lgb.train(
            params,
            train_data,
            num_boost_round=self.num_iterations
        )
        
        return self.model
    
    def predict(self, test_df: DataFrame) -> DataFrame:
        
        import pandas as pd
        import numpy as np
        from pyspark.sql.functions import lit
        from pyspark.ml.linalg import DenseVector, SparseVector
        
        if self.model is None:
            raise ValueError("Model has not been trained yet. Call train() first.")
        
        test_features = self.prepare_features(test_df, label_col="rating")
        
        test_pd = test_features.select("features", "rating").toPandas()
        
        def convert_vector(v):
            if isinstance(v, (DenseVector, SparseVector)):
                return v.toArray().tolist()
            elif isinstance(v, list):
                return v
            else:
                return list(v)
        
        X_test = pd.DataFrame([convert_vector(v) for v in test_pd['features']]).astype(np.float64)
        
        predictions_array = self.model.predict(X_test)
        
        test_pd['prediction'] = predictions_array
        
        predictions_df = test_df.sql_ctx.createDataFrame(test_pd)
        
        return predictions_df
    
    def evaluate(self, predictions: DataFrame, metric: str = "rmse") -> float:
        
        from pyspark.ml.evaluation import RegressionEvaluator
        
        evaluator = RegressionEvaluator(
            labelCol="rating",
            predictionCol="prediction",
            metricName=metric
        )
        
        score = evaluator.evaluate(predictions)
        
        return score
    
    def save_model(self, path: str):
        
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
        
        # LightGBM cannot write directly to HDFS
        # Save to local temp path first, then copy to HDFS if needed
        import os
        import subprocess
        
        if path.startswith("hdfs://"):
            # Extract HDFS path and save to local temp
            local_temp_path = "/tmp/lightgbm_model.txt"
            self.model.save_model(local_temp_path)
            print(f"Model saved to local temp: {local_temp_path}")
            
            # Copy to HDFS using hadoop fs command
            try:
                subprocess.run(
                    ["hadoop", "fs", "-put", "-f", local_temp_path, path],
                    check=True,
                    capture_output=True,
                    text=True
                )
                print(f"Model copied to HDFS: {path}")
                
                # Clean up local temp file
                os.remove(local_temp_path)
            except subprocess.CalledProcessError as e:
                print(f"Error copying to HDFS: {e.stderr}")
                raise
        else:
            # Save directly to local filesystem
            self.model.save_model(path)
            print(f"Model saved to {path}")
    
    def load_model(self, path: str):
        
        import lightgbm as lgb
        import os
        import subprocess
        
        if path.startswith("hdfs://"):
            # Download from HDFS to local temp first
            local_temp_path = "/tmp/lightgbm_model_load.txt"
            
            try:
                subprocess.run(
                    ["hadoop", "fs", "-get", "-f", path, local_temp_path],
                    check=True,
                    capture_output=True,
                    text=True
                )
                print(f"Model downloaded from HDFS to: {local_temp_path}")
                
                # Load from local temp
                self.model = lgb.Booster(model_file=local_temp_path)
                print(f"Model loaded from {path}")
                
                # Clean up local temp file
                os.remove(local_temp_path)
            except subprocess.CalledProcessError as e:
                print(f"Error downloading from HDFS: {e.stderr}")
                raise
        else:
            # Load directly from local filesystem
            self.model = lgb.Booster(model_file=path)
            print(f"Model loaded from {path}")
