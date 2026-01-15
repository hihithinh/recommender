from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf, col, concat
from pyspark.sql.types import ArrayType, DoubleType
from typing import List, Dict, Any
import os
import sys

sys.path.append('/opt/spark-apps')
from config.model_config import ALSConfig


class LightGBMRecommender:
    
    def __init__(
        self,
        num_leaves: int = 32,
        max_depth: int = -1,
        learning_rate: float = 0.1,
        num_iterations: int = 50,
        objective: str = "regression",
        metric: str = "rmse",
        feature_fraction: float = 0.8,
        bagging_fraction: float = 0.8,
        bagging_freq: int = 5,
        min_data_in_leaf: int = 20,
        boosting_type: str = "gbdt"
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
        self.boosting_type = boosting_type
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
        
        # Use all dimensions from ALS embeddings (rank from ALSConfig)
        embedding_dim = ALSConfig.RANK
        for i in range(embedding_dim):
            df_with_arrays = df_with_arrays.withColumn(f"user_f{i}", col("user_features_array")[i])
        
        for i in range(embedding_dim):
            df_with_arrays = df_with_arrays.withColumn(f"item_f{i}", col("item_features_array")[i])
        
        feature_cols = [f"user_f{i}" for i in range(embedding_dim)] + [f"item_f{i}" for i in range(embedding_dim)]
        
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
            'boosting_type': self.boosting_type,
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
        from pyspark.sql.functions import lit, monotonically_increasing_id
        from pyspark.ml.linalg import DenseVector, SparseVector
        
        if self.model is None:
            raise ValueError("Model has not been trained yet. Call train() first.")
        
        # Add row index to preserve order and join back
        test_df_indexed = test_df.withColumn("_row_id", monotonically_increasing_id())
        
        test_features = self.prepare_features(test_df_indexed, label_col="rating")
        
        # Collect to pandas for LightGBM prediction
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
        test_pd['_row_id'] = range(len(test_pd))
        
        # Convert back to Spark DataFrame
        spark = test_df.sparkSession
        predictions_df = spark.createDataFrame(test_pd[['_row_id', 'prediction']])
        
        # Join with original test_df to get user_id, item_id, rating, and other columns
        # Drop user_features and item_features to avoid ambiguity
        result_df = test_df_indexed.join(predictions_df, "_row_id", "inner") \
            .drop("_row_id", "user_features_array", "item_features_array")
        
        return result_df
    
    def evaluate(self, predictions: DataFrame, metric: str = "rmse") -> float:
        
        from pyspark.ml.evaluation import RegressionEvaluator
        
        evaluator = RegressionEvaluator(
            labelCol="rating",
            predictionCol="prediction",
            metricName=metric
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
        
        Args:
            predictions: DataFrame with user_id, item_id, rating, prediction columns
            user_col: Column name for user ID
            item_col: Column name for item ID
            rating_col: Column name for actual rating
            prediction_col: Column name for predicted rating
            k_values: List of K values to evaluate
            
        Returns:
            Dictionary with precision@k, recall@k, ndcg@k for each k
        """
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number, col, when, sum as spark_sum, count, log2
        
        # Define relevant threshold (e.g., rating >= 4 is relevant)
        relevant_threshold = 4.0
        
        metrics = {}
        
        for k in k_values:
            # Rank predictions per user
            window_spec = Window.partitionBy(user_col).orderBy(col(prediction_col).desc())
            ranked_predictions = predictions.withColumn("rank", row_number().over(window_spec))
            
            # Filter top-K predictions
            top_k = ranked_predictions.filter(col("rank") <= k)
            
            # Mark relevant items (actual rating >= threshold)
            top_k = top_k.withColumn(
                "is_relevant", 
                when(col(rating_col) >= relevant_threshold, 1).otherwise(0)
            )
            
            # Calculate metrics per user
            user_metrics = top_k.groupBy(user_col).agg(
                spark_sum("is_relevant").alias("relevant_in_top_k"),
                count("*").alias("k_count")
            )
            
            # Get total relevant items per user from full predictions
            total_relevant = predictions.filter(col(rating_col) >= relevant_threshold) \
                .groupBy(user_col) \
                .agg(count("*").alias("total_relevant"))
            
            # Join to calculate precision and recall
            user_metrics = user_metrics.join(total_relevant, user_col, "left")
            
            # Precision@K = relevant_in_top_k / k
            # Recall@K = relevant_in_top_k / total_relevant
            user_metrics = user_metrics.withColumn(
                "precision", col("relevant_in_top_k") / k
            ).withColumn(
                "recall", 
                when(col("total_relevant") > 0, col("relevant_in_top_k") / col("total_relevant")).otherwise(0)
            )
            
            # Average across users
            avg_metrics = user_metrics.agg(
                {"precision": "avg", "recall": "avg"}
            ).collect()[0]
            
            metrics[f'precision@{k}'] = float(avg_metrics['avg(precision)'] or 0.0)
            metrics[f'recall@{k}'] = float(avg_metrics['avg(recall)'] or 0.0)
            
            # Calculate NDCG@K
            # Add relevance score and ideal ranking
            top_k_ndcg = top_k.withColumn(
                "relevance", 
                when(col(rating_col) >= relevant_threshold, col(rating_col)).otherwise(0)
            )
            
            # DCG: sum(relevance / log2(rank + 1))
            top_k_ndcg = top_k_ndcg.withColumn(
                "dcg_component",
                col("relevance") / log2(col("rank") + 1)
            )
            
            dcg_per_user = top_k_ndcg.groupBy(user_col).agg(
                spark_sum("dcg_component").alias("dcg")
            )
            
            # IDCG: ideal ranking (sort by actual rating)
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
        Comprehensive evaluation with key metrics:
        - RMSE: Root Mean Squared Error
        - MAE: Mean Absolute Error
        - NDCG@K: Normalized Discounted Cumulative Gain (K=5,10,20)
        """
        metrics = {}
        
        # Regression metrics
        metrics['rmse'] = self.evaluate(predictions, metric='rmse')
        metrics['mae'] = self.evaluate(predictions, metric='mae')
        
        # Ranking metrics - only NDCG
        ranking_metrics = self.evaluate_ranking_metrics(predictions, k_values=[5, 10, 20])
        # Extract only NDCG metrics
        for k in [5, 10, 20]:
            metrics[f'ndcg@{k}'] = ranking_metrics[f'ndcg@{k}']
        
        return metrics
    
    def save_model(self, path: str):
        
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
        
        # LightGBM cannot write directly to HDFS
        # Save to local temp path first, then copy to HDFS if needed
        import os
        
        if path.startswith("hdfs://"):
            # Save to local temp first
            local_temp_path = "/tmp/lightgbm_model.txt"
            self.model.save_model(local_temp_path)
            print(f"Model saved to local temp: {local_temp_path}")
            
            # Copy to HDFS using PySpark
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                
                # Read local file content
                with open(local_temp_path, 'rb') as f:
                    model_bytes = f.read()
                
                # Write to HDFS using Spark's Hadoop FileSystem API
                sc = spark.sparkContext
                hadoop_conf = sc._jsc.hadoopConfiguration()
                fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                    sc._jvm.java.net.URI(path),
                    hadoop_conf
                )
                
                # Create output stream and write
                hdfs_path = sc._jvm.org.apache.hadoop.fs.Path(path)
                output_stream = fs.create(hdfs_path, True)  # True = overwrite
                output_stream.write(model_bytes)
                output_stream.close()
                
                print(f"Model copied to HDFS: {path}")
                
                # Clean up local temp file
                os.remove(local_temp_path)
            except Exception as e:
                print(f"Error copying to HDFS: {e}")
                raise
        else:
            # Save directly to local filesystem
            self.model.save_model(path)
            print(f"Model saved to {path}")
    
    def load_model(self, path: str):
        
        import lightgbm as lgb
        import os
        
        if path.startswith("hdfs://"):
            # Download from HDFS to local temp first
            local_temp_path = "/tmp/lightgbm_model_load.txt"
            
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                
                # Read from HDFS using Spark's Hadoop FileSystem API
                sc = spark.sparkContext
                hadoop_conf = sc._jsc.hadoopConfiguration()
                fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                    sc._jvm.java.net.URI(path),
                    hadoop_conf
                )
                
                # Read from HDFS
                hdfs_path = sc._jvm.org.apache.hadoop.fs.Path(path)
                input_stream = fs.open(hdfs_path)
                
                # Read bytes
                file_status = fs.getFileStatus(hdfs_path)
                file_length = file_status.getLen()
                model_bytes = bytearray(file_length)
                input_stream.readFully(model_bytes)
                input_stream.close()
                
                # Write to local temp
                with open(local_temp_path, 'wb') as f:
                    f.write(model_bytes)
                
                print(f"Model downloaded from HDFS to: {local_temp_path}")
                
                # Load from local temp
                self.model = lgb.Booster(model_file=local_temp_path)
                print(f"Model loaded from {path}")
                
                # Clean up local temp file
                os.remove(local_temp_path)
            except Exception as e:
                print(f"Error downloading from HDFS: {e}")
                raise
        else:
            # Load directly from local filesystem
            self.model = lgb.Booster(model_file=path)
            print(f"Model loaded from {path}")
