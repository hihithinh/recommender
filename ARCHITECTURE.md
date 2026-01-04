# Book Recommender System - Architecture

## Tổng quan hệ thống

Hệ thống gợi ý sách phân tán sử dụng Apache Spark, Kafka, và Machine Learning (ALS + LightGBM).

```
┌─────────────────────────────────────────────────────────────────┐
│                     DISTRIBUTED CLUSTER                          │
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Machine 1  │  │   Machine 2  │  │   Machine 3  │          │
│  │              │  │              │  │              │          │
│  │ Spark Master │  │ Spark Worker │  │ Spark Worker │          │
│  │ Kafka        │  │ Spark Worker │  │ Spark Worker │          │
│  │ Jupyter      │  │              │  │              │          │
│  │ API Server   │  │              │  │              │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│         │                  │                  │                  │
│         └──────────────────┴──────────────────┘                  │
│                  Tailscale VPN Network                           │
└─────────────────────────────────────────────────────────────────┘
```

### Đặc điểm kiến trúc

- **Flexible Deployment**: Mỗi máy có thể chạy Master, Worker, hoặc cả hai
- **Scalable**: Thêm workers để tăng processing power
- **Secure**: Kết nối qua Tailscale VPN
- **Cross-platform**: Mac, Windows, Linux

## Data Pipeline

```
Raw Data (CSV) → Preprocessing → Parquet (Data Lake)
                                       ↓
                    ┌──────────────────┴──────────────────┐
                    ↓                                      ↓
            Training Pipeline                     Serving Pipeline
                    ↓                                      ↓
            ALS + LightGBM Models                  API Server
                    ↓                                      ↓
            Model Storage                          Recommendations
```

---

## TRAINING PIPELINE

### 1. Data Ingestion

**Kafka Streaming** (Real-time)
- Topics: `book-ratings`, `user-events`
- Format: JSON messages
- Location: `src/streaming/` (nếu có)

**Batch Upload** (Historical data)
- Raw CSV files: `./data/raw/`
  - `BX-Book-Ratings.csv`
  - `BX-Users.csv`
  - `BX-Books.csv`

### 2. Data Processing

**Module**: `src/data_processing/`

**Components**:
- `data_loader.py`: Load raw CSV files
- `data_cleaner.py`: Clean và validate data
- `feature_engineering.py`: Tạo features cho models

**Output**: 
- `./data/processed/ratings.parquet`
- `./data/processed/users.parquet`
- `./data/processed/books.parquet`

**Paths** (trong Docker):
```python
# src/config/spark_config.py
{
    "raw_data": "hdfs://{master_ip}:9000/data/raw",
    "processed_data": "hdfs://{master_ip}:9000/data/processed",
    "embeddings": "hdfs://{master_ip}:9000/data/embeddings",
    "models": "hdfs://{master_ip}:9000/data/models"
}
```

### 3. Model Training

#### 3.1 ALS Model (Collaborative Filtering)

**File**: `src/training/train_als.py`

**Algorithm**: Spark MLlib ALS (Alternating Least Squares)

**Input**: 
- Processed ratings: `user_index`, `item_index`, `rating`

**Hyperparameters** (`src/config/model_config.py`):
```python
class ALSConfig:
    RANK = 50                    # Latent factors
    MAX_ITER = 20                # Training iterations
    REG_PARAM = 0.1              # Regularization
    ALPHA = 1.0                  # Confidence weight
    IMPLICIT_PREFS = False       # Explicit ratings
    COLD_START_STRATEGY = "drop"
    NUM_USER_BLOCKS = 10
    NUM_ITEM_BLOCKS = 10
```

**Output**:
- Model: `./data/models/als_model/`
- User embeddings: `./data/embeddings/als_user_embeddings.parquet`
- Item embeddings: `./data/embeddings/als_item_embeddings.parquet`

**Evaluation Metrics**:
- RMSE (Root Mean Square Error)
- MAE (Mean Absolute Error)

**Training Command**:
```bash
docker exec spark-master spark-submit \
  --master spark://<MASTER_IP>:7077 \
  /opt/spark-apps/training/train_als.py
```

#### 3.2 LightGBM Model (Gradient Boosting)

**File**: `src/training/train_lightgbm.py`

**Algorithm**: LightGBM via SynapseML

**Input**:
- ALS embeddings (user + item)
- User features (age, location)
- Item features (author, year, publisher)
- Interaction features

**Hyperparameters** (`src/config/model_config.py`):
```python
class LightGBMConfig:
    NUM_LEAVES = 31
    LEARNING_RATE = 0.05
    NUM_ITERATIONS = 100
    MAX_DEPTH = -1
    MIN_DATA_IN_LEAF = 20
    FEATURE_FRACTION = 0.8
    BAGGING_FRACTION = 0.8
    BAGGING_FREQ = 5
```

**Output**:
- Model: `./data/models/lightgbm_model/`

**Training Command**:
```bash
docker exec spark-master spark-submit \
  --master spark://<MASTER_IP>:7077 \
  --packages com.microsoft.azure:synapseml_2.12:0.11.3 \
  /opt/spark-apps/training/train_lightgbm.py
```

### 4. Model Evaluation

**Module**: `src/evaluation/metrics.py`

**Metrics**:
- RMSE, MAE (rating prediction)
- Precision@K, Recall@K (ranking)
- NDCG (Normalized Discounted Cumulative Gain)
- Coverage (catalog coverage)

---

## SERVING PIPELINE

### 1. API Server

**Framework**: Flask + PySpark

**File**: `src/api/app.py`

**Endpoints**:

#### GET `/`
- Web UI homepage
- Template: `templates/index.html`

#### POST `/api/recommend`
Request:
```json
{
  "user_id": "12345",
  "top_n": 10,
  "use_lgbm": true
}
```

Response:
```json
{
  "user_id": "12345",
  "recommendations": [
    {
      "isbn": "0439136350",
      "title": "Harry Potter and the Prisoner of Azkaban",
      "author": "J.K. Rowling",
      "year": 1999,
      "score": 8.5
    }
  ],
  "user_history": [...],
  "count": 10
}
```

#### GET `/api/search?q=harry+potter&limit=20`
- Search books by title/author
- Returns matching books

#### GET `/api/popular?top_n=20`
- Get most popular books
- Based on rating count and average rating

#### POST `/api/recommend/new-user`
Request:
```json
{
  "age": 25,
  "location": "New York",
  "favorite_authors": ["J.K. Rowling"],
  "favorite_genres": ["Fantasy"],
  "top_n": 10
}
```

- Cold-start recommendations
- Based on demographics and preferences

#### GET `/api/health`
- Health check endpoint
- Returns service status

**Start Command**:
```bash
docker compose -f docker-compose.cluster.yml up -d api-server
```

**Access**: `http://<MASTER_IP>:5001`

### 2. Recommendation Service

**File**: `src/api/recommendation_service.py`

**Class**: `RecommendationService`

**Methods**:
- `get_user_recommendations()`: Generate recommendations for existing user
- `get_recommendations_for_new_user()`: Cold-start recommendations
- `search_books()`: Search functionality
- `get_user_history()`: User's past ratings

**Model Loading**:
```python
# Load ALS model
als_model = ALSModel.load(f"{model_path}/als_model")

# Load LightGBM model (if available)
lgbm_model = LightGBMModel.load(f"{model_path}/lightgbm_model")
```

**Recommendation Strategy**:
1. **ALS only**: Fast, collaborative filtering
2. **ALS + LightGBM**: Slower, more accurate (uses features)

### 3. Model Classes

#### ALS Recommender

**File**: `src/models/als_recommender.py`

**Class**: `ALSRecommender`

**Methods**:
- `train(train_df)`: Train ALS model
- `predict(test_df)`: Generate predictions
- `evaluate(predictions, metric)`: Calculate metrics
- `get_user_embeddings()`: Extract user factors
- `get_item_embeddings()`: Extract item factors
- `recommend_for_user(user_id, n)`: Top-N recommendations
- `save_model(path)`: Save trained model
- `load_model(path)`: Load saved model

#### LightGBM Recommender

**File**: `src/models/lightgbm_recommender.py`

**Class**: `LightGBMRecommender`

**Methods**:
- `train(train_df, features)`: Train LightGBM
- `predict(test_df)`: Generate predictions
- `evaluate(predictions, metric)`: Calculate metrics
- `save_model(path)`: Save trained model
- `load_model(path)`: Load saved model

---

## Configuration

### Spark Configuration

**File**: `src/config/spark_config.py`

```python
class SparkConfig:
    @staticmethod
    def create_spark_session(
        app_name="BookCrossing-Recommender",
        master="spark://spark-master:7077",
        executor_memory="4g",
        executor_cores=2,
        driver_memory="2g"
    )
```

**Key Settings**:
- Adaptive Query Execution: Enabled
- Serializer: Kryo (faster)
- Compression: Snappy (Parquet)
- Shuffle partitions: 200
- Network timeout: 600s

### Model Configuration

**File**: `src/config/model_config.py`

**Classes**:
- `ALSConfig`: ALS hyperparameters
- `LightGBMConfig`: LightGBM hyperparameters
- `DataConfig`: Train/val/test split ratios

---

## Utilities

### Spark Utils

**File**: `src/utils/spark_utils.py`

**Functions**:
- `save_parquet()`: Save DataFrame to Parquet
- `load_parquet()`: Load Parquet to DataFrame
- `print_dataframe_info()`: Display DataFrame stats

### Logger

**File**: `src/utils/logger.py`

**Function**: `get_default_logger(name)`

Centralized logging configuration.

---

## Data Storage Structure

```
./data/                          # Processed data (on master machine)
  ├── processed/
  │   ├── ratings.parquet        # Cleaned ratings
  │   ├── users.parquet          # User features
  │   ├── books.parquet          # Book features
  │   ├── train_ratings.parquet
  │   ├── validation_ratings.parquet
  │   └── test_ratings.parquet
  ├── embeddings/
  │   ├── als_user_embeddings.parquet
  │   └── als_item_embeddings.parquet
  └── models/
      ├── als_model/             # Spark ALS model
      └── lightgbm_model/        # LightGBM model

./data/raw/                      # Raw CSV files
  ├── BX-Book-Ratings.csv
  ├── BX-Users.csv
  └── BX-Books.csv
```

---

## Docker Volumes Mapping

**In `docker-compose.cluster.yml`**:

```yaml
# Spark Master & API Server
volumes:
  - ./data:/opt/spark-data           # Processed data, models
  - ./data:/data                     # Parquet files (readable from host)
  - ./data/raw:/opt/raw-data         # Raw CSV files
  - ./src:/opt/spark-apps            # Source code
  - ./scripts:/opt/scripts           # Helper scripts
  - ./hdfs:/hadoop/dfs               # HDFS internal data

# Spark Workers
volumes:
  - ./src:/opt/spark-apps            # Source code (read-only)
  - ./scripts:/opt/scripts           # Helper scripts
  - worker-X-cache:/opt/spark-data   # Local cache
```

**Path trong code phải match với Docker volumes!**

---

## Deployment Flow

### Training Phase

1. **Upload data** → `./data/raw/` (then upload to HDFS with `./scripts/upload_to_hdfs.sh`)
2. **Preprocessing** → `docker exec spark-master python3 /opt/scripts/run_preprocessing.py`
3. **Train ALS** → `docker exec spark-master spark-submit /opt/spark-apps/training/train_als.py`
4. **Train LightGBM** → `docker exec spark-master spark-submit /opt/spark-apps/training/train_lightgbm.py`
5. **Models saved** → `./data/models/`

### Serving Phase

1. **Start API Server** → `docker compose -f docker-compose.cluster.yml up -d api-server`
2. **API loads models** from `./data/models/`
3. **API ready** at `http://<MASTER_IP>:5001`
4. **Make requests** via REST API or Web UI

---

## Performance Optimization

### Training

1. **Data Partitioning**: Adjust `spark.sql.shuffle.partitions` based on data size
2. **Caching**: Cache frequently accessed DataFrames
3. **Checkpointing**: Enable for iterative algorithms (ALS)
4. **Resource Allocation**: Increase executor memory/cores for large datasets

### Serving

1. **Model Caching**: Keep models in memory
2. **Batch Predictions**: Process multiple users at once
3. **Pre-computation**: Pre-compute popular recommendations
4. **Connection Pooling**: Reuse Spark sessions

---

## Monitoring

### Spark UIs

- **Master UI**: `http://<MASTER_IP>:8080` - Cluster status, workers
- **Worker UI**: `http://<WORKER_IP>:8081` - Worker resources, executors
- **Application UI**: `http://<MASTER_IP>:4040` - Job details, stages, tasks

### Kafka UI

- **Kafka UI**: `http://<MASTER_IP>:8090` - Topics, messages, consumers

### Logs

```bash
# Application logs
docker logs spark-master
docker logs spark-worker-1
docker logs api-server

# Spark logs
./logs/spark/
```

---

## Scaling Strategy

### Horizontal Scaling (Add Workers)

```bash
# On new machine
# Edit .env: WORKER_ID=4, THIS_MACHINE_IP=..., MASTER_TAILSCALE_IP=...
docker compose -f docker-compose.cluster.yml up -d spark-worker
```

### Vertical Scaling (Increase Resources)

```bash
# Edit .env
SPARK_WORKER_CORES=8
SPARK_WORKER_MEMORY=16g

# Restart
docker compose -f docker-compose.cluster.yml restart spark-worker
```

---

## Security

- ✅ **Tailscale VPN**: All traffic encrypted
- ✅ **No public ports**: Only accessible within Tailscale network
- ✅ **Authentication**: Can add API keys to Flask endpoints
- ✅ **Data isolation**: Each worker has isolated cache

---

## Future Enhancements

### Training Pipeline
- [ ] Hyperparameter tuning (Grid Search, Bayesian Optimization)
- [ ] Model versioning (MLflow)
- [ ] A/B testing framework
- [ ] Online learning (incremental updates)

### Serving Pipeline
- [ ] Caching layer (Redis)
- [ ] Load balancing (multiple API servers)
- [ ] Rate limiting
- [ ] User feedback loop

### Infrastructure
- [ ] Kubernetes deployment
- [ ] Auto-scaling workers
- [ ] Monitoring (Prometheus + Grafana)
- [ ] CI/CD pipeline
