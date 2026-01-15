# Book Recommender System - Architecture

## Tổng quan

Hệ thống gợi ý sách phân tán sử dụng Apache Spark cluster với HDFS storage và ML models (ALS + LightGBM).

```
┌─────────────────────────────────────────────────────────────────┐
│                     DISTRIBUTED CLUSTER                          │
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Master     │  │   Worker     │  │   Worker     │          │
│  │   (Docker)   │  │ (Docker/WSL) │  │ (Docker/WSL) │          │
│  │              │  │              │  │              │          │
│  │ Spark Master │  │ Spark Worker │  │ Spark Worker │          │
│  │ HDFS NN      │  │              │  │              │          │
│  │ 3x DataNodes │  │              │  │              │          │
│  │ Jupyter      │  │              │  │              │          │
│  │ API Server   │  │              │  │              │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│         │                  │                  │                  │
│         └──────────────────┴──────────────────┘                  │
│                  Tailscale VPN (Encrypted)                       │
└─────────────────────────────────────────────────────────────────┘
```

### Đặc điểm

- **Deployment linh hoạt**: Master chạy Docker, Worker chạy Docker hoặc WSL
- **Scalable**: Horizontal scaling bằng cách thêm worker nodes
- **Fault-tolerant**: HDFS replication factor = 2, 3 DataNodes
- **Secure**: Tailscale VPN end-to-end encryption

## Data Flow

```
CSV Files → HDFS → Preprocessing → Parquet Files
                                        ↓
                    ┌───────────────────┴────────────────┐
                    ↓                                     ↓
            Training Pipeline                    Serving Pipeline
                    ↓                                     ↓
        ALS Model → Embeddings → LightGBM         API Server (Flask)
                    ↓                                     ↓
            HDFS Model Storage                    REST Endpoints
```

---

## TRAINING PIPELINE

### 1. Data Ingestion

**Source**: BookCrossing Dataset (CSV files)
- `BX-Book-Ratings.csv` - User ratings (1.1M records)
- `BX-Users.csv` - User demographics
- `BX_Books.csv` - Book metadata

**Storage**: Upload to HDFS với replication factor = 2

### 2. Preprocessing

**Module**: `src/data_processing/`

**Chức năng**:
- `data_loader.py` - Load CSV từ HDFS
- `data_cleaner.py` - Làm sạch missing values, outliers
- `feature_engineering.py` - Tạo user/item indices, extract features

**Input**: HDFS `/data/raw/*.csv`

**Output**: HDFS `/data/processed/{ratings,users,books}.parquet`

**HDFS Paths** (config trong `src/config/spark_config.py`):
```python
raw_data = "hdfs://{master_ip}:9000/data/raw"
processed_data = "hdfs://{master_ip}:9000/data/processed"
embeddings = "hdfs://{master_ip}:9000/data/embeddings"
models = "hdfs://{master_ip}:9000/data/models"
```

### 3. ALS Model Training

**File**: `src/training/train_als.py`

**Algorithm**: Spark MLlib ALS (Alternating Least Squares)

**Input**: `/data/processed/ratings.parquet` (user_index, item_index, rating)

**Hyperparameters**:
Tham khảo từ repo [als_deep_dive.ipynb](https://github.com/recommenders-team/recommenders/blob/main/examples/02_model_collaborative_filtering/als_deep_dive.ipynb), một repo gồm các best practice cho recommenders
```python
RANK = 50              # Latent factors
MAX_ITER = 20          # Iterations
REG_PARAM = 0.1        # L2 regularization
IMPLICIT_PREFS = False # Explicit ratings
```

**Output**:
- Model: `/models/als_model/`
- Train/val/test splits: `/data/processed/{train,validation,test}_ratings.parquet`

**Metrics**: RMSE, MAE

### 4. Extract ALS Embeddings

**File**: `scripts/extract_als_embeddings.py`

**Input**: `/models/als_model/`

**Output**: 
- `/data/embeddings/als_user_embeddings.parquet` (user_id, features[50])
- `/data/embeddings/als_item_embeddings.parquet` (item_id, features[50])

### 5. LightGBM Model Training

**File**: `src/training/train_lightgbm.py`

**Algorithm**: LightGBM via SynapseML

**Features**:
- ALS embeddings (50-dim user + 50-dim item)
- User metadata (age, location)
- Book metadata (author, year, publisher)

**Hyperparameters**:
Tham khảo từ repo [mmlspark_lightgbm_criteo.ipynb](https://github.com/recommenders-team/recommenders/blob/main/examples/02_model_content_based_filtering/mmlspark_lightgbm_criteo.ipynb), Content-Based Personalization with LightGBM on Spark
```python
NUM_LEAVES = 32
LEARNING_RATE = 0.1
NUM_ITERATIONS = 50
FEATURE_FRACTION = 0.8
```

**Output**: `/models/lightgbm_model/`

### 6. Evaluation

**Module**: `src/evaluation/metrics.py`

**Metrics**:
- RMSE, MAE - Rating prediction accuracy
- Precision@K, Recall@K - Top-K ranking quality
- NDCG - Ranking relevance
- Coverage - Catalog diversity

---

## SERVING PIPELINE

### 1. API Server

**Framework**: Flask + PySpark

**File**: `src/api/app.py`

**Port**: 5001

**Endpoints**:
- `GET /` - Web UI
- `POST /api/recommend` - Gợi ý cho user hiện có
- `POST /api/recommend/new-user` - Cold-start recommendations
- `GET /api/search` - Tìm kiếm sách
- `GET /api/popular` - Top sách phổ biến
- `GET /api/health` - Health check

### 2. Recommendation Service

**File**: `src/api/recommendation_service.py`

**Chức năng**:
- Load models từ HDFS (`/models/als_model`, `/models/lightgbm_model`)
- Generate recommendations (ALS hoặc ALS + LightGBM)
- Search và filter books
- Cold-start handling cho new users

**Recommendation Strategies**:
- **ALS only**: Nhanh, dựa trên collaborative filtering
- **ALS + LightGBM**: Chậm hơn, kết hợp metadata features

### 3. Model Components

**ALS Recommender** (`src/models/als_recommender.py`):
- Train/load ALS model
- Extract user/item embeddings
- Generate top-N recommendations

**LightGBM Recommender** (`src/models/lightgbm_recommender.py`):
- Train/load LightGBM model
- Combine ALS embeddings với metadata
- Predict ratings

---

## Configuration

### Spark Config (`src/config/spark_config.py`)

```python
def create_spark_session(
    app_name="BookCrossing-Recommender",
    master="spark://spark-master:7077",
    executor_memory="4g",
    executor_cores=2,
    driver_memory="2g"
)
```

**Key settings**:
- Serializer: Kryo
- Compression: Snappy (Parquet)
- Adaptive Query Execution: Enabled
- Driver ports: 35000, 35001 (auto-configured)

### Model Config (`src/config/model_config.py`)

- `ALSConfig` - ALS hyperparameters
- `LightGBMConfig` - LightGBM hyperparameters
- `DataConfig` - Train/val/test split ratios

---

## Data Storage

### HDFS Structure
```
/data/
  ├── raw/                          # CSV files (replication=2)
  ├── processed/                    # Parquet files
  │   ├── ratings.parquet
  │   ├── users.parquet
  │   ├── books.parquet
  │   └── {train,validation,test}_ratings.parquet
  ├── embeddings/                   # ALS embeddings
  │   ├── als_user_embeddings.parquet
  │   └── als_item_embeddings.parquet
  └── models/                       # Trained models
      ├── als_model/
      └── lightgbm_model/
```

### Docker Volumes
```yaml
Master:
  - ./data:/opt/spark-data          # Models, processed data
  - ./src:/opt/spark-apps           # Source code
  - ./scripts:/opt/scripts          # Scripts
  - ./hdfs:/hadoop/dfs              # HDFS storage

Workers:
  - ./src:/opt/spark-apps           # Source code (read-only)
  - worker-cache:/opt/spark-data    # Local cache
```

---

## Monitoring

**Spark UIs**:
- Master: `http://<MASTER_IP>:8080` - Cluster status
- Worker: `http://<WORKER_IP>:8081` - Worker resources
- Application: `http://<MASTER_IP>:4040` - Job details

**HDFS UI**: `http://<MASTER_IP>:9870` - DataNode status, storage

**Logs**:
```bash
docker logs spark-master
docker logs spark-worker-1
docker logs api-server
```

---

## Scaling

**Horizontal (thêm workers)**:
```bash
# Trên máy mới: cấu hình .env và start worker
docker compose -f docker-compose.cluster.yml up -d spark-worker-1
```

**Vertical (tăng resources)**:
```bash
# Sửa .env: SPARK_WORKER_MEMORY=16g, SPARK_WORKER_CORES=8
docker compose -f docker-compose.cluster.yml restart spark-worker-1
```
