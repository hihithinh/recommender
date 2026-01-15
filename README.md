# Distributed Book Recommender System

Hệ thống gợi ý sách phân tán sử dụng Apache Spark ALS và LightGBM trên dữ liệu BookCrossing.

## Kiến trúc

### Tổng quan
```
HDFS (Distributed Storage) → Spark Cluster → ML Models (ALS + LightGBM) → REST API
```

### Deployment Model
Hệ thống hỗ trợ triển khai linh hoạt trên nhiều máy:

- **Master Node** (Docker): Spark Master, HDFS NameNode, Jupyter, API Server
- **Worker Nodes**: 
  - Docker: Chạy Spark Worker trong container
  - WSL: Chạy Spark Worker trực tiếp trên WSL environment
- **Kết nối**: Tailscale VPN (mã hóa end-to-end)

## Công nghệ

- **Storage**: HDFS (3 DataNodes, replication factor = 2)
- **Processing**: Apache Spark 3.5.0 (cluster mode)
- **ML Models**: 
  - Spark MLlib ALS (Collaborative Filtering)
  - LightGBM via SynapseML (Gradient Boosting)
- **API**: Flask + PySpark
- **Containerization**: Docker Compose
- **Network**: Tailscale VPN

## Yêu cầu hệ thống

- Docker Desktop
- Tailscale VPN
- RAM: 8GB tối thiểu (khuyến nghị 16GB+)
- Storage: 50GB+ cho Master node

## Setup

### Bước 1: Cài đặt Tailscale

```bash
# macOS
brew install tailscale && sudo tailscale up

# Windows: tải từ https://tailscale.com/download/windows
# Linux: curl -fsSL https://tailscale.com/install.sh | sh
```

### Bước 2: Cấu hình môi trường

```bash
cp .env.tailscale .env
nano .env
```

Cấu hình file `.env`:
```bash
THIS_MACHINE_IP=100.x.x.x        # Lấy từ: tailscale ip -4
MASTER_TAILSCALE_IP=100.y.y.y    # IP của Master node
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=8g
```

### Bước 3: Khởi động services

**Master Node (Docker):**
```bash
docker compose -f docker-compose.cluster.yml up -d \
  spark-master namenode datanode-1 datanode-2 datanode-3 \
  jupyter-notebook api-server
```

**Worker Node (Docker):**
```bash
docker compose -f docker-compose.cluster.yml up -d spark-worker-1
```

**Worker Node (WSL):**
```bash
# Cài đặt Java 11 và Spark trên WSL
# Chạy worker script với MASTER_IP
./start-worker-wsl.sh $MASTER_TAILSCALE_IP
```

### Bước 4: Kiểm tra cluster

Truy cập Spark Master UI: `http://<MASTER_IP>:8080`

**Các services khác:**
- HDFS NameNode: `http://<MASTER_IP>:9870`
- Jupyter Lab: `http://<MASTER_IP>:8888`
- API Server: `http://<MASTER_IP>:5001`

## Xử lý dữ liệu với Spark

### 1. Upload dữ liệu lên HDFS

**Mục đích:** Tải raw CSV files lên HDFS với replication

**Input:** `data/raw/*.csv` (BX-Book-Ratings.csv, BX-Users.csv, BX_Books.csv)

**Output:** HDFS `/data/raw/` với replication factor = 2

```bash
docker compose -f docker-compose.cluster.yml up -d namenode datanode-1 datanode-2 datanode-3
docker exec namenode hdfs dfsadmin -safemode wait

chmod +x scripts/upload_to_hdfs.sh
./scripts/upload_to_hdfs.sh

docker exec namenode hdfs dfs -setrep -R 2 /data/raw/
```

### 2. Preprocessing

**Mục đích:** Làm sạch dữ liệu, tạo user/item indices, chuẩn bị cho training

**Input:** HDFS `/data/raw/*.csv`

**Output:** HDFS `/data/processed/{ratings,users,books}.parquet`

```bash
docker exec spark-master python3 /opt/scripts/run_preprocessing.py
```

### 3. Train ALS Model

**Mục đích:** Huấn luyện Collaborative Filtering model, tạo train/val/test splits

**Input:** `/data/processed/ratings.parquet`

**Output:** 
- Model: `/models/als_model`
- Splits: `/data/processed/{train,validation,test}_ratings.parquet`

```bash
docker exec spark-master python3 /opt/spark-apps/training/train_als.py
```

### 4. Extract ALS Embeddings

**Mục đích:** Trích xuất user và item embeddings từ ALS model làm features cho LightGBM

**Input:** `/models/als_model`

**Output:** `/data/embeddings/als_{user,item}_embeddings.parquet`

```bash
docker exec spark-master python3 /opt/scripts/extract_als_embeddings.py
```

### 5. Train LightGBM Model

**Mục đích:** Huấn luyện Gradient Boosting model kết hợp ALS embeddings và metadata features

**Input:** 
- `/data/embeddings/als_{user,item}_embeddings.parquet`
- `/data/processed/{users,books}.parquet`

**Output:** `/models/lightgbm_model`

```bash
docker exec spark-master python3 /opt/spark-apps/training/train_lightgbm.py
```

## API Endpoints

**Base URL:** `http://<MASTER_IP>:5001`

- `POST /api/recommend` - Gợi ý sách cho user hiện có
- `POST /api/recommend/new-user` - Gợi ý cho user mới (cold-start)
- `GET /api/search?q=<query>` - Tìm kiếm sách
- `GET /api/popular?top_n=20` - Sách phổ biến
- `GET /api/health` - Health check

## Monitoring

```bash
docker compose -f docker-compose.cluster.yml ps
docker logs -f spark-master
docker logs -f spark-worker-1
```

## Tài liệu kỹ thuật

- [ARCHITECTURE.md](./ARCHITECTURE.md) - Chi tiết kiến trúc và pipeline