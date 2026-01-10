# Distributed Book Recommender System

Hệ thống gợi ý sách phân tán sử dụng Spark ALS và LightGBM với dữ liệu BookCrossing.

**Pipeline**: HDFS (Distributed Storage) → Spark Cluster → ML Models → API

## Kiến trúc phân tán linh hoạt

Mỗi máy có thể chạy:
- **Spark Master** + HDFS + Jupyter + API (máy có data)
- **Spark Worker** (1 hoặc nhiều workers trên cùng 1 máy)
- **Cả Master và Worker** (máy Mac có thể vừa là master vừa là worker)

**Connection**: Tailscale VPN (encrypted, no port forwarding)

## Yêu cầu

- Docker Desktop + Tailscale VPN
- RAM: 8GB minimum, 16GB+ recommended
- Storage: 50GB+ cho máy có data

## Setup

### 1. Cài Tailscale

```bash
# macOS
brew install tailscale
sudo tailscale up

# Windows: https://tailscale.com/download/windows
# Linux: curl -fsSL https://tailscale.com/install.sh | sh
```

### 2. Lấy Tailscale IP

```bash
tailscale ip -4
# Output: 100.x.x.x
```

### 3. Cấu hình .env

```bash
# Copy template
cp .env.tailscale .env

# Sửa file .env
nano .env  # hoặc notepad .env trên Windows
```

**Điền các giá trị:**
```bash
THIS_MACHINE_IP=100.x.x.x        # IP của máy này (tailscale ip -4)
MASTER_TAILSCALE_IP=100.y.y.y    # IP của máy chạy Master (QUAN TRỌNG!)
SPARK_WORKER_CORES=4              # Số cores cho worker
SPARK_WORKER_MEMORY=8g            # RAM cho worker
```

**⚠️ LƯU Ý QUAN TRỌNG**: `MASTER_TAILSCALE_IP` phải là Tailscale IP thật của máy master, không phải `localhost` hay `127.0.0.1`. Dùng lệnh `tailscale ip -4` trên máy master để lấy IP.

### 4. Chạy Services

**Trên máy chạy Master (có data):**
```bash
# Start all master services including 3 datanodes for fault tolerance
docker compose -f docker-compose.cluster.yml up -d spark-master namenode datanode-1 datanode-2 datanode-3 jupyter-notebook api-server
```

**Trên máy chạy Worker (bất kỳ máy nào):**
```bash
# Worker 1
docker compose -f docker-compose.cluster.yml up -d spark-worker-1

# Worker 2 trên cùng máy (optional)
docker compose -f docker-compose.cluster.yml up -d spark-worker-2

# Worker 3 trên cùng máy (optional)
docker compose -f docker-compose.cluster.yml up -d spark-worker-3

# Hoặc start nhiều workers cùng lúc
docker compose -f docker-compose.cluster.yml up -d spark-worker-1 spark-worker-2 spark-worker-3
```

**Máy Mac vừa Master vừa Worker:**
```bash
# Start tất cả (master + 1 worker + HDFS with 3 datanodes)
docker compose -f docker-compose.cluster.yml up -d spark-master spark-worker-1 namenode datanode-1 datanode-2 datanode-3 jupyter-notebook

# Hoặc thêm nhiều workers
docker compose -f docker-compose.cluster.yml up -d spark-worker-2 spark-worker-3
```

### 5. Verify Cluster

Truy cập: `http://<MASTER_IP>:8080` để xem workers đã kết nối

**Services:**
- Spark Master UI: `http://[MASTER_IP]:8080`
- Spark Worker UIs: `http://[WORKER_IP]:8081`, `8082`, `8083`, ...
- HDFS NameNode UI: `http://[MASTER_IP]:9870`
- Jupyter Lab: `http://[MASTER_IP]:8888`
- API Server: `http://[MASTER_IP]:5001`

## Data Pipeline

**Pipeline phải chạy tuần tự theo thứ tự sau:**

### 1. Upload Data to HDFS

**Lần đầu tiên**, upload CSV files lên HDFS:

```bash
# Start HDFS services with 3 datanodes for replication
docker compose -f docker-compose.cluster.yml up -d namenode datanode-1 datanode-2 datanode-3

# Wait for HDFS to exit safe mode (30-60 seconds)
docker exec namenode hdfs dfsadmin -safemode wait

# Verify all datanodes are live
docker exec namenode hdfs dfsadmin -report
# Should show: Live datanodes (3)

# Upload CSV files to HDFS
chmod +x scripts/upload_to_hdfs.sh
./scripts/upload_to_hdfs.sh

# Set replication factor to 2 (chỉ cần chạy 1 lần duy nhất)
docker exec namenode hdfs dfs -setrep -R 2 /data/raw/

# Verify replication
docker exec namenode hdfs fsck /data/raw/ -files -blocks -locations
# Should show: Average block replication: 2.0
```

### 2. Preprocessing (Tạo data cho ALS)

**Mục đích**: Clean và index data từ CSV, tạo `ratings.parquet` cho ALS training.

```bash
docker exec spark-master python3 /opt/scripts/run_preprocessing.py
```

**Output**: `/data/processed/ratings.parquet`, `/data/processed/users.parquet`, `/data/processed/books.parquet`

### 3. Train ALS Model

**Mục đích**: Train ALS model và tạo train/val/test splits.

**Lưu ý**: Code đã tự động config driver ports (35000, 35001) qua `SparkConfig.create_spark_session()`, không cần thêm `--conf`.

```bash
docker exec spark-master python3 /opt/spark-apps/training/train_als.py
```

**Output**: 
- ALS model: `/models/als_model`
- Splits: `/data/processed/train_ratings.parquet`, `validation_ratings.parquet`, `test_ratings.parquet`

### 4. Extract ALS Embeddings (Tạo features cho LightGBM)

**Mục đích**: Load ALS model đã train và extract user/item embeddings.

```bash
docker exec spark-master python3 /opt/scripts/extract_als_embeddings.py
```

**Output**: 
- Embeddings: `/data/embeddings/als_user_embeddings.parquet`, `als_item_embeddings.parquet`

### 5. Train LightGBM Model (Sử dụng ALS embeddings)

**Mục đích**: Train LightGBM model sử dụng ALS embeddings làm features.

```bash
docker exec spark-master python3 /opt/spark-apps/training/train_lightgbm.py
```

**Output**: LightGBM model: `/models/lightgbm_model`

**Hoặc dùng spark-submit** (nếu cần custom config):
```bash
docker exec spark-master spark-submit \
  --master spark://<MASTER_IP>:7077 \
  /opt/spark-apps/training/train_als.py
```

## Monitoring

```bash
# Check status
docker compose -f docker-compose.cluster.yml ps

# View logs
docker compose -f docker-compose.cluster.yml logs -f
docker logs spark-worker-1

# Restart services
docker compose -f docker-compose.cluster.yml restart
```

## Troubleshooting

**Worker không kết nối:**
```bash
ping <MASTER_IP>
telnet <MASTER_IP> 7077
docker logs spark-worker-1
```

**Tăng memory:**
```bash
# Edit .env: SPARK_WORKER_MEMORY=16g
docker compose -f docker-compose.cluster.yml restart spark-worker
```

## Stop Services

```bash
# Stop tất cả
docker compose -f docker-compose.cluster.yml down

# Stop specific service
docker compose -f docker-compose.cluster.yml stop spark-worker
```

## Ví dụ Deployment

### Scenario 1: Mac Master + Windows Worker
**Mac (có data):**
```bash
# .env: THIS_MACHINE_IP=100.1.1.1, MASTER_TAILSCALE_IP=100.1.1.1
docker compose -f docker-compose.cluster.yml up -d spark-master namenode datanode-1 datanode-2 datanode-3 jupyter-notebook
```

**Windows:**
```bash
# .env: THIS_MACHINE_IP=100.1.1.2, MASTER_TAILSCALE_IP=100.1.1.1
docker compose -f docker-compose.cluster.yml up -d spark-worker-1
```

### Scenario 2: Mac vừa Master vừa Worker + Windows 2 Workers
**Mac:**
```bash
# .env: THIS_MACHINE_IP=100.1.1.1, MASTER_TAILSCALE_IP=100.1.1.1
docker compose -f docker-compose.cluster.yml up -d spark-master spark-worker-1 namenode datanode-1 datanode-2 datanode-3 jupyter-notebook
```

**Windows:**
```bash
# .env: THIS_MACHINE_IP=100.1.1.2, MASTER_TAILSCALE_IP=100.1.1.1
docker compose -f docker-compose.cluster.yml up -d spark-worker-1 spark-worker-2
```

## Tài liệu

- **[ARCHITECTURE.md](ARCHITECTURE.md)**: Chi tiết kiến trúc, training pipeline, serving pipeline
- **Source code**: `src/` directory
  - Training: `src/training/`
  - Models: `src/models/`
  - API: `src/api/`
  - Config: `src/config/`

## License

MIT
