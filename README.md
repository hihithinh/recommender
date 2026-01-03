# Distributed Book Recommender System

Hệ thống gợi ý sách phân tán sử dụng Spark ALS và LightGBM với dữ liệu BookCrossing.

**Pipeline**: Kafka → Parquet (Data Lake) → Spark Cluster → ML Models → API

## Kiến trúc phân tán linh hoạt

Mỗi máy có thể chạy:
- **Spark Master** + Kafka + Jupyter + API (máy có data)
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
docker compose -f docker-compose.cluster.yml up -d spark-master kafka zookeeper kafka-ui jupyter-notebook api-server
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
# Start tất cả (master + 1 worker)
docker compose -f docker-compose.cluster.yml up -d spark-master spark-worker-1 kafka zookeeper jupyter-notebook

# Hoặc thêm nhiều workers
docker compose -f docker-compose.cluster.yml up -d spark-worker-2 spark-worker-3
```

### 5. Verify Cluster

Truy cập: `http://<MASTER_IP>:8080` để xem workers đã kết nối

**Services:**
- Spark Master UI: `http://[MASTER_IP]:8080`
- Spark Worker UIs: `http://[WORKER_IP]:8081`, `8082`, `8083`, ...
- Jupyter Lab: `http://[MASTER_IP]:8888`
- Kafka UI: `http://[MASTER_IP]:8090`
- API Server: `http://[MASTER_IP]:5001`

## Data Pipeline

### 1. Kafka Topics

```bash
docker exec -it kafka bash
kafka-topics --create --topic book-ratings --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics --list --bootstrap-server localhost:9092
```

### 2. Preprocessing

```bash
docker exec spark-master spark-submit \
  --master spark://<MASTER_IP>:7077 \
  /opt/scripts/run_preprocessing.py
```

### 3. Train Models

```bash
# ALS Model
docker exec spark-master spark-submit \
  --master spark://<MASTER_IP>:7077 \
  /opt/spark-apps/training/train_als.py

# LightGBM Model
docker exec spark-master spark-submit \
  --master spark://<MASTER_IP>:7077 \
  --packages com.microsoft.azure:synapseml_2.12:0.11.3 \
  /opt/spark-apps/training/train_lightgbm.py
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
docker compose -f docker-compose.cluster.yml up -d spark-master kafka zookeeper jupyter-notebook
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
docker compose -f docker-compose.cluster.yml up -d spark-master spark-worker-1 kafka zookeeper jupyter-notebook
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
