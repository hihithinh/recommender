# Hướng dẫn Triển khai Hệ thống Phân tán

## Kiến trúc Triển khai

### Mô hình Phân tán Thực tế

```
┌─────────────────────────────────────────────────────────────┐
│                    Shared Storage (NFS/HDFS)                │
│              /mnt/shared/{data,raw_data,logs}               │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
┌───────▼────────┐   ┌────────▼────────┐   ┌───────▼────────┐
│  Master Node   │   │  Worker Node 1  │   │  Worker Node 2 │
│ 192.168.1.100  │   │ 192.168.1.101   │   │ 192.168.1.102  │
│                │   │                 │   │                │
│ - Spark Master │   │ - Spark Worker  │   │ - Spark Worker │
│ - Jupyter      │   │ - 4 cores       │   │ - 4 cores      │
│                │   │ - 8GB RAM       │   │ - 8GB RAM      │
└────────────────┘   └─────────────────┘   └────────────────┘
```

## Bước 1: Chuẩn bị Môi trường

### Trên tất cả các máy:

```bash
# 1. Cài đặt Docker và Docker Compose
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# 2. Cài đặt Docker Compose Plugin (v2)
sudo apt-get update
sudo apt-get install docker-compose-plugin

# 3. Tạo thư mục shared
sudo mkdir -p /mnt/shared/{data,raw_data,logs}
sudo chown -R $USER:$USER /mnt/shared
```

## Bước 2: Thiết lập Shared Storage

### Option 1: NFS (Network File System)

#### Trên Master Node (NFS Server):
```bash
# Cài đặt NFS server
sudo apt-get update
sudo apt-get install -y nfs-kernel-server

# Cấu hình exports
sudo nano /etc/exports
# Thêm dòng:
# /mnt/shared 192.168.1.0/24(rw,sync,no_subtree_check,no_root_squash)

# Restart NFS
sudo exportfs -a
sudo systemctl restart nfs-kernel-server

# Copy dữ liệu BookCrossing
cp -r raw_data/* /mnt/shared/raw_data/
```

#### Trên Worker Nodes (NFS Clients):
```bash
# Cài đặt NFS client
sudo apt-get install -y nfs-common

# Mount NFS share
sudo mount -t nfs 192.168.1.100:/mnt/shared /mnt/shared

# Auto-mount khi khởi động (thêm vào /etc/fstab)
echo "192.168.1.100:/mnt/shared /mnt/shared nfs defaults 0 0" | sudo tee -a /etc/fstab
```

### Option 2: HDFS (Hadoop Distributed File System)

```bash
# Cài đặt Hadoop HDFS cluster
# (Tham khảo: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html)

# Upload dữ liệu lên HDFS
hdfs dfs -mkdir -p /shared/raw_data
hdfs dfs -put raw_data/* /shared/raw_data/
```

## Bước 3: Clone Project

Trên tất cả các máy:
```bash
cd ~
git clone <your-repo-url> recommender
cd recommender
```

## Bước 4: Cấu hình Network

### Đảm bảo các máy có thể kết nối với nhau:

```bash
# Test connectivity
ping 192.168.1.100  # Master
ping 192.168.1.101  # Worker 1
ping 192.168.1.102  # Worker 2

# Mở firewall ports
sudo ufw allow 7077/tcp   # Spark Master
sudo ufw allow 8080/tcp   # Spark Master UI
sudo ufw allow 8081/tcp   # Worker UI
sudo ufw allow 6066/tcp   # Spark REST
sudo ufw allow 7000:7100/tcp  # Worker communication
```

### Cập nhật /etc/hosts trên tất cả máy:

```bash
sudo nano /etc/hosts

# Thêm:
192.168.1.100 spark-master
192.168.1.101 spark-worker-1
192.168.1.102 spark-worker-2
```

## Bước 5: Triển khai Services

### Trên Master Node (192.168.1.100):

```bash
cd ~/recommender

# Tạo file .env
cp .env.example .env
nano .env
# Cập nhật:
# SPARK_MASTER_HOST=192.168.1.100

# Build images
docker compose -f docker-compose.distributed.yml build spark-master jupyter

# Start services
export SPARK_MASTER_HOST=192.168.1.100
docker compose -f docker-compose.distributed.yml up -d spark-master jupyter

# Kiểm tra logs
docker compose -f docker-compose.distributed.yml logs -f spark-master
```

### Trên Worker Node 1 (192.168.1.101):

```bash
cd ~/recommender

# Build image
docker compose -f docker-compose.distributed.yml build spark-worker

# Start worker
export SPARK_MASTER_HOST=192.168.1.100
export DOCKER_HOST_IP=192.168.1.101
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=8g
export HOSTNAME=worker-1

docker compose -f docker-compose.distributed.yml up -d spark-worker

# Kiểm tra logs
docker compose -f docker-compose.distributed.yml logs -f spark-worker
```

### Trên Worker Node 2 (192.168.1.102):

```bash
cd ~/recommender

# Start worker
export SPARK_MASTER_HOST=192.168.1.100
export DOCKER_HOST_IP=192.168.1.102
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=8g
export HOSTNAME=worker-2

docker compose -f docker-compose.distributed.yml up -d spark-worker

# Kiểm tra logs
docker compose -f docker-compose.distributed.yml logs -f spark-worker
```

## Bước 6: Kiểm tra Cluster

### Truy cập Spark Master UI:
```
http://192.168.1.100:8080
```

Bạn sẽ thấy:
- Master status: ALIVE
- Workers: 2 workers connected
- Total cores: 8 (4 + 4)
- Total memory: 16 GB (8 + 8)

### Truy cập Jupyter:
```
http://192.168.1.100:8888
```

## Bước 7: Chạy Pipeline

### 1. Preprocessing Data

```bash
# Từ Master node hoặc Jupyter
docker exec spark-master spark-submit \
  --master spark://192.168.1.100:7077 \
  --deploy-mode client \
  --executor-memory 4g \
  --executor-cores 2 \
  --total-executor-cores 8 \
  /opt/scripts/run_preprocessing.py
```

### 2. Train ALS Model

```bash
docker exec spark-master spark-submit \
  --master spark://192.168.1.100:7077 \
  --deploy-mode client \
  --executor-memory 4g \
  --executor-cores 2 \
  --total-executor-cores 8 \
  /opt/spark-apps/training/train_als.py
```

### 3. Monitor Jobs

- **Spark Master UI**: http://192.168.1.100:8080
- **Running Application UI**: http://192.168.1.100:4040
- **Worker 1 UI**: http://192.168.1.101:8081
- **Worker 2 UI**: http://192.168.1.102:8081

## Troubleshooting

### Worker không kết nối được Master

```bash
# Kiểm tra network
telnet 192.168.1.100 7077

# Kiểm tra logs
docker logs spark-worker

# Restart worker
docker compose -f docker-compose.distributed.yml restart spark-worker
```

### Out of Memory

```bash
# Tăng worker memory
export SPARK_WORKER_MEMORY=16g
docker compose -f docker-compose.distributed.yml up -d spark-worker

# Hoặc giảm executor memory trong spark-submit
--executor-memory 2g
```

### Slow Performance

```bash
# Tăng số executors
--num-executors 4

# Tăng parallelism
--conf spark.default.parallelism=200
--conf spark.sql.shuffle.partitions=200

# Enable dynamic allocation
--conf spark.dynamicAllocation.enabled=true
```

## Scaling

### Thêm Worker mới:

```bash
# Trên máy mới (192.168.1.103)
export SPARK_MASTER_HOST=192.168.1.100
export DOCKER_HOST_IP=192.168.1.103
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=8g
export HOSTNAME=worker-3

docker compose -f docker-compose.distributed.yml up -d spark-worker
```

### Xóa Worker:

```bash
docker compose -f docker-compose.distributed.yml down spark-worker
```

## Backup và Recovery

### Backup Models và Data:

```bash
# Từ Master node
tar -czf backup_$(date +%Y%m%d).tar.gz /mnt/shared/data/models /mnt/shared/data/embeddings

# Copy to safe location
scp backup_*.tar.gz user@backup-server:/backups/
```

### Restore:

```bash
tar -xzf backup_20240101.tar.gz -C /mnt/shared/
```

## Production Checklist

- [ ] Shared storage configured và tested
- [ ] All nodes có thể ping nhau
- [ ] Firewall ports opened
- [ ] Docker và Docker Compose installed
- [ ] Master node running và accessible
- [ ] All workers connected to master
- [ ] Data copied to shared storage
- [ ] Test job chạy thành công
- [ ] Monitoring setup (logs, metrics)
- [ ] Backup strategy implemented
