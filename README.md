# Distributed Book Recommender System

Hệ thống gợi ý sách phân tán sử dụng Spark ALS và LightGBM với dữ liệu BookCrossing.

## Kiến trúc hệ thống

### Các thành phần chính:
- **Spark Master**: Điều phối các tác vụ phân tán
- **Spark Workers**: Thực thi các tác vụ (có thể mở rộng trên nhiều máy)
- **Jupyter Notebook**: Phát triển và phân tích tương tác
- **Shared Storage**: Lưu trữ dữ liệu và mô hình dạng Parquet

### Mô hình:
1. **Spark ALS**: Collaborative Filtering để tạo user/item embeddings
2. **LightGBM**: Gradient Boosting trên embeddings và features

## Cài đặt và Triển khai

### Yêu cầu hệ thống
- Docker Desktop (Mac/Windows) hoặc Docker Engine (Linux)
- Docker Compose v2.0+
- Tối thiểu 8GB RAM, khuyến nghị 16GB+
- Tối thiểu 20GB dung lượng ổ cứng

### 1. Triển khai Local (Single Machine)

**Trên Linux / macOS / Windows với WSL:**
```bash
# Build và khởi động tất cả services
docker compose up -d

# Kiểm tra logs
docker compose logs -f

# Dừng services
docker compose down
```

**Trên Windows (PowerShell - không dùng WSL):**
```powershell
# Build và khởi động tất cả services
docker compose up -d

# Kiểm tra logs
docker compose logs -f

# Dừng services
docker compose down
```

**Lưu ý cho Windows:**
- Đảm bảo Docker Desktop đã bật WSL 2 backend (Settings > General > Use WSL 2)
- Nếu gặp lỗi line endings, chạy: `git config --global core.autocrlf false`

**Truy cập các services:**
- Spark Master UI: http://localhost:8080
- Spark Worker 1 UI: http://localhost:8081
- Spark Worker 2 UI: http://localhost:8082
- Spark Worker 3 UI: http://localhost:8083
- Jupyter Lab: http://localhost:8888

### 2. Triển khai Phân tán (Multi-Host)

#### Bước 1: Chuẩn bị Shared Storage

**Trên Linux / Windows với WSL:**
```bash
# Tạo thư mục shared
sudo mkdir -p /mnt/shared/{data,raw_data,logs}

# Mount NFS hoặc distributed filesystem
# Ví dụ với NFS:
sudo mount -t nfs <nfs-server-ip>:/shared /mnt/shared
```

**Trên macOS:**
```bash
# Tạo thư mục shared trong home directory (không cần sudo)
mkdir -p ~/shared/{data,raw_data,logs}

# Mount NFS hoặc distributed filesystem
# Ví dụ với NFS:
sudo mount -t nfs <nfs-server-ip>:/shared ~/shared
```

**Trên Windows (không dùng WSL):**
```powershell
# Mở PowerShell với quyền Administrator
# Tạo thư mục shared
New-Item -Path "C:\shared\data" -ItemType Directory -Force
New-Item -Path "C:\shared\raw_data" -ItemType Directory -Force
New-Item -Path "C:\shared\logs" -ItemType Directory -Force

# Mount network drive (ví dụ với SMB/CIFS)
net use Z: \\<server-ip>\shared
```

#### Bước 2: Copy dữ liệu

**Trên Linux / Windows với WSL:**
```bash
# Copy dữ liệu BookCrossing vào shared storage
cp -r raw_data/* /mnt/shared/raw_data/
```

**Trên macOS:**
```bash
# Copy dữ liệu BookCrossing vào shared storage
cp -r raw_data/* ~/shared/raw_data/
```

**Trên Windows (không dùng WSL):**
```powershell
# Copy dữ liệu BookCrossing vào shared storage
Copy-Item -Path "raw_data\*" -Destination "C:\shared\raw_data\" -Recurse
```

#### Bước 3: Triển khai Master Node
Trên máy Master (ví dụ: 192.168.1.100):

**Trên Linux / macOS / Windows với WSL:**
```bash
# Set environment variables
export SPARK_MASTER_HOST=192.168.1.100

# Start master và jupyter
docker compose -f docker-compose.distributed.yml up -d spark-master jupyter
```

**Trên Windows (PowerShell - không dùng WSL):**
```powershell
# Set environment variables
$env:SPARK_MASTER_HOST="192.168.1.100"

# Start master và jupyter
docker compose -f docker-compose.distributed.yml up -d spark-master jupyter
```

#### Bước 4: Triển khai Worker Nodes
Trên mỗi máy Worker (ví dụ: 192.168.1.101, 192.168.1.102):

**Trên Linux / macOS / Windows với WSL:**
```bash
# Set environment variables
export SPARK_MASTER_HOST=192.168.1.100
export DOCKER_HOST_IP=<worker-machine-ip>
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=8g

# Start worker
docker compose -f docker-compose.distributed.yml up -d spark-worker
```

**Trên Windows (PowerShell - không dùng WSL):**
```powershell
# Set environment variables
$env:SPARK_MASTER_HOST="192.168.1.100"
$env:DOCKER_HOST_IP="<worker-machine-ip>"
$env:SPARK_WORKER_CORES="4"
$env:SPARK_WORKER_MEMORY="8g"

# Start worker
docker compose -f docker-compose.distributed.yml up -d spark-worker
```

#### Bước 5: Kiểm tra Cluster
Truy cập Spark Master UI tại: http://192.168.1.100:8080
Bạn sẽ thấy tất cả workers đã kết nối.

## Pipeline Xử lý Dữ liệu

### 1. Preprocessing
```bash
# Chạy trong Jupyter hoặc submit job
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/scripts/run_preprocessing.py
```

**Output:**
- `/opt/spark-data/processed/ratings.parquet`
- `/opt/spark-data/processed/users.parquet`
- `/opt/spark-data/processed/books.parquet`

### 2. Training ALS Model
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark-apps/training/train_als.py
```

**Output:**
- `/opt/spark-data/models/als_model/`
- `/opt/spark-data/embeddings/als_user_embeddings.parquet`
- `/opt/spark-data/embeddings/als_item_embeddings.parquet`

### 3. Training LightGBM Model
```bash
# Cài đặt SynapseML trước
pip install synapseml

# Train model
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages com.microsoft.azure:synapseml_2.12:0.11.3 \
  /opt/spark-apps/training/train_lightgbm.py
```

## Cấu trúc Dữ liệu

### BookCrossing Dataset
- **BX-Book-Ratings.csv**: User ratings (user_id, isbn, rating)
- **BX-Users.csv**: User information (user_id, location, age)
- **BX_Books.csv**: Book information (isbn, title, author, year, publisher)

### Processed Data (Parquet)
- **ratings.parquet**: Cleaned ratings với user_index và item_index
- **users.parquet**: User features
- **books.parquet**: Book features
- **embeddings/**: ALS user và item embeddings

## Development

### Chạy Jupyter Notebook
```bash
# Jupyter đã tự động start, truy cập:
http://localhost:8888

# Hoặc xem token:
docker logs jupyter-notebook
```

### Chạy Tests
```bash
docker exec spark-master python -m pytest /opt/spark-apps/tests/
```

### Monitoring
- **Spark Master UI**: Xem cluster status, running jobs
- **Spark Worker UI**: Xem worker resources, executors
- **Spark Application UI**: Xem job details, stages, tasks (port 4040)

## Cấu hình

### Spark Configuration
Chỉnh sửa `src/config/spark_config.py`:
- Executor memory
- Executor cores
- Driver memory
- Shuffle partitions

### Model Hyperparameters
Chỉnh sửa `src/config/model_config.py`:
- ALS: rank, iterations, regularization
- LightGBM: num_leaves, learning_rate, iterations

## Cấu hình Docker Volumes cho các Platform

### macOS
Docker Desktop trên macOS sử dụng VM, nên volumes được mount qua file sharing:
- Mặc định, Docker có thể truy cập: `/Users`, `/Volumes`, `/private`, `/tmp`
- Nếu cần mount thư mục khác, vào Docker Desktop > Settings > Resources > File Sharing
- Khuyến nghị sử dụng thư mục trong `~/` để tránh vấn đề permission

### Windows với WSL 2
Docker Desktop sử dụng WSL 2 backend:
- Volumes trong WSL: `/mnt/wsl/...` hoặc `\\wsl$\Ubuntu\home\...`
- Volumes trong Windows: `C:\Users\...` được mount tự động
- Khuyến nghị: Đặt project trong WSL filesystem để tốc độ tốt hơn

### Windows không dùng WSL (Hyper-V)
- Volumes phải ở trong `C:\Users\` hoặc được share trong Docker Desktop Settings
- Sử dụng đường dẫn Windows: `C:\shared\data` thay vì `/mnt/shared/data`
- Chú ý: Performance có thể chậm hơn so với WSL 2

### Linux
- Volumes được mount trực tiếp từ filesystem
- Không có overhead như macOS hay Windows
- Cần chú ý permission: user trong container phải có quyền truy cập

## Troubleshooting

### Worker không kết nối được Master
```bash
# Kiểm tra network
docker network inspect recommender_spark-network

# Kiểm tra logs
docker logs spark-worker-1
```

### Out of Memory
```bash
# Tăng executor memory trong docker-compose.yml
SPARK_WORKER_MEMORY=8g

# Hoặc giảm số partitions
spark.sql.shuffle.partitions=100
```

### Slow Performance
- Tăng số workers
- Tăng executor cores
- Optimize data partitioning
- Enable adaptive query execution

### Lỗi Permission Denied (macOS/Linux)
```bash
# Kiểm tra ownership của thư mục
ls -la ~/shared

# Thay đổi ownership nếu cần
sudo chown -R $(whoami):$(whoami) ~/shared
```

### Lỗi Line Endings trên Windows
```bash
# Nếu gặp lỗi "bad interpreter" hoặc script không chạy
git config --global core.autocrlf false
git rm --cached -r .
git reset --hard
```

### Docker Desktop không khởi động (Windows)
- Kiểm tra Hyper-V hoặc WSL 2 đã được bật
- Chạy: `wsl --set-default-version 2`
- Restart Docker Desktop

### Volume mount không hoạt động
**macOS:**
- Kiểm tra File Sharing trong Docker Desktop Settings
- Thử restart Docker Desktop

**Windows:**
- Kiểm tra drive đã được share trong Docker Desktop Settings
- Đảm bảo đường dẫn sử dụng forward slash `/` trong docker-compose.yml

## Tài liệu tham khảo

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Spark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [LightGBM Documentation](https://lightgbm.readthedocs.io/)
- [SynapseML](https://microsoft.github.io/SynapseML/)

## License

MIT License
