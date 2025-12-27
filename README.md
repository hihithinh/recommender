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

### 1. Triển khai Local (Single Machine)

```bash
# Build và khởi động tất cả services
docker-compose up -d

# Kiểm tra logs
docker-compose logs -f

# Dừng services
docker-compose down
```

**Truy cập các services:**
- Spark Master UI: http://localhost:8080
- Spark Worker 1 UI: http://localhost:8081
- Spark Worker 2 UI: http://localhost:8082
- Spark Worker 3 UI: http://localhost:8083
- Jupyter Lab: http://localhost:8888

### 2. Triển khai Phân tán (Multi-Host)

#### Bước 1: Chuẩn bị Shared Storage
Trên tất cả các máy, mount shared storage:
```bash
# Tạo thư mục shared
sudo mkdir -p /mnt/shared/{data,raw_data,logs}

# Mount NFS hoặc distributed filesystem
# Ví dụ với NFS:
sudo mount -t nfs <nfs-server-ip>:/shared /mnt/shared
```

#### Bước 2: Copy dữ liệu
```bash
# Copy dữ liệu BookCrossing vào shared storage
cp -r raw_data/* /mnt/shared/raw_data/
```

#### Bước 3: Triển khai Master Node
Trên máy Master (ví dụ: 192.168.1.100):
```bash
# Set environment variables
export SPARK_MASTER_HOST=192.168.1.100

# Start master và jupyter
docker-compose -f docker-compose.distributed.yml up -d spark-master jupyter
```

#### Bước 4: Triển khai Worker Nodes
Trên mỗi máy Worker (ví dụ: 192.168.1.101, 192.168.1.102):
```bash
# Set environment variables
export SPARK_MASTER_HOST=192.168.1.100
export DOCKER_HOST_IP=<worker-machine-ip>
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=8g

# Start worker
docker-compose -f docker-compose.distributed.yml up -d spark-worker
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

## Tài liệu tham khảo

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Spark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [LightGBM Documentation](https://lightgbm.readthedocs.io/)
- [SynapseML](https://microsoft.github.io/SynapseML/)

## License

MIT License
