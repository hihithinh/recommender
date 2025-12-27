# Kiến trúc Phân tán - Data Flow

## Vai trò của các thành phần

### 1. Spark Master
- **KHÔNG lưu trữ dữ liệu**
- Chỉ điều phối và quản lý cluster
- Phân bổ tasks cho workers
- Theo dõi trạng thái workers
- Quản lý resource allocation

### 2. Spark Workers
- **KHÔNG lấy dữ liệu từ Master**
- Đọc/ghi dữ liệu **trực tiếp từ Shared Storage**
- Thực thi tasks được phân bổ
- Xử lý dữ liệu phân tán
- Giao tiếp với nhau khi cần shuffle data

### 3. Shared Storage (NFS/HDFS)
- **Nguồn dữ liệu chung** cho tất cả nodes
- Tất cả workers đọc/ghi trực tiếp
- Master cũng đọc/ghi từ đây (nếu cần)

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Shared Storage (NFS/HDFS)                │
│              /mnt/shared/{data,raw_data,logs}               │
│                                                             │
│  - BX-Book-Ratings.csv                                      │
│  - ratings.parquet                                          │
│  - embeddings.parquet                                       │
│  - models/                                                  │
└─────────────────────────────────────────────────────────────┘
         ▲                    ▲                    ▲
         │                    │                    │
    READ/WRITE           READ/WRITE           READ/WRITE
         │                    │                    │
         │                    │                    │
┌────────┴────────┐   ┌───────┴────────┐   ┌──────┴─────────┐
│  Master Node    │   │  Worker Node 1 │   │  Worker Node 2 │
│ 100.101.102.103 │   │ 100.101.102.104│   │ 100.101.102.105│
│                 │   │                │   │                │
│ Spark Master    │   │ Spark Worker   │   │ Spark Worker   │
│ (Coordinator)   │   │ (Executor)     │   │ (Executor)     │
│                 │   │                │   │                │
│ - Schedule jobs │   │ - Read data    │   │ - Read data    │
│ - Assign tasks  │   │ - Process      │   │ - Process      │
│ - Monitor       │   │ - Write results│   │ - Write results│
└─────────────────┘   └────────────────┘   └────────────────┘
         │                    ▲                    ▲
         │                    │                    │
         └────────────────────┴────────────────────┘
              Task Assignment & Coordination
```

## Luồng xử lý dữ liệu chi tiết

### Ví dụ: Đọc file CSV và xử lý

```python
# Code chạy trên Driver (Master hoặc Jupyter)
df = spark.read.csv("/mnt/shared/raw_data/BX-Book-Ratings.csv")
result = df.filter(col("rating") > 5).groupBy("user_id").count()
result.write.parquet("/mnt/shared/data/processed/filtered.parquet")
```

**Điều gì xảy ra:**

```
Step 1: Driver (Master) tạo execution plan
┌─────────────┐
│   Master    │  "Tôi cần đọc file CSV, filter, group by"
│   Driver    │  → Tạo DAG (Directed Acyclic Graph)
└─────────────┘  → Chia thành các tasks

Step 2: Master phân bổ tasks cho Workers
┌─────────────┐
│   Master    │  "Worker 1: xử lý partition 0-99"
└──────┬──────┘  "Worker 2: xử lý partition 100-199"
       │
       ├──────────────┬──────────────┐
       ▼              ▼              ▼
   Worker 1       Worker 2       Worker 3

Step 3: Workers đọc dữ liệu TRỰC TIẾP từ Shared Storage
┌──────────────┐
│    Worker 1  │ ──READ──→ /mnt/shared/raw_data/file.csv (rows 0-999)
└──────────────┘

┌──────────────┐
│    Worker 2  │ ──READ──→ /mnt/shared/raw_data/file.csv (rows 1000-1999)
└──────────────┘

Step 4: Workers xử lý dữ liệu trong memory
┌──────────────┐
│    Worker 1  │  Filter → Group By → Aggregate
└──────────────┘  (Xử lý partition của nó)

┌──────────────┐
│    Worker 2  │  Filter → Group By → Aggregate
└──────────────┘  (Xử lý partition của nó)

Step 5: Shuffle data giữa workers (nếu cần)
┌──────────────┐         ┌──────────────┐
│    Worker 1  │ ←─────→ │    Worker 2  │
└──────────────┘  Data   └──────────────┘
                Exchange

Step 6: Workers ghi kết quả TRỰC TIẾP vào Shared Storage
┌──────────────┐
│    Worker 1  │ ──WRITE──→ /mnt/shared/data/processed/part-00000.parquet
└──────────────┘

┌──────────────┐
│    Worker 2  │ ──WRITE──→ /mnt/shared/data/processed/part-00001.parquet
└──────────────┘
```

## Tại sao cần Shared Storage?

### ❌ KHÔNG dùng Shared Storage:
```
Master có dữ liệu → Workers phải lấy từ Master
→ Master trở thành bottleneck
→ Không scale được
→ Master quá tải
```

### ✅ Dùng Shared Storage:
```
Shared Storage ← Workers đọc/ghi song song
→ Không có bottleneck
→ Scale tốt
→ Master chỉ điều phối
```

## Các loại Shared Storage

### 1. NFS (Network File System)
```
┌─────────────┐
│ Master Node │ ← NFS Server
│ /mnt/shared │
└──────┬──────┘
       │
       ├──────────────┬──────────────┐
       ▼              ▼              ▼
   Worker 1       Worker 2       Worker 3
   (NFS Client)   (NFS Client)   (NFS Client)
   /mnt/shared    /mnt/shared    /mnt/shared
```

**Ưu điểm:**
- Dễ setup
- Transparent (như local filesystem)

**Nhược điểm:**
- Single point of failure (NFS server)
- Performance giới hạn bởi network bandwidth

### 2. HDFS (Hadoop Distributed File System)
```
┌─────────────────────────────────────────────┐
│         HDFS Cluster (Distributed)          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ DataNode │  │ DataNode │  │ DataNode │  │
│  │ (Master) │  │(Worker 1)│  │(Worker 2)│  │
│  └──────────┘  └──────────┘  └──────────┘  │
└─────────────────────────────────────────────┘
```

**Ưu điểm:**
- Highly available (replication)
- Better performance (data locality)
- No single point of failure

**Nhược điểm:**
- Phức tạp hơn để setup

### 3. Cloud Storage (S3, GCS, Azure Blob)
```
┌─────────────────────────────────────────────┐
│         Cloud Storage (AWS S3)              │
│         s3://my-bucket/data/                │
└─────────────────────────────────────────────┘
         ▲              ▲              ▲
         │              │              │
     Master         Worker 1       Worker 2
```

**Ưu điểm:**
- Highly available
- Unlimited storage
- Managed service

**Nhược điểm:**
- Latency cao hơn
- Chi phí bandwidth

## Data Locality Optimization

Spark cố gắng tối ưu bằng cách:

```
1. NODE_LOCAL: Data và task trên cùng node
   ┌──────────────┐
   │   Worker 1   │
   │ - Data: ✓    │  ← Best case
   │ - Task: ✓    │
   └──────────────┘

2. RACK_LOCAL: Data và task trong cùng rack
   ┌──────────────┐     ┌──────────────┐
   │   Worker 1   │────→│   Worker 2   │
   │ - Data: ✓    │     │ - Task: ✓    │
   └──────────────┘     └──────────────┘
   (Same rack - fast network)

3. ANY: Data và task ở khác rack
   ┌──────────────┐           ┌──────────────┐
   │   Worker 1   │─────X─────│   Worker 3   │
   │ - Data: ✓    │           │ - Task: ✓    │
   └──────────────┘           └──────────────┘
   (Different rack - slower)
```

## Trong project này với Tailscale + NFS

```
┌─────────────────────────────────────────────────────────────┐
│              Master Node (100.101.102.103)                  │
│                                                             │
│  ┌──────────────┐         ┌─────────────────────────┐      │
│  │ Spark Master │         │ NFS Server              │      │
│  │ (Coordinator)│         │ /mnt/shared/            │      │
│  └──────────────┘         │ - raw_data/             │      │
│                           │ - data/processed/       │      │
│                           │ - data/embeddings/      │      │
│                           │ - data/models/          │      │
│                           └─────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
                                    ▲
                                    │ NFS over Tailscale VPN
                    ┌───────────────┼───────────────┐
                    │               │               │
        ┌───────────▼──────────┐   │   ┌───────────▼──────────┐
        │ Worker 1             │   │   │ Worker 2             │
        │ 100.101.102.104      │   │   │ 100.101.102.105      │
        │                      │   │   │                      │
        │ ┌──────────────────┐ │   │   │ ┌──────────────────┐ │
        │ │ Spark Worker     │ │   │   │ │ Spark Worker     │ │
        │ │ (Executor)       │ │   │   │ │ (Executor)       │ │
        │ └──────────────────┘ │   │   │ └──────────────────┘ │
        │                      │   │   │                      │
        │ /mnt/shared/         │   │   │ /mnt/shared/         │
        │ (NFS Mount)          │   │   │ (NFS Mount)          │
        │ ├─ raw_data/         │   │   │ ├─ raw_data/         │
        │ ├─ data/processed/   │   │   │ ├─ data/processed/   │
        │ ├─ data/embeddings/  │   │   │ ├─ data/embeddings/  │
        │ └─ data/models/      │   │   │ └─ data/models/      │
        └──────────────────────┘   │   └──────────────────────┘
                                   │
                       ┌───────────▼──────────┐
                       │ Worker 3             │
                       │ 100.101.102.106      │
                       │ /mnt/shared/         │
                       └──────────────────────┘
```

## Ví dụ cụ thể: Training ALS Model

```python
# 1. Driver (trên Master hoặc Jupyter) đọc metadata
ratings_df = spark.read.parquet("/mnt/shared/data/processed/ratings.parquet")
# → Master chỉ đọc schema, không đọc toàn bộ data

# 2. Master tạo execution plan
train_df, test_df = ratings_df.randomSplit([0.8, 0.2])
als = ALS(rank=50, maxIter=20)
model = als.fit(train_df)  # ← Trigger execution

# 3. Master phân bổ tasks
# Task 1 → Worker 1: "Đọc partition 0-99 từ /mnt/shared/..."
# Task 2 → Worker 2: "Đọc partition 100-199 từ /mnt/shared/..."
# Task 3 → Worker 3: "Đọc partition 200-299 từ /mnt/shared/..."

# 4. Workers đọc data TRỰC TIẾP từ /mnt/shared
# Worker 1: mount /mnt/shared → đọc part-00000.parquet
# Worker 2: mount /mnt/shared → đọc part-00001.parquet
# Worker 3: mount /mnt/shared → đọc part-00002.parquet

# 5. Workers xử lý ALS algorithm
# - Matrix factorization
# - Gradient descent
# - Shuffle data giữa workers khi cần

# 6. Workers ghi embeddings TRỰC TIẾP vào /mnt/shared
# Worker 1 → /mnt/shared/data/embeddings/part-00000.parquet
# Worker 2 → /mnt/shared/data/embeddings/part-00001.parquet
# Worker 3 → /mnt/shared/data/embeddings/part-00002.parquet
```

## Network Traffic Pattern

### ❌ Nếu Workers lấy data từ Master:
```
Master (Bottleneck!)
  ↓ 100MB/s
Worker 1
  ↓ 100MB/s
Worker 2
  ↓ 100MB/s
Worker 3

Total: 300MB/s qua Master → KHÔNG SCALE!
```

### ✅ Workers đọc từ Shared Storage:
```
Shared Storage (NFS Server)
  ↓ 100MB/s    ↓ 100MB/s    ↓ 100MB/s
Worker 1     Worker 2     Worker 3

Total: 300MB/s song song → SCALE TỐT!
Master chỉ gửi metadata và commands (KB/s)
```

## Monitoring Data Access

Để xem workers đang đọc/ghi từ đâu:

```bash
# Trên Worker node
# 1. Kiểm tra NFS mount
df -h | grep nfs
# Output: 100.101.102.103:/mnt/shared  100G  10G  90G  10% /mnt/shared

# 2. Monitor NFS traffic
nfsstat -c

# 3. Monitor disk I/O
iostat -x 1

# 4. Xem Spark UI
# http://100.101.102.103:8080
# → Click vào running application
# → Tab "Storage" → Xem data location
# → Tab "Executors" → Xem I/O metrics
```

## Tóm tắt

| Thành phần | Vai trò | Đọc/Ghi dữ liệu từ đâu |
|------------|---------|------------------------|
| **Master** | Điều phối, schedule tasks | Shared Storage (chỉ metadata) |
| **Workers** | Thực thi tasks, xử lý data | **Shared Storage (trực tiếp)** |
| **Shared Storage** | Lưu trữ tập trung | N/A |
| **Driver** | Submit jobs, collect results | Shared Storage hoặc local |

**Key Point:** Workers KHÔNG bao giờ lấy dữ liệu từ Master. Tất cả đều đọc/ghi trực tiếp từ Shared Storage!
