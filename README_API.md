# Book Recommendation API Server

Demo server cho hệ thống gợi ý sách sử dụng Spark ALS + LightGBM.

## Tính năng

- 🎯 **Gợi ý cá nhân hóa**: Nhập User ID để nhận gợi ý sách phù hợp
- 📚 **Lịch sử đọc**: Hiển thị các sách user đã đánh giá
- 🔍 **Tìm kiếm**: Tìm sách theo tên hoặc tác giả
- ⭐ **Sách phổ biến**: Xem các sách được đánh giá cao nhất
- 🧠 **Giải thích gợi ý**: Hiển thị điểm số từ ALS và LightGBM

## Cấu trúc

```
src/api/
├── app.py                      # Flask API server
└── recommendation_service.py   # Service xử lý recommendation logic

templates/
└── index.html                  # Web UI demo

docker/api-server/
├── Dockerfile
└── requirements.txt
```

## Khởi động

### 1. Build và start tất cả services

```bash
# Build API server
docker compose build api-server

# Start toàn bộ cluster
docker compose up -d
```

### 2. Kiểm tra services

```bash
# Check health
curl http://localhost:5000/api/health

# Xem logs
docker compose logs -f api-server
```

### 3. Truy cập Web UI

Mở trình duyệt: **http://localhost:5000**

## API Endpoints

### 1. Get Recommendations

**POST** `/api/recommend`

```json
{
  "user_id": "276725",
  "top_n": 10,
  "use_lgbm": true
}
```

Response:
```json
{
  "user_id": "276725",
  "recommendations": [
    {
      "isbn": "0316666343",
      "title": "The Catcher in the Rye",
      "author": "J.D. Salinger",
      "year": 1991,
      "publisher": "Little, Brown and Company",
      "als_score": 4.523,
      "lgbm_score": 4.812,
      "image_url": "http://..."
    }
  ],
  "user_history": [...],
  "count": 10
}
```

### 2. Search Books

**GET** `/api/search?q=harry+potter&limit=20`

Response:
```json
{
  "query": "harry potter",
  "results": [...],
  "count": 20
}
```

### 3. Popular Books

**GET** `/api/popular?top_n=20`

Response:
```json
{
  "results": [...],
  "count": 20
}
```

### 4. Health Check

**GET** `/api/health`

Response:
```json
{
  "status": "healthy",
  "spark_active": true,
  "service_ready": true
}
```

## Cách hoạt động

### 1. ALS Collaborative Filtering
- Tạo embeddings cho users và items từ lịch sử rating
- Dự đoán rating dựa trên similarity giữa user/item embeddings

### 2. LightGBM Re-ranking
- Sử dụng ALS embeddings làm features
- Train gradient boosting model để cải thiện độ chính xác
- Kết hợp cả user features và item features

### 3. Explanation
- **ALS Score**: Độ tương đồng giữa user và item trong không gian embedding
- **LightGBM Score**: Dự đoán rating sau khi re-ranking
- Hiển thị cả 2 scores để user hiểu tại sao sách được gợi ý

## Ví dụ sử dụng

### Từ Web UI
1. Nhập User ID (ví dụ: `276725`)
2. Chọn số lượng gợi ý (mặc định: 10)
3. Bật/tắt LightGBM re-ranking
4. Click "Get Recommendations"

### Từ Python

```python
import requests

# Get recommendations
response = requests.post('http://localhost:5000/api/recommend', json={
    'user_id': '276725',
    'top_n': 10,
    'use_lgbm': True
})

recommendations = response.json()
for book in recommendations['recommendations']:
    print(f"{book['title']} by {book['author']}")
    print(f"  ALS: {book['als_score']:.3f}, LightGBM: {book['lgbm_score']:.3f}")
```

### Từ cURL

```bash
curl -X POST http://localhost:5000/api/recommend \
  -H "Content-Type: application/json" \
  -d '{"user_id": "276725", "top_n": 10, "use_lgbm": true}'
```

## Troubleshooting

### API server không start
```bash
# Check logs
docker compose logs api-server

# Restart service
docker compose restart api-server
```

### Models không load được
```bash
# Verify models exist
docker compose exec api-server ls -la /opt/spark-data/models/

# Should see:
# - als_model/
# - lightgbm_model
```

### Spark connection error
```bash
# Check Spark master
docker compose ps spark-master

# Check workers
docker compose ps | grep spark-worker
```

## Performance

- **Cold start**: ~10-15s (load models)
- **Recommendation latency**: ~2-5s (depends on dataset size)
- **Memory usage**: ~2GB (Spark driver)

## Notes

- API server chạy trong Spark cluster, có thể scale bằng cách tăng số workers
- Models được load vào memory khi khởi động
- Sử dụng pandas để convert Spark DataFrame cho LightGBM prediction
- Web UI responsive, hoạt động tốt trên mobile

## Next Steps

- [ ] Add caching layer (Redis) để tăng tốc
- [ ] Implement batch prediction cho nhiều users
- [ ] Add A/B testing framework
- [ ] Export metrics (Prometheus)
- [ ] Add authentication/authorization
