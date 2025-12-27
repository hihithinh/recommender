# Hướng dẫn Triển khai với Tailscale VPN

## Tổng quan

Tailscale tạo một mạng VPN riêng (mesh network) giữa các máy, cho phép chúng kết nối với nhau như thể đang trong cùng một mạng LAN, ngay cả khi chúng ở các mạng khác nhau hoặc phía sau NAT/firewall.

## Ưu điểm của Tailscale

- ✅ Không cần mở ports trên firewall
- ✅ Kết nối được các máy ở mạng khác nhau
- ✅ Mã hóa end-to-end tự động
- ✅ Dễ dàng quản lý quyền truy cập
- ✅ IP cố định cho mỗi máy trong mạng Tailscale

## Bước 1: Cài đặt Tailscale

### Trên tất cả các máy (Ubuntu/Debian):

```bash
# Cài đặt Tailscale
curl -fsSL https://tailscale.com/install.sh | sh

# Khởi động Tailscale
sudo tailscale up

# Xác thực qua browser (sẽ mở link)
# Đăng nhập bằng tài khoản Google/GitHub/Microsoft
```

### Trên macOS:
```bash
brew install tailscale
sudo tailscale up
```

### Trên Windows:
Download và cài đặt từ: https://tailscale.com/download

## Bước 2: Kiểm tra IP Tailscale

Sau khi kết nối, kiểm tra IP Tailscale của mỗi máy:

```bash
# Xem IP Tailscale của máy hiện tại
tailscale ip -4

# Xem danh sách tất cả các máy trong mạng
tailscale status

# Output ví dụ:
# 100.101.102.103  master-node          user@    linux   -
# 100.101.102.104  worker-node-1        user@    linux   -
# 100.101.102.105  worker-node-2        user@    linux   -
```

## Bước 3: Cấu hình IP trong Project

### Cập nhật file `.env.tailscale`:

```bash
cd ~/recommender
cp .env.tailscale .env

# Chỉnh sửa file .env với IP thực tế
nano .env
```

**Ví dụ cấu hình:**
```bash
# Master Node (lấy từ: tailscale ip -4)
SPARK_MASTER_HOST=100.101.102.103

# Worker Nodes
WORKER_1_IP=100.101.102.104
WORKER_2_IP=100.101.102.105
WORKER_3_IP=100.101.102.106
```

### Cập nhật /etc/hosts (Optional nhưng recommended):

Trên tất cả các máy:
```bash
sudo nano /etc/hosts

# Thêm các dòng sau (thay bằng IP thực tế):
100.101.102.103 spark-master master-node
100.101.102.104 spark-worker-1 worker-node-1
100.101.102.105 spark-worker-2 worker-node-2
100.101.102.106 spark-worker-3 worker-node-3
```

## Bước 4: Thiết lập Shared Storage qua Tailscale

### Option 1: NFS qua Tailscale

#### Trên Master Node (NFS Server):

```bash
# Cài đặt NFS
sudo apt-get install -y nfs-kernel-server

# Tạo thư mục shared
sudo mkdir -p /mnt/shared/{data,raw_data,logs}
sudo chown -R $USER:$USER /mnt/shared

# Cấu hình exports với Tailscale subnet
sudo nano /etc/exports

# Thêm (cho phép toàn bộ Tailscale network 100.x.x.x/8):
/mnt/shared 100.0.0.0/8(rw,sync,no_subtree_check,no_root_squash)

# Hoặc chỉ định cụ thể từng worker:
/mnt/shared 100.101.102.104(rw,sync,no_subtree_check,no_root_squash)
/mnt/shared 100.101.102.105(rw,sync,no_subtree_check,no_root_squash)

# Apply changes
sudo exportfs -a
sudo systemctl restart nfs-kernel-server

# Copy dữ liệu
cp -r raw_data/* /mnt/shared/raw_data/
```

#### Trên Worker Nodes (NFS Clients):

```bash
# Cài đặt NFS client
sudo apt-get install -y nfs-common

# Tạo mount point
sudo mkdir -p /mnt/shared

# Mount NFS (sử dụng Tailscale IP của master)
sudo mount -t nfs 100.101.102.103:/mnt/shared /mnt/shared

# Auto-mount khi khởi động
echo "100.101.102.103:/mnt/shared /mnt/shared nfs defaults 0 0" | sudo tee -a /etc/fstab

# Test
ls -la /mnt/shared/raw_data/
```

### Option 2: Syncthing qua Tailscale

```bash
# Cài đặt Syncthing trên tất cả máy
curl -s https://syncthing.net/release-key.txt | sudo apt-key add -
echo "deb https://apt.syncthing.net/ syncthing stable" | sudo tee /etc/apt/sources.list.d/syncthing.list
sudo apt-get update
sudo apt-get install syncthing

# Khởi động Syncthing
syncthing

# Truy cập UI: http://localhost:8384
# Thêm các máy khác bằng Tailscale IP
```

## Bước 5: Triển khai Spark Cluster

### Trên Master Node:

```bash
cd ~/recommender

# Load environment variables
source .env

# Build và start master
docker-compose -f docker-compose.distributed.yml build spark-master jupyter

# Start services với Tailscale IP
export SPARK_MASTER_HOST=100.101.102.103
docker-compose -f docker-compose.distributed.yml up -d spark-master jupyter

# Kiểm tra
docker logs spark-master
```

### Trên Worker Node 1:

```bash
cd ~/recommender

# Load environment variables
source .env

# Build worker
docker-compose -f docker-compose.distributed.yml build spark-worker

# Start worker với Tailscale IPs
export SPARK_MASTER_HOST=100.101.102.103
export DOCKER_HOST_IP=100.101.102.104
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=8g
export HOSTNAME=worker-1

docker-compose -f docker-compose.distributed.yml up -d spark-worker

# Kiểm tra
docker logs spark-worker
```

### Trên Worker Node 2:

```bash
cd ~/recommender

# Start worker
export SPARK_MASTER_HOST=100.101.102.103
export DOCKER_HOST_IP=100.101.102.105
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=8g
export HOSTNAME=worker-2

docker-compose -f docker-compose.distributed.yml up -d spark-worker
```

## Bước 6: Truy cập Services

### Từ bất kỳ máy nào trong Tailscale network:

```bash
# Spark Master UI
http://100.101.102.103:8080

# Jupyter Lab
http://100.101.102.103:8888

# Worker 1 UI
http://100.101.102.104:8081

# Worker 2 UI
http://100.101.102.105:8081
```

### Từ máy local (nếu có Tailscale):

Cài Tailscale trên laptop/desktop của bạn và truy cập trực tiếp qua Tailscale IP!

## Bước 7: Test Connectivity

```bash
# Từ Master node, test kết nối đến workers
ping 100.101.102.104
ping 100.101.102.105

# Test Spark port
telnet 100.101.102.104 7000

# Từ Worker, test kết nối đến master
ping 100.101.102.103
telnet 100.101.102.103 7077
```

## Bước 8: Chạy Jobs

```bash
# Submit job từ Master
docker exec spark-master spark-submit \
  --master spark://100.101.102.103:7077 \
  --deploy-mode client \
  --executor-memory 4g \
  --executor-cores 2 \
  --total-executor-cores 8 \
  /opt/scripts/run_preprocessing.py
```

## Script Tự động Lấy IP

Tạo script để tự động lấy Tailscale IP:

```bash
# create-env-from-tailscale.sh
#!/bin/bash

MASTER_IP=$(tailscale ip -4)

cat > .env << EOF
# Auto-generated from Tailscale
SPARK_MASTER_HOST=${MASTER_IP}
DOCKER_HOST_IP=${MASTER_IP}
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=8g

# Add other configs...
EOF

echo "Created .env with Tailscale IP: ${MASTER_IP}"
```

Chạy script:
```bash
chmod +x create-env-from-tailscale.sh
./create-env-from-tailscale.sh
```

## Quản lý Tailscale Network

### Xem danh sách máy:
```bash
tailscale status
```

### Tắt Tailscale tạm thời:
```bash
sudo tailscale down
```

### Bật lại:
```bash
sudo tailscale up
```

### Xóa máy khỏi network:
```bash
# Từ Tailscale admin console: https://login.tailscale.com/admin/machines
# Hoặc:
sudo tailscale logout
```

## Troubleshooting

### Worker không kết nối được Master:

```bash
# 1. Kiểm tra Tailscale đang chạy
tailscale status

# 2. Ping master
ping 100.101.102.103

# 3. Test Spark port
telnet 100.101.102.103 7077

# 4. Kiểm tra Docker logs
docker logs spark-worker

# 5. Restart Tailscale
sudo systemctl restart tailscaled
```

### NFS mount failed:

```bash
# Kiểm tra NFS exports trên master
sudo exportfs -v

# Test NFS từ worker
showmount -e 100.101.102.103

# Mount thủ công để debug
sudo mount -v -t nfs 100.101.102.103:/mnt/shared /mnt/shared
```

### Slow performance:

```bash
# Kiểm tra Tailscale connection type
tailscale status

# Nếu thấy "relay" thay vì "direct", có thể cần:
# 1. Enable UPnP trên router
# 2. Hoặc chấp nhận relay (vẫn nhanh, chỉ chậm hơn direct một chút)
```

## Bảo mật

### Tailscale ACLs (Access Control Lists):

Trong Tailscale admin console, bạn có thể giới hạn:
- Máy nào được kết nối với máy nào
- Port nào được mở
- User nào có quyền truy cập

Ví dụ ACL:
```json
{
  "acls": [
    {
      "action": "accept",
      "src": ["tag:worker"],
      "dst": ["tag:master:7077"]
    },
    {
      "action": "accept",
      "src": ["tag:master"],
      "dst": ["tag:worker:*"]
    }
  ]
}
```

## Lợi ích của Tailscale cho Project này

1. **Không cần Public IP**: Các máy có thể ở sau NAT/firewall
2. **Bảo mật**: Mã hóa WireGuard tự động
3. **Dễ setup**: Không cần config firewall phức tạp
4. **Linh hoạt**: Thêm/bớt máy dễ dàng
5. **Remote access**: Truy cập cluster từ bất kỳ đâu
6. **Cross-platform**: Linux, macOS, Windows đều support

## Tóm tắt File Cấu hình

```
.env.tailscale          # Template với placeholder IPs
.env                    # File thực tế với Tailscale IPs (git ignored)
/etc/hosts              # Hostname mapping (optional)
```

**Workflow:**
1. Cài Tailscale trên tất cả máy
2. Lấy IP: `tailscale ip -4`
3. Cập nhật `.env` với IP thực tế
4. Deploy cluster như bình thường
