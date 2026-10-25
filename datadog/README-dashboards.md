# Dashboard PulseOps trên Datadog

3 dashboard chỉ dùng **dữ liệu thật** do hệ thống PulseOps sinh ra. Không dùng sample log nhập tay.

## Dữ liệu dùng

| Nguồn | Sinh ra bởi | Cách tìm trong Datadog |
|---|---|---|
| Metric `pulseops.*` (máy UP/DOWN, số test, PASS/FAIL, vi phạm QA, nhiệt độ, cycle time, giá trị đo, độ trễ xử lý, consumer pause) | `service/pulseops/metrics.py`, được Datadog Agent scrape trên cluster Kind | **Metrics → Explorer**, gõ `pulseops.` |
| `kafka.consumer_lag`, `network.http.can_connect`, `kubernetes_state.*`, `mongodb.*` | Datadog Agent (check Kafka, HTTP, Kubernetes, MongoDB) | Metrics Explorer, lọc `kube_namespace:pulseops` |
| Sự kiện cảnh báo của 6 monitor `[PulseOps]` (trigger / warn / no data / recover) | `datadog/monitors/*.json` (tạo bằng `apply.sh`) | **Events → Explorer**: `source:alert app:pulseops` |

Ghi chú: Datadog chỉ có dữ liệu trong những khoảng cluster đang chạy. Nếu một widget hiện *No data*, hãy đổi khung thời gian ở góc phải trên (ví dụ **Past 1 Day**).

## Cách import (cho từng file trong `dashboards/`)
1. **Dashboards → New Dashboard → Blank Dashboard**, thêm một widget Note bất kỳ rồi **Save**. Dashboard phải được lưu trước, nếu không sẽ gặp lỗi "Dashboard with ID … not found".
2. **⚙ Configure → Import dashboard JSON**, kéo thả hoặc dán file JSON, xác nhận thay thế.

Nếu đã import bản cũ: mở dashboard đó và import đè file mới theo bước 2.

Cách khác: `set -a; . ./.env; set +a; ./datadog/apply-dashboards.sh` (cần `DD_API_KEY`, `DD_APP_KEY`). Script tạo mới hoặc cập nhật theo tên dashboard.

## Metric mới (widget có dấu ⓝ)

7 metric mới đã được thêm vào service. Tên trong Datadog:

| Metric | Ý nghĩa | Nằm ở dashboard |
|---|---|---|
| `pulseops.product_status_changes.count{status}` | Số sản phẩm đạt PASSED / FAILED / NON_CONFORMING, dùng để tính yield toàn dây chuyền | 1 |
| `pulseops.product_line_seconds.*` | Thời gian một sản phẩm PASSED đi từ machine-1 đến machine-3 | 1, 2 |
| `pulseops.measurement_zone.count{machine,name,zone}` | Phép đo theo vùng spec: ok / marginal (gần giới hạn) / out | 3 |
| `pulseops.heartbeats.count{machine}` | Số heartbeat nhận được (kỳ vọng 12/phút) | 3 |
| `pulseops.machine_status_changes.count{machine,status}` | Số lần máy chuyển UP↔DOWN (máy chập chờn) | 3 |
| `pulseops.pending_qa_checks` | Số QA check đang chờ kết quả upstream | 1, 2 |
| `pulseops.mongodb_operation_seconds.*{operation}` | Thời gian của từng thao tác MongoDB | 2 |

Các chỉ số **tính từ metric cũ** cũng được thêm vào và có dữ liệu ngay: tỷ lệ FAIL, vi phạm QA trên 100 sản phẩm, % thời gian máy DOWN, tỷ lệ FAIL theo máy.

### Deploy service bản mới (để widget ⓝ có dữ liệu)
Chép các file trong thư mục `repo/` của gói này đè lên repo (hoặc `git apply pulseops-new-metrics.patch`), rồi chạy:
```bash
cd service && uv run pytest && cd ..                     # chạy test
docker build -t pulseops-service:dev service && kind load docker-image pulseops-service:dev --name pulseops
kubectl apply -k k8s/overlays/local                      # annotation mới để Datadog Agent thu thập metric mới
kubectl rollout restart -n pulseops deployment/pulseops-service
```
Khoảng 1–2 phút sau, kiểm tra trong **Metrics → Explorer**, ví dụ với `pulseops.heartbeats.count`.

Lưu ý:
- Với dữ liệu từ simulator, vùng `marginal` gần như luôn bằng 0. Simulator chỉ sinh giá trị PASS ở giữa spec, và giá trị FAIL nằm hẳn ngoài spec. Với máy thật, chỉ số này giúp thấy máy đang trôi dần về giới hạn trước khi bắt đầu FAIL.
- Một sản phẩm đổi trạng thái sau này (ví dụ PASSED → NON_CONFORMING khi hết thời gian chờ QA) sẽ được đếm thêm ở trạng thái mới.

## Mô tả

### 1. PulseOps – Operations Overview (dùng cho demo)
- **KPI:** số máy đang UP (x/3), số cảnh báo phát sinh, số sản phẩm đã kiểm tra, tỷ lệ PASS (đỏ khi <90%, vàng khi <97%), số vi phạm QA, độ trễ xử lý trung bình.
- **Dây chuyền sản xuất:** bảng UP/DOWN và số giây im lặng của từng máy, throughput (sản phẩm/phút), vi phạm QA theo loại (MISSING_UPSTREAM / FAILED_UPSTREAM).
- **Cảnh báo & sự cố:** trạng thái 6 monitor, danh sách cảnh báo gần đây kèm lúc phục hồi.
- **Pipeline:** Kafka lag, độ trễ xử lý, consumer pause.
- Biến `machine` dùng để lọc theo máy.

### 2. PulseOps – Pipeline & Observability
- **KPI:** độ trễ, số message đã xử lý, số message bị reject, Kafka lag (đỏ khi >500, cùng ngưỡng với monitor), consumer bị pause, số replica sẵn sàng (đỏ khi <2).
- **Ingestion & latency:** độ trễ theo thời gian, message/phút, lag theo topic, pause theo pod, reject theo lý do.
- **Sức khỏe service & hạ tầng:** `/health` theo pod, replica, container restart, MongoDB ops/s và số connection.
- **Cảnh báo pipeline:** trạng thái và lịch sử các monitor Kafka lag, Consumption paused, Service unhealthy.

### 3. PulseOps – Device Health & Incident Investigation
- **Nhiệt độ & tình trạng máy:** nhiệt độ từng máy; biểu đồ *anomaly*, trong đó dải xám là mức bình thường Datadog học từ lịch sử và điểm đỏ là bất thường (dùng để phát hiện quá nhiệt mà không cần ngưỡng cố định); nhiệt độ cao nhất; số giây im lặng (vạch 15s, quá ngưỡng thì máy bị coi là DOWN); UP/DOWN theo thời gian.
- **Chất lượng sản xuất:** tỷ lệ PASS (vạch 90% theo monitor), cycle time, vi phạm QA, giá trị đo của từng máy so với spec: length 119.85–120.15 mm, leak ≤ 0.50 mL/min, voltage 4.90–5.10 V.
- **Điều tra cảnh báo theo máy:** số sự kiện theo máy và trạng thái, lịch sử cảnh báo và phục hồi. Chọn máy ở biến `machine` để khoanh vùng.

## Chưa có trong dữ liệu
- Nhiệt độ của máy chưa có ngưỡng cảnh báo chính thức trong code, nên dashboard dùng anomaly detection. Nếu nhóm thống nhất được ngưỡng, có thể thêm monitor và vạch ngưỡng.
- Datadog Incidents đang trống. Phần "sự cố" hiện dựa trên sự kiện cảnh báo của monitor.
