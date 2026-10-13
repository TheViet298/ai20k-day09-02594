# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `TheViet298` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602594` |
| GitHub | `TheViet298` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04622` |

## Kết quả cuối

- `dataset_version` của bạn: `0660117289cf`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `699 / 146 / 174`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A dùng các cột `ts_utc`, `speed_mps`, `heading_deg`, `lat/lon`, còn nguồn B dùng `timestamp_ms`, `spd_kmh`, `hdg`, `latitude/longitude`; B còn có `throttle_pct`, `brake_pct` và `steering`. A đã ở m/s và ISO UTC, trong khi B ở km/h và epoch ms. Pipeline giữ hai bảng Bronze riêng, ghi raw text và thêm lineage/batch/row number để không làm mất dữ liệu hoặc ép kiểu sai trước khi xử lý schema drift.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Hai nguồn được chuẩn hoá về speed m/s, timestamp UTC và schema chung; tốc độ B được đổi từ km/h, epoch ms được đổi sang timestamp, đồng thời trừ độ lệch đồng hồ 1,5 giây. Drive A có 2968 dòng sạch và drive B có 3181 dòng sạch. Mỗi drive có 20 duplicate, 30 speed_missing và 3 speed_out_of_range; tổng quarantine là 53 dòng/drive. Dòng lỗi được giữ trong rejects để audit, sửa hoặc tái xử lý, thay vì xoá không truy vết.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` là cảnh báo vì mất tín hiệu là vấn đề chất lượng cần theo dõi nhưng chưa chắc làm dữ liệu không dùng được. `in_range` là error vì tốc độ ngoài miền hợp lệ có thể làm sai mô hình và phải chặn pipeline. Khi nới ngưỡng lên 1000 m/s, gate bắt test `in_range_silver_telemetry_speed_mps__70__0` với 6 dòng; kết quả PASS=5, WARN=1, ERROR=1. Khôi phục luật cho PASS=6, WARN=1, ERROR=0.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Theo `idempotency.json`, trước khi chạy lại có 1246 dòng và sau hai lần chạy lại vẫn là 1246 dòng, fingerprint đều là `6c7480dfde7e`. Riêng một trip có 60 frame, cách append sai tạo 120 dòng sau hai lần chạy. Delta overwrite theo predicate của một partition trip thay thế đúng phạm vi cần cập nhật và giữ transaction atomic; vì vậy chạy lại sau lỗi giữa chừng không cộng dồn bản ghi và an toàn hơn xoá rồi ghi thủ công.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen gần như không có tín hiệu ảnh và được loại khỏi dataset; khung tối vẫn có cấu trúc nên được gắn cờ nhưng giữ lại. Tôi chọn ngưỡng dHash 2 như pipeline dùng: đủ bắt frame gần trùng nhưng ít loại nhầm cảnh khác nhau hơn ngưỡng 6. Ở ngưỡng 0/2/6, drive A có 61/149/363 frame trùng và drive B có 8/53/244. Ngưỡng phù hợp còn phụ thuộc tốc độ xe: xe chạy chậm hoặc camera đứng yên tạo nhiều frame giống nhau hơn.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json` cho thấy chia ngẫu nhiên theo frame làm cả 26 trip bị chia cắt và cả 26 trip có frame ở cả train lẫn test; chia theo trip làm 0 trip bị chia cắt. Các frame liền nhau rất giống nhau, nên mô hình có thể gần như đã nhìn thấy cùng cảnh ở train rồi đạt điểm test giả tạo. Vì vậy split phải theo đơn vị cảnh/chuyến đi, không theo frame độc lập.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là hash nội dung của danh sách frame và split nên cùng input, cùng MSSV và cùng quy tắc sẽ tái tạo cùng version; nó không phụ thuộc thời điểm chạy hay số lần chạy. Lần chạy lại cho version `0660117289cf` ổn định. Bạn bè có MSSV khác sẽ có thứ tự phân chia trip khác, nên nội dung split và hash version khác dù dùng cùng file dữ liệu gốc.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Lineage của dataset gồm các bảng Bronze `raw_telemetry_a`, `raw_telemetry_b`, `trips_raw`, `video_files` và các bảng Silver `frames`, `telemetry`, `frames_flagged`, `frame_telemetry`, cùng SHA256 của hai video đầu vào. Manifest còn ghi dataset version, hash manifest, luật loại frame và split theo trip. Sau ba tháng, các table versions, `data/MANIFEST.json`, SHA256 video, MSSV và các rule này là thông tin cần để tái tạo đúng dataset.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Với trip được chọn `trip-04622`, kịch bản retry làm đúng một task `silver_frames` chuyển đỏ; các task downstream phụ thuộc trực tiếp chuyển `upstream_failed`. Tôi chỉ clear task đỏ để chạy lại, sau đó DAG tiếp tục xanh. Cách ghi overwrite theo partition nên không nhân đôi dữ liệu: verify vẫn là 27/27, gold vẫn 1019 frame và dataset version vẫn `0660117289cf`. Ảnh log task đỏ và graph xanh sau retry được lưu trong `report/screenshots/` khi hoàn tất chụp.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi tăng từ 2 lên 500 xe/ngày, đầu tiên phần xử lý frame bằng một DAG LocalExecutor, MinIO đơn lẻ và Postgres warehouse sẽ nghẽn do CPU, I/O, số task và dung lượng. Tôi sẽ chuyển ingestion sang queue/object-storage có partition theo ngày-xe, dùng worker phân tán như Kubernetes/Celery, table format lakehouse có compaction, và nạp warehouse theo batch/incremental. dbt vẫn giữ quality gate nhưng cần orchestration và autoscaling phù hợp.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

SQL tôi thêm trên Metabase: `SELECT trip_id, COUNT(*) AS frames, SUM(CASE WHEN is_black OR is_blurry OR is_near_dup THEN 1 ELSE 0 END) AS rejected, ROUND(100.0 * SUM(CASE WHEN is_black OR is_blurry OR is_near_dup THEN 1 ELSE 0 END) / COUNT(*), 2) AS reject_pct FROM marts.mart_frame_funnel GROUP BY trip_id ORDER BY reject_pct DESC;`. Câu hỏi này cho biết trip nào có tỷ lệ frame bị loại cao nhất để ưu tiên kiểm tra camera hoặc dữ liệu.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều bất ngờ nhất là các lỗi được cài vào dữ liệu vẫn được giữ lại có lý do thay vì biến mất, và chia theo frame làm leakage ở cả 26 trip. Tôi còn cần hiểu sâu hơn cách chọn ngưỡng dHash theo từng tốc độ/camera. Một lỗi pipeline có thể chưa bắt tốt là frame hợp lệ nhưng sai nội dung do camera bị che một phần; các tín hiệu đen, mờ và dHash không nhất thiết phát hiện được trường hợp đó.
