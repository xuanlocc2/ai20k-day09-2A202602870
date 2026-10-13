# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Nguyễn Văn Xuân Lộc` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602870` |
| GitHub | `xuanlocc2` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Từ 2 dòng mẫu đầu của `bronze.raw_telemetry_a` và `bronze.raw_telemetry_b` (bước 1):
1. **Timestamp:** A dùng `ts_utc` dạng chuỗi ISO-8601 UTC (`2025-10-07T08:00:00.014Z`); B dùng `timestamp_ms` là epoch mili-giây (`1759933801509`). Ngoài ra đồng hồ B lệch video `telemetry_clock_offset_ms = 1500` (A = 0).
2. **Tốc độ khác đơn vị:** A `speed_mps` (m/s, `1.213`); B `spd_kmh` (km/h, `55.11`).
3. **Ga/phanh khác thang:** A `gas`/`brake` là tỉ lệ 0–1 (`0.028`); B `throttle_pct`/`brake_pct` là phần trăm 0–100 (`2.0`).
4. **Tên cột khác nhau cho cùng đại lượng:** `heading_deg`↔`hdg`, `lat`/`lon`↔`latitude`/`longitude`, `alt_m`↔`altitude`, `acc_x`/`acc_y`↔`ax`/`ay`, `steer`↔`steering`.

Mọi cột telemetry ở bronze đều là `VARCHAR`, kể cả giá trị trống (30 dòng `speed` rỗng ở mỗi nguồn). Giữ mỗi nguồn một bảng, dạng chữ, để bronze là bản sao **trung thực, chỉ thêm** của file gốc (đối chiếu được 3021 = 3021, 3234 = 3234 dòng CSV): nếu ép kiểu ngay thì dòng hỏng (ô trống, giá trị lạ) sẽ làm lỗi cả lượt nạp hoặc bị biến thành NULL/xóa im lặng, và nếu gộp ngay thì phải chọn đơn vị/tên cột chung trước khi biết hết khác biệt. Đẩy việc đổi đơn vị, ép kiểu, bỏ lệch đồng hồ và cách ly dòng lỗi sang silver giúp sửa logic rồi chạy lại từ bronze mà không cần nạp lại nguồn. Bằng chứng nạp lặp không nhân đôi: lần nạp thứ hai báo `already present`, `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, 3021 dòng); `bronze.video_files` có 2 dòng với 2 sha256 khác nhau (mỗi drive 1 commit), video nhận diện bằng SHA256 chứ không phải tên file.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Hai model staging đưa cả hai nguồn về cùng schema `ts timestamptz, speed_mps double, heading_deg, lat, lon, ... gas, brake` (ga/phanh 0–1):
- **A** (`stg_telemetry_a.sql`): chỉ đổi tên/ép kiểu, `cast(ts_utc as timestamptz)`; tốc độ đã là m/s, offset = 0.
- **B** (`stg_telemetry_b.sql`): `spd_kmh / 3.6` → m/s; `throttle_pct/100`, `brake_pct/100`; thời gian `to_timestamp((timestamp_ms - telemetry_clock_offset_ms) / 1000.0)`, với offset = **1500 ms** lấy từ `bronze.video_files` (manifest lúc ingest), không viết cứng.
- Ô trống được `try_cast(nullif(speed, '') as double)` thành NULL để sau đó phân loại `speed_missing`.

Bằng chứng: sau chuẩn hoá, mẫu đầu của B là `2025-10-08 14:30:00.009+00` trong khi video B bắt đầu `14:30:00+00` (lệch 9 ms, check "drive B clock offset removed" PASS); nếu không trừ offset thì sẽ lệch 1,5 s. Tốc độ tối đa sau chuẩn hoá: A 71 km/h, B 100 km/h, cả hai hợp lý (< 130 km/h), check "all speeds in 0..70 m/s" PASS.

| drive | duplicate | speed_missing | speed_out_of_range | rejects | sạch | nhận |
|---|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 53 | 3181 | 3234 |

Tổng: `silver.telemetry` 6149 dòng, `silver.telemetry_rejects` 106 dòng; sạch + rejects = nhận (2968+53=3021, 3181+53=3234), không dòng nào biến mất. Dòng lỗi vào `rejects` kèm `reject_reason`, `_batch_id`, `_row_no` (ví dụ drive_a `_row_no=1700` có `speed_mps=250.0`) để đếm, truy ngược đúng dòng trong file nguồn và báo lại cho bên cung cấp. Xoá im lặng thì không chứng minh được số dòng khớp, không phát hiện được nguồn đang xấu dần, và không thể khôi phục nếu luật làm sạch sai.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (`max_ms: 1000`, `severity: warn` trong `schema.yml`) bắt khoảng hở > 1 s giữa hai mẫu: ở bước 2 nó WARN vì mỗi drive có 1 khoảng hở (A 3073 ms, B 3124 ms). Đây là **cảm biến mất tín hiệu** — dữ liệu còn lại vẫn đúng, frame trong khoảng hở chỉ không ghép được telemetry (sau này bị loại vì mất đồng bộ), nên chỉ cần báo để điều tra chứ không đáng dừng cả pipeline. `in_range` (0..70 m/s) là `error` vì một giá trị ngoài khoảng là **dữ liệu sai** (250 m/s = 900 km/h); nếu lọt vào gold nó làm hỏng nhãn/đặc trưng tốc độ mà mô hình học, nên phải chặn.

`break_gate.json`: khi nới `max_speed_mps` lên 1000, các dòng vô lý lọt khỏi quarantine vào silver; dbt test chuyển từ `PASS=6 WARN=1 ERROR=0` sang `PASS=5 WARN=1 ERROR=1`, test fail là `in_range_silver_telemetry_speed_mps__70__0` với **6 dòng** (đúng 3 dòng `speed_out_of_range` × 2 drive). Khôi phục luật → `PASS=6 WARN=1 ERROR=0`. Nghĩa là cổng chất lượng là lớp phòng thủ **độc lập** với logic làm sạch: khi ai đó sửa nhầm luật làm sạch, test `error` vẫn chặn pipeline trước khi dữ liệu xấu tới gold, còn `warn` chỉ ghi nhận mà không chặn.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
