# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Nguyễn Văn Xuân Lộc` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602870` |
| GitHub | `xuanlocc2` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04237` |

## Kết quả cuối

- `dataset_version` của bạn: `ecfe1509494e`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `679 / 186 / 154` (tổng 1019; trip 18 / 4 / 4)
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

`idempotency.json` (trip `trip-00003`, 60 khung): trước khi chạy lại `silver.frames` có **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau **2 lần** chạy lại trip đó vẫn **1246 dòng**, vân tay `6c7480dfde7e` (`unchanged: true`). Cách sai ghi `append` hai lần cho một trip: **120 dòng** cho trip chỉ có 60 khung → nhân đôi.

Pipeline ghi bằng Delta `overwrite` với `predicate = "trip_id = 'trip-00003'"` (`pipeline/frames.py`): xoá-dòng-cũ và thêm-dòng-mới của trip nằm trong **cùng một commit** của transaction log. Nếu lỗi giữa chừng (container chết khi đang ghi Parquet), commit không xảy ra, người đọc vẫn thấy phiên bản cũ đầy đủ; chạy lại chỉ cần làm lại đúng trip đó. "Xoá rồi ghi lại" là hai thao tác tách rời: sập sau bước xoá thì trip mất dữ liệu (bảng thiếu 60 khung mà không ai biết), và người đọc giữa hai bước có thể thấy bảng thiếu. Ngoài ra overwrite theo partition chỉ đụng một trip, 25 trip còn lại không bị ghi lại.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 / 602 | 8 / 644 |
| 2 | 149 / 602 | 53 / 644 |
| 6 | 363 / 602 | 244 / 644 |

**Đen** là độ sáng trung bình < 12 (`dark_luma`) — ống kính bị che, lỗi giải mã, không có thông tin ảnh — bị `is_black` và **loại** khỏi gold. **Tối** là 12 ≤ độ sáng < 40 (`dark_soft`) — đường hầm, bóng râm, chiều tối — chỉ gắn `is_dark` và **giữ lại**. Ở bước 3: drive A có 6 khung đen, 4 khung tối; drive B có 4 khung đen nhưng **61 khung tối được giữ**. Mình đồng ý: mô hình chạy ngoài đường thật sẽ gặp đường hầm, bỏ các khung tối sẽ làm dataset thiên về điều kiện đẹp; cờ `is_dark` vẫn cho phép lọc/cân bằng sau nếu cần. (Khung mờ cũng chỉ tính khi độ sáng ≥ 12, để một khung đen không bị đếm hai lần.)

Ngưỡng 0 chỉ bắt khung giống hệt bit (A 61, B 8) — bỏ sót camera đứng hình có nhiễu nén nhẹ; ngưỡng 6 coi 363/602 khung A (60%) là trùng, chắc chắn loại cả khung khác nhau thật khi xe chạy chậm. Mình chọn giữ **ngưỡng 2**: A 149 khung trùng, bắt được 11/12 khung của đoạn camera đứng hình 6 giây ở A (check PASS), trong khi B chỉ 53. Ngưỡng phụ thuộc tốc độ: lấy mẫu 2 khung/giây, xe đứng yên hoặc chạy chậm (A, tối đa 71 km/h, nhiều đoạn dừng) thì hai khung liên tiếp gần như giống nhau → nhiều khung "trùng"; xe nhanh (B, tối đa 100 km/h) thì cảnh đổi nhiều giữa hai khung → ít trùng. Cải tiến: ngưỡng thay đổi theo `speed_mps` (xe dừng: ngưỡng cao; xe chạy nhanh: ngưỡng thấp), hoặc lấy mẫu theo quãng đường thay vì theo thời gian.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json`: với 1019 khung đáng dùng thuộc 26 trip, chia **theo khung hình** ngẫu nhiên 70/15/15 làm **26/26 trip** nằm ở nhiều hơn một split, và cả **26 trip** có khung ở cả train lẫn test. Chia **theo trip** (gold của pipeline) thì **0 trip** bị chia cắt; bước 4 cho thấy mỗi drive vẫn có đủ train/val/test (A 9/2/2 trip, B 9/2/2 trip).

Lấy mẫu 2 khung/giây nên hai khung liền nhau của cùng một trip gần như giống hệt (cùng đoạn đường, cùng ánh sáng, cùng xe phía trước). Khi chia theo khung, gần như mọi khung test đều có "anh em sinh đôi" trong train: mô hình chỉ cần nhớ chứ không cần tổng quát hoá, nên điểm test cao giả tạo — nó đo khả năng nhận lại cảnh đã thấy, không phải khả năng chạy trên đoạn đường mới. Khi triển khai trên trip mới, điểm sẽ tụt và ta không biết trước.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`pipeline/publish.py` tính `dataset_version = sha256("frame_id|split" của mọi khung, theo thứ tự)[:12]`. Vì là băm **nội dung**, cùng tập khung và cùng cách chia luôn cho cùng phiên bản; chỉ cần một khung thêm/bớt hoặc một trip đổi split là phiên bản đổi. Ngày giờ hay số đếm sẽ đổi mỗi lần chạy dù dữ liệu y hệt (không biết hai dataset có giống nhau không, chạy lại thì tạo "phiên bản mới" giả), và hai người chạy cùng lúc có thể ra cùng số đếm cho hai dataset khác nhau.

Hai lần chạy liên tiếp của mình: `step 4` rồi `cli export-dataset` đều ra **`ecfe1509494e`** (1019 khung, 679/186/154), giống cả lượt chạy thử ban đầu `step all --reset` → ổn định. Phiên bản của mình khác bạn bè dù cùng video/telemetry vì `gold_dataset_frames.sql` xếp trip theo `md5(trip_id || LAB_STUDENT_ID)`: MSSV `2A202602870` làm muối nên thứ tự trip, và do đó trip nào vào train/val/test, khác với MSSV khác → cặp `frame_id|split` khác → băm khác.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/ecfe1509494e/manifest.json` (cạnh `train/`, `val/`, `test/`, `manifest.parquet`) có:

```json
"lineage": {
  "bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
  "silver.frames": 27, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
  "input_video_sha256": {
    "drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa",
    "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc" } },
"rules": { "drop": ["is_black", "is_blurry", "is_near_dup", "not sync_ok"], "split": "per trip, stratified by drive, md5 order" },
"manifest_sha256": "19efb905663c5a3d94ad38ec1a330bf0e1840f47e349e52b8b72e9802d87290e"
```

Trả lời "dataset này được tạo từ đâu": từ hai video có đúng hai sha256 trên, qua `silver.frames` **phiên bản 27** (26 commit đầu = 26 trip, +2 commit từ thí nghiệm idempotency chạy lại trip-00003), `silver.telemetry` v0 và các bảng bronze v0/v1, bỏ khung đen/mờ/gần trùng/mất đồng bộ, chia theo trip. 3 tháng sau, để tái tạo: dùng Delta **time travel** đọc đúng các phiên bản bảng ghi trong `lineage`, kiểm tra video gốc còn khớp sha256, chạy lại với cùng `rules`, cùng code (commit Git) và cùng MSSV `2A202602870` (muối chia split); kết quả phải ra lại `dataset_version = ecfe1509494e` và `manifest_sha256` như trên — nếu khác là biết ngay có gì đã đổi.

Đối chiếu: sau lượt chạy cuối `step all --reset` (không có thí nghiệm chạy lại), `lineage` ghi `silver.frames: 25` (26 commit = 26 trip) thay vì 27, nhưng `dataset_version` vẫn `ecfe1509494e` và `manifest_sha256` vẫn `19efb905…290e`. Tức là lineage ghi **lịch sử bảng đã tạo ra** dataset (khác nhau giữa hai lần chạy), còn version/manifest_sha256 đo **nội dung** dataset (giống nhau) — cần cả hai để vừa tái tạo vừa kiểm chứng.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip của mình: **`trip-04237`** (5 số cuối MSSV `02870` = 2870, 2870 mod 26 = 10 → phần tử thứ 10 của `list-trips`; công thức `int(MSSV) % 26` không dùng được vì MSSV có chữ "A"). Đặt `LAB_FAIL_ONCE=trip-04237`, trigger `day09_reset` rồi `day09_video_pipeline` (run `manual__2026-10-13T08:31:05`):
- **1 task đỏ**: `silver_frames` map_index 10, log `RuntimeError: simulated crash while extracting trip-04237; run it again` (`fail_task_log.png`). 25 task `silver_frames` còn lại vẫn **success** vì mỗi trip là một task mapped độc lập.
- **7 task `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse` — quality gate và gold không chạy trên dữ liệu thiếu một trip. Tổng run: 29 success, 1 failed, 7 upstream_failed.
- **Clear** chỉ task instance `silver_frames[10]` (kèm downstream, không upstream): bronze và 25 trip khác **không** chạy lại. Mình lỡ bấm Clear hai lần nên trip-04237 chạy lại 2 lần (attempt 2 và 3, mỗi lần `{"trip": "trip-04237", "frames": 60}`), sau đó 7 task phía sau chạy tiếp, run chuyển **success** — 37/37 task xanh (`airflow_retry_graph.png`).
- **Không nhân đôi**: `silver.frames` có 2 commit mới (v25, v26) đều là overwrite 60 dòng của đúng trip đó, nhưng `frame_id unique` vẫn **1246 khung**, trip-04237 vẫn **60 khung**. `scripts/lab.sh verify`: **27/27**, `dataset_version` vẫn **`ecfe1509494e`** — giống hệt lượt chạy không lỗi (`airflow_graph.png`, run `08:18:52`).

Kết luận: nhờ task theo partition + Delta overwrite theo `trip_id`, sửa lỗi chỉ tốn chạy lại 1/26 phần việc nặng, và chạy lại thừa (bấm clear 2 lần) cũng vô hại.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Hiện tại: 2 drive ≈ 10 phút video → 26 trip, 1246 khung, DAG ≈ 2–3 phút. 500 xe/ngày (giả sử mỗi xe vài giờ video) là gấp hàng nghìn lần:
1. **Trích khung hình (`silver_frames`)** vỡ đầu tiên: decode video + tính độ sáng/blur/dHash bằng CPU trong **một** container Airflow, `max_active_tis_per_dagrun=1` (chạy tuần tự từng trip). Thay bằng worker phân tán (KubernetesExecutor/Celery hoặc Spark/Ray), decode bằng GPU, mỗi trip/xe một job song song; Airflow chỉ điều phối chứ không tự làm việc nặng.
2. **Airflow dynamic mapping một DAG cho mọi trip**: 500 xe × hàng chục trip = hàng chục nghìn task mapped mỗi lần chạy, scheduler và metadata DB sẽ nghẽn; `day09_reset` + chạy lại cả DAG cũng không còn khả thi. Thay bằng DAG phân vùng theo ngày/xe, ingest hướng sự kiện (thông báo S3/Kafka khi xe upload xong), backfill theo partition.
3. **DuckDB một file + một tiến trình ghi Delta**: `lakehouse.duckdb` nằm trên một máy, dbt chạy full-refresh và test quét toàn bảng; `silver.frames` nhận hàng nghìn commit overwrite nhỏ/ngày → nhiều file nhỏ, xung đột commit, log Delta phình. Thay bằng engine phân tán (Spark/Trino) trên Delta/Iceberg, model dbt `incremental` theo partition ngày/xe, compaction (OPTIMIZE) định kỳ; MinIO một node cũng phải chuyển sang object store nhiều node/S3.

Ngoài ra: test `in_range`/`max_gap_ms` vẫn dùng được nhưng cần chạy theo partition mới, và `mart_frame_gallery` (một dòng/khung trong Postgres) nên chỉ giữ mẫu hoặc tổng hợp thay vì toàn bộ khung.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**"Tỷ lệ khung bị loại theo từng trip, so với tốc độ trung bình"**

```sql
select trip_id, drive_id, split, round(avg_speed_kmh::numeric, 1) as toc_do_tb_kmh, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / frames, 1) as ty_le_loai_pct
from marts.mart_trip_summary
order by ty_le_loai_pct desc;
```

Kết quả: tỷ lệ loại dao động từ 0% (4 trip, toàn drive_b trừ trip-07304) đến **56,7% ở `trip-04622`** (drive_a, 34/60 khung bị loại). 5 trip loại nhiều nhất đều thuộc **drive_a**. Gom theo tốc độ: trip < 30 km/h loại **29,3%**, 30–60 km/h **17,9%**, > 60 km/h **14,0%** — xe chậm thì khung gần trùng nhiều, khớp với Câu 5. Nhưng `trip-04622` chạy 67 km/h mà vẫn loại hơn một nửa, nên nguyên nhân không phải tốc độ mà là sự cố camera (đoạn đứng hình 6 s/khung đen/mờ của drive_a) — đây là trip cần mở ảnh trong MinIO để kiểm tra trước khi tin dữ liệu của nó. Câu hỏi này giúp phát hiện trip "có vấn đề" mà tổng số khung của cả dataset che mất. Lưu ý thêm: cả 4 trip test của mình (`trip-07774` 38,2% … `trip-01037` 0%) có tỷ lệ loại rất khác nhau, nên tập test nhỏ (154 khung) có thể không đại diện.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất:** chỉ đổi thứ tự trip bằng MSSV mà `dataset_version` khác hẳn, trong khi bấm Clear hai lần cho cùng một trip vẫn giữ nguyên `ecfe1509494e` — phiên bản theo nội dung chứ không theo lần chạy.
- **Bất ngờ thứ hai:** ngưỡng dHash đổi từ 2 lên 6 làm số khung "trùng" của drive A tăng từ 149 lên 363 (60% dataset) — một con số cấu hình nhỏ quyết định phần lớn dữ liệu.
- **Còn chưa rõ:** chọn ngưỡng 40 cho khung "tối" và 150 ms cho đồng bộ dựa trên căn cứ nào; nên hiệu chỉnh theo từng camera chứ không dùng một số chung.
- **Lỗi pipeline chưa bắt:** `in_range` chỉ chặn tốc độ ngoài 0..70 m/s; một giá trị sai nhưng vẫn "hợp lệ" (ví dụ nhảy 10 → 60 m/s trong 0,1 s, gia tốc vô lý) hoặc GPS nhảy vị trí vẫn lọt vào silver. Ngoài ra `telemetry_clock_offset_ms` là số khai báo tay — nếu khai sai (lệch 1,5 s thành 0), mọi check vẫn có thể PASS trừ check riêng cho drive B; pipeline nên tự ước lượng độ lệch (ví dụ so tốc độ với chuyển động quang học của video).
- **Thực tế Windows:** Git tự đổi file `.sh` sang CRLF và làm hỏng script trong container; phải thêm `.gitattributes` (`eol=lf`).
