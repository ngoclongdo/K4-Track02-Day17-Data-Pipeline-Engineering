# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Đỗ Nguyễn Ngọc Long / 2A202602390
**Repo:** `K4-Track02-Day17-DoNguyenNgocLong-2A202602390-DataPipelineEngineering`
**Commit bài nộp:** `efd463d3f0e09124a9315e1c37dced3aa8d2afc2`
**AI đã dùng và phạm vi hỗ trợ:** Gemini CLI (hỗ trợ phân tích kiến trúc, hướng dẫn triển khai CDC/Lookback/dbt và viết test)
**Nguồn tham khảo khác:** Tài liệu Debezium CDC, DuckDB documentation, dbt-duckdb adapter docs.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Bảng `silver_tickets` chứa nhiều dòng cho cùng 1 `ticket_id` (trùng lặp do redelivery/update). | Features sự kiện ngày 08-12 thiếu sự kiện đến muộn ngày 08-15 (`LOOKBACK_DAYS = 1` mặc định quá ngắn). | Ticket T-97 đã bị xoá ở nguồn nhưng vẫn xuất hiện trong snapshot Gold và RAG chunks. |
| **Nguyên nhân gốc** | Thiếu cơ chế dedup/idempotent upsert theo thứ tự LSN (`_lsn`) trong `pipeline/silver.py`. | Khoảng cách thời gian P99 lateness là 3 ngày, nhưng window lookback chỉ quét trong ngày (`LOOKBACK_DAYS = 1`). | Debezium CDC delete có `after = null`, khiến `ticket_id` bị mất khi parse và tombstone bị bỏ sót. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: Dùng `MERGE INTO` với `_lsn >= t._lsn` để update trạng thái mới nhất dựa trên LSN. | `pipeline/config.py`: Đặt `LOOKBACK_DAYS = 3` để phủ P99 lateness (3.00 ngày). | `pipeline/staging.py`: Dùng `COALESCE(j->'value'->'after'->>'ticket_id', j->'key'->>'ticket_id')` để giữ `ticket_id` khi delete. |
| **Khái niệm trên slide** | Idempotency & Upsert pattern trong Lakehouse layers. | Watermarking & Out-of-order event processing with lookback window. | CDC Debezium protocol (`op = "d"`), Tombstone records & Soft delete propagation. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: Vì `silver_tickets` là bảng dimension/state entity cần upsert tích lũy bất biến theo LSN, trong khi `gold_feature_daily` là bảng aggregate theo ngày có thể compute lại hoàn toàn cho mỗi batch/partition.
- Tombstone thay vì xoá hẳn hàng trong Silver: Vì giúp duy trì audit trail lịch sử CDC, ghi nhận trạng thái xoá (`is_deleted = true`) và chống hồi sinh bản ghi khi replay log cũ.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến (immutability) của dữ liệu huấn luyện ML theo mốc thời gian point-in-time, tránh data leakage.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Vì dữ liệu ở quy mô vừa (vài chục MB/GB), DuckDB cung cấp hiệu năng cực cao qua vectorization trên single node với cú pháp SQL tiêu chuẩn mà không cần overhead cluster phức tạp của Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   *Trả lời:* Trong kiến trúc lakehouse, khi có yêu cầu xóa tuân thủ GDPR/CCPA (Right to be Forgotten), thay vì ghi đè các snapshot cũ (vi phạm tính bất biến), hệ thống áp dụng cơ chế **Cryptographic Erasure** (mã hóa dữ liệu nhạy cảm bằng key độc lập và hủy key khi xóa) hoặc định kỳ chạy **Tombstone Masking / Compaction Job** tạo phiên bản snapshot mới (với retention policy) đồng thời invalidate các artifact LLM/RAG cũ.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   *Trả lời:* Để xử lý PII dạng tên riêng hoặc thực thể nhạy cảm không bắt bằng regex đơn thuần, ta cần tích hợp mô hình **NER (Named Entity Recognition)** hoặc LLM-based redaction (như Presidio hoặc custom masking macro) tại **tầng Staging / Silver** trước khi dữ liệu được đẩy vào Gold hay embedding index. Mức độ tuân thủ được đo lường bằng automated PII contract tests (kiểm tra tỷ lệ xuất hiện từ khóa PII/entropy check trên bảng Silver & Gold).

## 5. Output (dán nguyên văn)

```powershell
$ python -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ python -m pytest
============================= test session starts =============================
platform win32 -- Python 3.11.4, pytest-8.x.x, pluggy-1.x.x
rootdir: D:\VinUni\day17\K4-Track02-Day17-Data-Pipeline-Engineering
configfile: pytest.ini
collected 34 items

tests/test_contracts.py ..................................             [100%]
34 passed in 3.22s

$ python -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ python main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ dbt build --profiles-dir dbt_project --event-time-start 2026-08-10 --event-time-end 2026-08-17
06:56:20  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 4.01 seconds (4.01s).
06:56:20  Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ python -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
