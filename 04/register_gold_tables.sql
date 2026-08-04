-- Gold 레이어(s3a://test-bucket/gold/*)를 Hive Metastore에 외부 테이블로 등록.
-- 컬럼 정의는 02/실습코드/01/03.py의 집계 로직(및 .describe()의 string 출력 특성) 기준.
-- 04/upload_gold_tables.md 나 03/upload_to_bucket.py처럼, Gold 로직이 바뀌면 이 파일도 같이 고치고 재실행해야 함.

CREATE SCHEMA IF NOT EXISTS minio_lake.gold WITH (location = 's3a://test-bucket/gold/');

CREATE TABLE IF NOT EXISTS minio_lake.gold.event_funnel (
  event_type VARCHAR,
  count BIGINT
) WITH (external_location = 's3a://test-bucket/gold/event_funnel/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS minio_lake.gold.category_revenue (
  category_code VARCHAR,
  revenue DOUBLE,
  purchase_count BIGINT
) WITH (external_location = 's3a://test-bucket/gold/category_revenue/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS minio_lake.gold.brand_revenue (
  brand VARCHAR,
  revenue DOUBLE,
  purchase_count BIGINT
) WITH (external_location = 's3a://test-bucket/gold/brand_revenue/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS minio_lake.gold.daily_events (
  event_date DATE,
  event_type VARCHAR,
  count BIGINT
) WITH (external_location = 's3a://test-bucket/gold/daily_events/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS minio_lake.gold.daily_purchase (
  event_date DATE,
  purchase_count BIGINT,
  daily_revenue DOUBLE
) WITH (external_location = 's3a://test-bucket/gold/daily_purchase/', format = 'PARQUET');

-- Spark .describe()는 결과 컬럼이 전부 문자열(string)로 나옴 (summary 행: count/mean/stddev/min/max)
CREATE TABLE IF NOT EXISTS minio_lake.gold.price_stats (
  summary VARCHAR,
  price VARCHAR
) WITH (external_location = 's3a://test-bucket/gold/price_stats/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS minio_lake.gold.price_percentiles (
  p25 DOUBLE,
  median DOUBLE,
  p75 DOUBLE,
  p95 DOUBLE
) WITH (external_location = 's3a://test-bucket/gold/price_percentiles/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS minio_lake.gold.purchase_price_stats (
  summary VARCHAR,
  price VARCHAR
) WITH (external_location = 's3a://test-bucket/gold/purchase_price_stats/', format = 'PARQUET');
