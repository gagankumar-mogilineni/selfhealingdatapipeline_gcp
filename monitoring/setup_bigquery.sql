-- BigQuery Schema Setup for Financial Monitoring
-- Run these queries to create the required tables

-- 1. Create dataset
CREATE SCHEMA IF NOT EXISTS `beaming-force-480014-v3.financial_monitoring`
OPTIONS(
  location="us-central1",
  description="Financial monitoring and anomaly detection"
);

-- 2. Feed arrivals table
CREATE TABLE IF NOT EXISTS `beaming-force-480014-v3.financial_monitoring.feed_arrivals` (
  feed_id STRING NOT NULL,
  arrival_time TIMESTAMP NOT NULL,
  record_count INT64,
  file_size_bytes INT64,
  source_system STRING,
  status STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(arrival_time)
OPTIONS(
  description="Tracks feed arrivals for monitoring",
  partition_expiration_days=90
);

-- 3. Daily revenue table
CREATE TABLE IF NOT EXISTS `beaming-force-480014-v3.financial_monitoring.daily_revenue` (
  transaction_id STRING NOT NULL,
  transaction_date TIMESTAMP NOT NULL,
  revenue FLOAT64 NOT NULL,
  product_category STRING,
  region STRING,
  customer_id STRING,
  transaction_type STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(transaction_date)
OPTIONS(
  description="Daily revenue transactions for anomaly detection",
  partition_expiration_days=365
);

-- 4. Monitoring alerts table (to track alert history)
CREATE TABLE IF NOT EXISTS `beaming-force-480014-v3.financial_monitoring.monitoring_alerts` (
  alert_id STRING NOT NULL,
  alert_type STRING NOT NULL,
  severity STRING NOT NULL,
  title STRING NOT NULL,
  details JSON,
  recommendations JSON,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
OPTIONS(
  description="Historical record of all monitoring alerts",
  partition_expiration_days=180
);

-- 5. Baseline metrics table (to store calculated baselines)
CREATE TABLE IF NOT EXISTS `beaming-force-480014-v3.financial_monitoring.baseline_metrics` (
  metric_name STRING NOT NULL,
  metric_date DATE NOT NULL,
  baseline_value FLOAT64,
  std_dev FLOAT64,
  min_value FLOAT64,
  max_value FLOAT64,
  sample_size INT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY metric_date
OPTIONS(
  description="Calculated baseline metrics for anomaly detection",
  partition_expiration_days=90
);

-- ============================================================
-- SAMPLE DATA INSERTION
-- ============================================================

-- Insert sample feed arrivals (last 30 days)
INSERT INTO `beaming-force-480014-v3.financial_monitoring.feed_arrivals` 
(feed_id, arrival_time, record_count, source_system, status)
SELECT 
  CONCAT('FEED_', LPAD(CAST(feed_num AS STRING), 3, '0')) as feed_id,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL day_offset DAY) as arrival_time,
  CAST(RAND() * 10000 + 5000 AS INT64) as record_count,
  'DataProvider_X' as source_system,
  'SUCCESS' as status
FROM 
  UNNEST(GENERATE_ARRAY(1, 15)) as feed_num,
  UNNEST(GENERATE_ARRAY(0, 29)) as day_offset
WHERE 
  -- Simulate some missing feeds
  NOT (feed_num IN (7, 12, 15) AND day_offset < 2);

-- Insert sample revenue data (last 60 days)
INSERT INTO `beaming-force-480014-v3.financial_monitoring.daily_revenue`
(transaction_id, transaction_date, revenue, product_category, region, customer_id, transaction_type)
SELECT 
  GENERATE_UUID() as transaction_id,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL day_offset DAY) as transaction_date,
  -- Normal revenue around $10M with some variation
  CASE 
    WHEN day_offset = 0 THEN RAND() * 2000000 + 7000000  -- Today: anomaly (lower)
    WHEN day_offset = 1 THEN RAND() * 2000000 + 12000000  -- Yesterday: anomaly (higher)
    ELSE RAND() * 2000000 + 9000000  -- Normal: ~$10M
  END as revenue,
  CASE CAST(RAND() * 5 AS INT64)
    WHEN 0 THEN 'Electronics'
    WHEN 1 THEN 'Clothing'
    WHEN 2 THEN 'Food'
    WHEN 3 THEN 'Books'
    ELSE 'Other'
  END as product_category,
  CASE CAST(RAND() * 4 AS INT64)
    WHEN 0 THEN 'NORTH_AMERICA'
    WHEN 1 THEN 'EUROPE'
    WHEN 2 THEN 'ASIA'
    ELSE 'OTHER'
  END as region,
  CONCAT('CUST_', CAST(CAST(RAND() * 10000 AS INT64) AS STRING)) as customer_id,
  'SALE' as transaction_type
FROM 
  UNNEST(GENERATE_ARRAY(0, 59)) as day_offset,
  UNNEST(GENERATE_ARRAY(1, CAST(RAND() * 100 + 50 AS INT64))) as transaction_num;

-- ============================================================
-- USEFUL QUERIES FOR MONITORING
-- ============================================================

-- Check today's feed status
SELECT 
  feed_id,
  MAX(arrival_time) as last_arrival,
  COUNT(*) as arrival_count
FROM `beaming-force-480014-v3.financial_monitoring.feed_arrivals`
WHERE DATE(arrival_time) = CURRENT_DATE()
GROUP BY feed_id
ORDER BY feed_id;

-- Check today's revenue vs 30-day average
WITH today_revenue AS (
  SELECT SUM(revenue) as today_total
  FROM `beaming-force-480014-v3.financial_monitoring.daily_revenue`
  WHERE DATE(transaction_date) = CURRENT_DATE()
),
baseline AS (
  SELECT 
    AVG(daily_revenue) as avg_revenue,
    STDDEV(daily_revenue) as std_dev
  FROM (
    SELECT 
      DATE(transaction_date) as date,
      SUM(revenue) as daily_revenue
    FROM `beaming-force-480014-v3.financial_monitoring.daily_revenue`
    WHERE DATE(transaction_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
      AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY date
  )
)
SELECT 
  t.today_total,
  b.avg_revenue as baseline_avg,
  b.std_dev,
  ((t.today_total - b.avg_revenue) / b.avg_revenue * 100) as deviation_pct,
  ((t.today_total - b.avg_revenue) / b.std_dev) as z_score
FROM today_revenue t, baseline b;

-- Revenue trend (last 30 days)
SELECT 
  DATE(transaction_date) as date,
  SUM(revenue) as daily_revenue,
  COUNT(*) as transaction_count,
  AVG(revenue) as avg_transaction_value
FROM `beaming-force-480014-v3.financial_monitoring.daily_revenue`
WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY date
ORDER BY date DESC;

-- Feed arrival patterns
SELECT 
  feed_id,
  COUNT(DISTINCT DATE(arrival_time)) as days_arrived,
  COUNT(*) as total_arrivals,
  MIN(arrival_time) as first_seen,
  MAX(arrival_time) as last_seen,
  AVG(EXTRACT(HOUR FROM arrival_time)) as avg_arrival_hour
FROM `beaming-force-480014-v3.financial_monitoring.feed_arrivals`
WHERE arrival_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY feed_id
ORDER BY feed_id;
