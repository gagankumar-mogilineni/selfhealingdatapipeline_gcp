# AI-Powered Financial Monitoring System

## Overview

Complete monitoring system that detects missing feeds, revenue anomalies, and other financial data issues using AI-powered analysis.

## Features

### 1. Missing Feed Detection
- Monitors expected feed arrivals
- Detects missing feeds in real-time
- AI analysis of historical patterns
- Automated alerting

### 2. Revenue Anomaly Detection
- Statistical baseline calculation (30-day moving average)
- Z-score based anomaly detection
- AI-powered root cause analysis
- Revenue forecasting

### 3. Intelligent Alerting
- Multi-channel notifications (Slack, Email)
- Severity-based routing
- Alert deduplication
- Actionable recommendations

## Project Structure

```
monitoring/
├── __init__.py
├── detectors/
│   ├── __init__.py
│   ├── feed_detector.py       # Missing feed detection
│   └── revenue_detector.py    # Revenue anomaly detection
├── alerts/
│   ├── __init__.py
│   └── alert_manager.py       # Alert routing and notifications
└── setup_bigquery.sql         # Database schema

dags/
└── financial_monitoring_dag.py  # Airflow orchestration
```

## Setup Instructions

### Step 1: Create BigQuery Tables

```bash
# Run the setup SQL
bq query --use_legacy_sql=false < monitoring/setup_bigquery.sql
```

Or run in BigQuery Console:
1. Open BigQuery in GCP Console
2. Copy contents of `monitoring/setup_bigquery.sql`
3. Run each CREATE TABLE statement
4. Run sample data insertion (optional)

### Step 2: Configure Alerts

Edit `dags/financial_monitoring_dag.py` and update:

```python
# Slack webhook (recommended)
ALERT_CONFIG = {
    'slack_webhook': 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'
}
```

**To get Slack webhook:**
1. Go to https://api.slack.com/apps
2. Create new app → Incoming Webhooks
3. Add to workspace → Copy webhook URL

**For email alerts:**
```python
ALERT_CONFIG = {
    'email': {
        'enabled': True,
        'from_address': 'alerts@yourcompany.com',
        'to_addresses': ['finance-team@yourcompany.com'],
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'username': 'your-email@gmail.com',
        'password': 'your-app-password'  # Use app password, not regular password
    }
}
```

### Step 3: Upload to Cloud Composer

```bash
# Upload monitoring module
gsutil -m cp -r monitoring gs://YOUR-COMPOSER-BUCKET/dags/

# Upload DAG
gsutil cp dags/financial_monitoring_dag.py gs://YOUR-COMPOSER-BUCKET/dags/
```

### Step 4: Customize Expected Feeds

Edit `dags/financial_monitoring_dag.py`:

```python
EXPECTED_FEEDS = [
    'FEED_001', 'FEED_002', 'FEED_003',  # Add your actual feed IDs
    # ... up to 15 feeds or more
]
```

### Step 5: Test the System

```bash
# Trigger DAG manually
gcloud composer environments run selfhealingpipeline \
    --location us-central1 \
    dags trigger financial_monitoring
```

## Usage Examples

### Check Feed Status Manually

```python
from monitoring.detectors.feed_detector import FeedDetector

detector = FeedDetector('your-project-id')
status = detector.check_feed_status([
    'FEED_001', 'FEED_002', 'FEED_003'
])

print(f"Missing: {status['missing_count']}")
print(f"AI Analysis: {status['ai_analysis']}")
```

### Check Revenue Anomalies

```python
from monitoring.detectors.revenue_detector import RevenueDetector

detector = RevenueDetector('your-project-id')
status = detector.check_revenue_anomaly()

if status['is_anomaly']:
    print(f"Anomaly detected: {status['deviation_percent']:.1f}% deviation")
    print(f"AI Analysis: {status['ai_analysis']['root_cause']}")
```

### Send Custom Alert

```python
from monitoring.alerts.alert_manager import AlertManager

alert_manager = AlertManager(config)
alert_manager.send_alert(
    alert_type='CUSTOM',
    severity='HIGH',
    title='Custom Financial Alert',
    details={'metric': 'value'},
    recommendations=['Action 1', 'Action 2']
)
```

## Alert Examples

### Missing Feed Alert

```
⚠️ **HIGH: Missing Feeds Detected: 3 feeds**

**Type:** FEED
**Timestamp:** 2025-12-05 17:00:00

**Details:**
- Expected Feeds: 15
- Arrived Feeds: 12
- Missing Feeds: 3
- Missing IDs: FEED_007, FEED_012, FEED_015

**AI Analysis:**
Root Cause: These feeds typically arrive from DataProvider_X between 2-3 PM. 
Last successful arrival was 2 days ago. Upstream system may be experiencing issues.

**Recommended Actions:**
1. Contact DataProvider_X immediately
2. Check network connectivity to upstream systems
3. Use backup data source if available
4. Estimate impact on downstream processing
```

### Revenue Anomaly Alert

```
🚨 **CRITICAL: Revenue Drop: 28.7% deviation**

**Type:** REVENUE
**Timestamp:** 2025-12-05 17:00:00

**Details:**
- Current Revenue: $7,200,000.00
- Expected Revenue: $10,100,000.00
- Deviation: -28.7%
- Dollar Impact: -$2,900,000.00
- Z-Score: -3.45

**AI Analysis:**
Root Cause: Missing 3 major feeds (FEED_007, FEED_012, FEED_015) which typically 
contribute $3.1M daily revenue. Transaction volume is normal, but revenue is down 
due to incomplete data.

**Recommended Actions:**
1. Resolve missing feed issues immediately
2. Notify finance team of potential reporting gap
3. Estimate impact on monthly revenue targets
4. Consider using historical data to fill gaps
```

## Monitoring Schedule

**Default Schedule:** Daily at 5 PM (17:00)

To change schedule, edit `financial_monitoring_dag.py`:

```python
schedule_interval='0 17 * * *'  # Daily at 5 PM
# OR
schedule_interval='0 */4 * * *'  # Every 4 hours
# OR
schedule_interval='@hourly'  # Every hour
```

## Advanced Features

### 1. Revenue Forecasting

```python
detector = RevenueDetector('your-project-id')
forecast = detector.forecast_revenue(days_ahead=7)

print(f"7-day forecast: ${forecast['forecast_total']:,.2f}")
```

### 2. Feed Trend Analysis

```python
detector = FeedDetector('your-project-id')
trends = detector.get_feed_trends(days=30)

for trend in trends:
    print(f"{trend['date']}: {trend['feed_count']} feeds")
```

### 3. Custom Baselines

Modify `revenue_detector.py` to use different baseline periods:

```python
baseline = self._calculate_baseline(current_date, lookback_days=60)  # 60-day baseline
```

## Troubleshooting

### DAG Not Appearing in Airflow

1. Check file upload:
   ```bash
   gsutil ls gs://YOUR-COMPOSER-BUCKET/dags/financial_monitoring_dag.py
   gsutil ls gs://YOUR-COMPOSER-BUCKET/dags/monitoring/
   ```

2. Check Airflow logs for import errors

3. Verify all `__init__.py` files exist

### No Alerts Being Sent

1. Check Slack webhook URL is correct
2. Verify alert severity thresholds
3. Check for duplicate alert suppression (1-hour window)
4. Review Airflow task logs

### BigQuery Errors

1. Verify dataset exists:
   ```bash
   bq ls beaming-force-480014-v3
   ```

2. Check table schemas:
   ```bash
   bq show beaming-force-480014-v3:financial_monitoring.feed_arrivals
   ```

3. Verify service account permissions

## Cost Estimate

**Monthly Costs:**
- BigQuery Storage: ~$50 (10 GB)
- BigQuery Queries: ~$50 (100 GB processed)
- Gemini AI: ~$30 (daily analysis)
- Cloud Composer: ~$300 (existing)

**Total Additional: ~$130/month**

**ROI:**
- Catch 1 missing feed issue: Save $3M revenue
- Detect 1 pricing error: Save $500K
- Prevent 1 data quality issue: Save 10 hours manual work

## Next Steps

1. ✅ Set up BigQuery tables
2. ✅ Configure Slack webhook
3. ✅ Upload to Composer
4. ✅ Customize expected feeds
5. ✅ Test with sample data
6. ✅ Monitor for 1 week
7. ✅ Tune thresholds
8. ✅ Add custom metrics

## Support

For issues or questions:
1. Check Airflow logs in Composer UI
2. Review BigQuery query results
3. Test detectors individually
4. Verify alert configuration

## Extending the System

### Add New Detector

1. Create new file in `monitoring/detectors/`
2. Implement detection logic
3. Add to DAG
4. Create alert formatter in `alert_manager.py`

### Add New Alert Channel

1. Add method to `alert_manager.py` (e.g., `_send_pagerduty`)
2. Update `_send_to_all_channels` for critical alerts
3. Add configuration to `ALERT_CONFIG`

### Custom Metrics

1. Create BigQuery table for metric
2. Create detector class
3. Add to monitoring DAG
4. Configure alerts

## License

Internal use only - Your Company Name
