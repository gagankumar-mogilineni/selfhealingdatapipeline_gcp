"""
Financial Monitoring DAG
Monitors feeds and revenue, detects anomalies, and sends alerts
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Add monitoring module to path
sys.path.append(os.path.dirname(__file__))

from monitoring.detectors.feed_detector import FeedDetector
from monitoring.detectors.revenue_detector import RevenueDetector
from monitoring.alerts.alert_manager import AlertManager

# Configuration
PROJECT_ID = "beaming-force-480014-v3"
DATASET_ID = "financial_monitoring"

# Expected feeds (customize this list)
EXPECTED_FEEDS = [
    'FEED_001', 'FEED_002', 'FEED_003', 'FEED_004', 'FEED_005',
    'FEED_006', 'FEED_007', 'FEED_008', 'FEED_009', 'FEED_010',
    'FEED_011', 'FEED_012', 'FEED_013', 'FEED_014', 'FEED_015'
]

# Alert configuration
ALERT_CONFIG = {
    'slack_webhook': os.getenv('SLACK_WEBHOOK_URL', ''),  # Set in Composer environment variables
    'email': {
        'enabled': False,  # Set to True when configured
        'from_address': 'alerts@yourcompany.com',
        'to_addresses': ['finance-team@yourcompany.com'],
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'username': os.getenv('EMAIL_USERNAME', ''),
        'password': os.getenv('EMAIL_PASSWORD', '')
    }
}

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def check_feeds(**kwargs):
    """Check for missing feeds."""
    print("\n" + "="*80)
    print("TASK: Feed Status Check")
    print("="*80)
    
    detector = FeedDetector(PROJECT_ID, DATASET_ID)
    status = detector.check_feed_status(EXPECTED_FEEDS)
    
    # Push to XCom for alert task
    kwargs['ti'].xcom_push(key='feed_status', value=status)
    
    return status

def check_revenue(**kwargs):
    """Check for revenue anomalies."""
    print("\n" + "="*80)
    print("TASK: Revenue Anomaly Check")
    print("="*80)
    
    detector = RevenueDetector(PROJECT_ID, DATASET_ID)
    status = detector.check_revenue_anomaly()
    
    # Push to XCom for alert task
    kwargs['ti'].xcom_push(key='revenue_status', value=status)
    
    return status

def send_alerts(**kwargs):
    """Send alerts based on detection results."""
    print("\n" + "="*80)
    print("TASK: Alert Processing")
    print("="*80)
    
    ti = kwargs['ti']
    
    # Get results from previous tasks
    feed_status = ti.xcom_pull(key='feed_status', task_ids='check_feed_status')
    revenue_status = ti.xcom_pull(key='revenue_status', task_ids='check_revenue_anomaly')
    
    # Initialize alert manager
    alert_manager = AlertManager(ALERT_CONFIG)
    
    # Send feed alerts
    if feed_status and feed_status.get('missing_count', 0) > 0:
        alert_manager.create_feed_alert(feed_status)
    else:
        print("✓ No feed alerts needed")
    
    # Send revenue alerts
    if revenue_status and revenue_status.get('is_anomaly'):
        alert_manager.create_revenue_alert(revenue_status)
    else:
        print("✓ No revenue alerts needed")
    
    print("\n" + "="*80)
    print("Alert processing complete")
    print("="*80 + "\n")

def generate_daily_report(**kwargs):
    """Generate daily monitoring summary."""
    print("\n" + "="*80)
    print("TASK: Daily Report Generation")
    print("="*80)
    
    ti = kwargs['ti']
    
    feed_status = ti.xcom_pull(key='feed_status', task_ids='check_feed_status')
    revenue_status = ti.xcom_pull(key='revenue_status', task_ids='check_revenue_anomaly')
    
    report = f"""
DAILY FINANCIAL MONITORING REPORT
Date: {datetime.now().strftime('%Y-%m-%d')}
{'='*80}

FEED STATUS:
- Expected: {feed_status.get('expected_count', 'N/A')}
- Arrived: {feed_status.get('arrived_count', 'N/A')}
- Missing: {feed_status.get('missing_count', 0)}
- Status: {'✓ OK' if feed_status.get('missing_count', 0) == 0 else '⚠ ISSUES'}

REVENUE STATUS:
- Current: ${revenue_status.get('current_revenue', 0):,.2f}
- Expected: ${revenue_status.get('baseline_avg', 0):,.2f}
- Deviation: {revenue_status.get('deviation_percent', 0):+.1f}%
- Status: {'✓ OK' if not revenue_status.get('is_anomaly') else '⚠ ANOMALY'}

{'='*80}
"""
    
    print(report)
    
    # Could save to BigQuery, send via email, etc.
    return report

# Define DAG
with DAG(
    'financial_monitoring',
    default_args=default_args,
    description='AI-powered financial data monitoring with anomaly detection',
    schedule_interval='0 17 * * *',  # Run daily at 5 PM
    catchup=False,
    tags=['monitoring', 'financial', 'ai']
) as dag:
    
    # Task 1: Check feed status
    feed_check = PythonOperator(
        task_id='check_feed_status',
        python_callable=check_feeds,
        provide_context=True
    )
    
    # Task 2: Check revenue anomalies
    revenue_check = PythonOperator(
        task_id='check_revenue_anomaly',
        python_callable=check_revenue,
        provide_context=True
    )
    
    # Task 3: Send alerts
    alert_task = PythonOperator(
        task_id='send_alerts',
        python_callable=send_alerts,
        provide_context=True
    )
    
    # Task 4: Generate daily report
    report_task = PythonOperator(
        task_id='generate_daily_report',
        python_callable=generate_daily_report,
        provide_context=True
    )
    
    # Define task dependencies
    # Run feed and revenue checks in parallel
    [feed_check, revenue_check] >> alert_task >> report_task
