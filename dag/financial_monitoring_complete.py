"""
Complete Financial Monitoring DAG
Monitors all aspects: feeds, revenue, transactions, freshness, patterns, reconciliation, SLA, quality
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
from monitoring.detectors.transaction_detector import TransactionDetector
from monitoring.detectors.freshness_detector import FreshnessDetector
from monitoring.detectors.pattern_detector import PatternDetector
from monitoring.detectors.reconciliation_detector import ReconciliationDetector
from monitoring.detectors.sla_detector import SLADetector
from monitoring.detectors.quality_detector import QualityDetector
from monitoring.alerts.alert_manager import AlertManager

# Configuration
PROJECT_ID = "beaming-force-480014-v3"
DATASET_ID = "financial_monitoring"

# Expected feeds
EXPECTED_FEEDS = [
    'FEED_001', 'FEED_002', 'FEED_003', 'FEED_004', 'FEED_005',
    'FEED_006', 'FEED_007', 'FEED_008', 'FEED_009', 'FEED_010',
    'FEED_011', 'FEED_012', 'FEED_013', 'FEED_014', 'FEED_015'
]

# Alert configuration
ALERT_CONFIG = {
    'slack_webhook': os.getenv('SLACK_WEBHOOK_URL', ''),
    'email': {
        'enabled': False,
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

# Task functions
def check_feeds(**kwargs):
    """Check for missing feeds."""
    detector = FeedDetector(PROJECT_ID, DATASET_ID)
    status = detector.check_feed_status(EXPECTED_FEEDS)
    kwargs['ti'].xcom_push(key='feed_status', value=status)
    return status

def check_revenue(**kwargs):
    """Check for revenue anomalies."""
    detector = RevenueDetector(PROJECT_ID, DATASET_ID)
    status = detector.check_revenue_anomaly()
    kwargs['ti'].xcom_push(key='revenue_status', value=status)
    return status

def check_transactions(**kwargs):
    """Check for transaction volume anomalies."""
    detector = TransactionDetector(PROJECT_ID, DATASET_ID)
    status = detector.check_transaction_volume(time_window_hours=1)
    kwargs['ti'].xcom_push(key='transaction_status', value=status)
    return status

def check_freshness(**kwargs):
    """Check data freshness."""
    detector = FreshnessDetector(PROJECT_ID, DATASET_ID)
    status = detector.check_data_freshness(max_age_minutes=30)
    kwargs['ti'].xcom_push(key='freshness_status', value=status)
    return status

def check_patterns(**kwargs):
    """Check for pattern breaks."""
    detector = PatternDetector(PROJECT_ID, DATASET_ID)
    status = detector.check_pattern_breaks()
    kwargs['ti'].xcom_push(key='pattern_status', value=status)
    return status

def check_reconciliation(**kwargs):
    """Check reconciliation between source and destination."""
    detector = ReconciliationDetector(PROJECT_ID, DATASET_ID)
    # Example: reconcile daily_revenue with itself (in real use, compare different tables)
    status = detector.check_reconciliation('daily_revenue', 'daily_revenue')
    kwargs['ti'].xcom_push(key='reconciliation_status', value=status)
    return status

def check_sla(**kwargs):
    """Predict SLA breaches."""
    detector = SLADetector(PROJECT_ID, DATASET_ID)
    # Example: 100k records, 4-hour SLA
    status = detector.predict_sla_breach(total_records=100000, sla_hours=4)
    kwargs['ti'].xcom_push(key='sla_status', value=status)
    return status

def check_quality(**kwargs):
    """Check for data quality degradation."""
    detector = QualityDetector(PROJECT_ID, DATASET_ID)
    status = detector.check_quality_degradation()
    kwargs['ti'].xcom_push(key='quality_status', value=status)
    return status

def send_all_alerts(**kwargs):
    """Send alerts for all detected issues."""
    ti = kwargs['ti']
    alert_manager = AlertManager(ALERT_CONFIG)
    
    # Get all statuses
    feed_status = ti.xcom_pull(key='feed_status', task_ids='check_feeds')
    revenue_status = ti.xcom_pull(key='revenue_status', task_ids='check_revenue')
    transaction_status = ti.xcom_pull(key='transaction_status', task_ids='check_transactions')
    freshness_status = ti.xcom_pull(key='freshness_status', task_ids='check_freshness')
    pattern_status = ti.xcom_pull(key='pattern_status', task_ids='check_patterns')
    reconciliation_status = ti.xcom_pull(key='reconciliation_status', task_ids='check_reconciliation')
    sla_status = ti.xcom_pull(key='sla_status', task_ids='check_sla')
    quality_status = ti.xcom_pull(key='quality_status', task_ids='check_quality')
    
    # Send alerts
    if feed_status and feed_status.get('missing_count', 0) > 0:
        alert_manager.create_feed_alert(feed_status)
    
    if revenue_status and revenue_status.get('is_anomaly'):
        alert_manager.create_revenue_alert(revenue_status)
    
    # Add custom alerts for new detectors
    if transaction_status and transaction_status.get('is_anomaly'):
        alert_manager.send_alert('TRANSACTION', transaction_status['severity'], 
                                'Transaction Volume Anomaly', transaction_status,
                                transaction_status.get('ai_analysis', {}).get('recommended_actions'))
    
    if freshness_status and freshness_status.get('is_stale'):
        alert_manager.send_alert('FRESHNESS', freshness_status['severity'],
                                'Stale Data Detected', freshness_status,
                                freshness_status.get('ai_analysis', {}).get('recommended_actions'))
    
    if pattern_status and pattern_status.get('has_breaks'):
        alert_manager.send_alert('PATTERN', pattern_status['severity'],
                                'Pattern Break Detected', pattern_status,
                                pattern_status.get('ai_analysis', {}).get('recommended_actions'))
    
    if reconciliation_status and not reconciliation_status.get('is_reconciled'):
        alert_manager.send_alert('RECONCILIATION', reconciliation_status['severity'],
                                'Reconciliation Failure', reconciliation_status,
                                reconciliation_status.get('ai_analysis', {}).get('recommended_actions'))
    
    if sla_status and sla_status.get('will_breach_sla'):
        alert_manager.send_alert('SLA', sla_status['severity'],
                                'SLA Breach Predicted', sla_status,
                                sla_status.get('ai_analysis', {}).get('recommended_actions'))
    
    if quality_status and quality_status.get('has_degradation'):
        alert_manager.send_alert('QUALITY', quality_status['severity'],
                                'Data Quality Degradation', quality_status,
                                quality_status.get('ai_analysis', {}).get('recommended_actions'))

# Define DAG
with DAG(
    'financial_monitoring_complete',
    default_args=default_args,
    description='Complete AI-powered financial monitoring with 8 detectors',
    schedule_interval='0 17 * * *',  # Daily at 5 PM
    catchup=False,
    tags=['monitoring', 'financial', 'ai', 'complete']
) as dag:
    
    # All monitoring tasks
    task_feeds = PythonOperator(task_id='check_feeds', python_callable=check_feeds, provide_context=True)
    task_revenue = PythonOperator(task_id='check_revenue', python_callable=check_revenue, provide_context=True)
    task_transactions = PythonOperator(task_id='check_transactions', python_callable=check_transactions, provide_context=True)
    task_freshness = PythonOperator(task_id='check_freshness', python_callable=check_freshness, provide_context=True)
    task_patterns = PythonOperator(task_id='check_patterns', python_callable=check_patterns, provide_context=True)
    task_reconciliation = PythonOperator(task_id='check_reconciliation', python_callable=check_reconciliation, provide_context=True)
    task_sla = PythonOperator(task_id='check_sla', python_callable=check_sla, provide_context=True)
    task_quality = PythonOperator(task_id='check_quality', python_callable=check_quality, provide_context=True)
    
    # Alert task
    task_alerts = PythonOperator(task_id='send_alerts', python_callable=send_all_alerts, provide_context=True)
    
    # Run all checks in parallel, then send alerts
    [task_feeds, task_revenue, task_transactions, task_freshness, 
     task_patterns, task_reconciliation, task_sla, task_quality] >> task_alerts
