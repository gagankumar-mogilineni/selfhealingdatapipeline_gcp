"""
Transaction Volume Anomaly Detector
Monitors transaction counts and detects unusual spikes or drops
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import google.generativeai as genai
from google.auth import default
from google.auth.transport.requests import Request
import os
import json
import statistics

class TransactionDetector:
    def __init__(self, project_id, dataset_id='financial_monitoring'):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def check_transaction_volume(self, time_window_hours=1):
        """
        Check if transaction volume is anomalous.
        
        Args:
            time_window_hours: Time window to analyze (default: 1 hour)
        
        Returns:
            dict: Anomaly status with AI analysis
        """
        print(f"\n{'='*60}")
        print(f"TRANSACTION VOLUME CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*60}")
        
        # Get current hour's transaction count
        current_count = self._get_recent_transaction_count(time_window_hours)
        
        # Get baseline (same hour for last 30 days)
        baseline = self._calculate_hourly_baseline(time_window_hours)
        
        if current_count is None or baseline is None:
            return {'error': 'Insufficient data for analysis'}
        
        # Calculate deviation
        deviation_pct = ((current_count - baseline['avg']) / baseline['avg']) * 100 if baseline['avg'] > 0 else 0
        z_score = (current_count - baseline['avg']) / baseline['std_dev'] if baseline['std_dev'] > 0 else 0
        
        # Determine if anomaly (2.5 std deviations)
        is_anomaly = abs(z_score) > 2.5
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'time_window_hours': time_window_hours,
            'current_count': current_count,
            'baseline_avg': baseline['avg'],
            'baseline_std_dev': baseline['std_dev'],
            'deviation_amount': current_count - baseline['avg'],
            'deviation_percent': deviation_pct,
            'z_score': z_score,
            'is_anomaly': is_anomaly
        }
        
        print(f"Current Transactions: {current_count:,}")
        print(f"Expected (baseline): {baseline['avg']:,.0f}")
        print(f"Deviation: {deviation_pct:+.1f}% ({status['deviation_amount']:+,.0f} transactions)")
        print(f"Z-Score: {z_score:.2f}")
        
        if is_anomaly:
            print(f"\n🚨 ANOMALY DETECTED!")
            
            # Get transaction breakdown
            breakdown = self._get_transaction_breakdown(time_window_hours)
            status['breakdown'] = breakdown
            
            # AI Analysis
            ai_analysis = self._analyze_with_ai(status)
            status['ai_analysis'] = ai_analysis
            status['severity'] = self._calculate_severity(abs(deviation_pct))
        else:
            print("\n✓ Transaction volume within normal range")
            status['severity'] = 'NONE'
        
        return status
    
    def _get_recent_transaction_count(self, hours):
        """Get transaction count for recent time window."""
        query = f"""
        SELECT COUNT(*) as transaction_count
        FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
        WHERE transaction_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        """
        
        try:
            results = self.client.query(query).result()
            row = next(results, None)
            return int(row.transaction_count) if row else None
        except Exception as e:
            print(f"Error getting transaction count: {e}")
            return None
    
    def _calculate_hourly_baseline(self, hours):
        """Calculate baseline for same hour over last 30 days."""
        current_hour = datetime.now().hour
        
        query = f"""
        WITH hourly_counts AS (
          SELECT 
            DATE(transaction_date) as date,
            EXTRACT(HOUR FROM transaction_date) as hour,
            COUNT(*) as hourly_count
          FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
          WHERE transaction_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
          AND transaction_date < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
          GROUP BY date, hour
        )
        SELECT 
          AVG(hourly_count) as avg_count,
          STDDEV(hourly_count) as std_dev,
          MIN(hourly_count) as min_count,
          MAX(hourly_count) as max_count,
          COUNT(*) as sample_size
        FROM hourly_counts
        WHERE hour = {current_hour}
        """
        
        try:
            results = self.client.query(query).result()
            row = next(results, None)
            
            if row and row.sample_size >= 7:
                return {
                    'avg': float(row.avg_count),
                    'std_dev': float(row.std_dev) if row.std_dev else 0,
                    'min': int(row.min_count),
                    'max': int(row.max_count),
                    'sample_size': int(row.sample_size)
                }
            return None
        except Exception as e:
            print(f"Error calculating baseline: {e}")
            return None
    
    def _get_transaction_breakdown(self, hours):
        """Get breakdown of recent transactions."""
        query = f"""
        SELECT 
          region,
          product_category,
          COUNT(*) as count,
          SUM(revenue) as total_revenue
        FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
        WHERE transaction_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        GROUP BY region, product_category
        ORDER BY count DESC
        LIMIT 10
        """
        
        try:
            results = self.client.query(query).result()
            breakdown = []
            for row in results:
                breakdown.append({
                    'region': row.region,
                    'category': row.product_category,
                    'count': int(row.count),
                    'revenue': float(row.total_revenue)
                })
            return breakdown
        except Exception as e:
            print(f"Error getting breakdown: {e}")
            return []
    
    def _analyze_with_ai(self, status):
        """Use Gemini AI to analyze transaction volume anomaly."""
        try:
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            credentials.refresh(Request())
            
            os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
            os.environ['GOOGLE_CLOUD_PROJECT'] = self.project_id
            os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'
            
            genai.configure(credentials=credentials)
            model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            direction = 'spike' if status['deviation_percent'] > 0 else 'drop'
            
            prompt = f"""Analyze this transaction volume {direction}:

Current Transactions: {status['current_count']:,}
Expected: {status['baseline_avg']:,.0f}
Deviation: {status['deviation_percent']:+.1f}% ({status['deviation_amount']:+,.0f} transactions)
Z-Score: {status['z_score']:.2f}

Top Transaction Sources:
{json.dumps(status.get('breakdown', [])[:5], indent=2)}

Provide analysis in JSON format:
{{
    "root_cause": "Most likely reason for the {direction}",
    "risk_assessment": "Fraud/Duplicate/Legitimate/System Issue",
    "urgency": "LOW/MEDIUM/HIGH/CRITICAL",
    "recommended_actions": ["action1", "action2", "action3"]
}}

Return only valid JSON."""

            response = model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(temperature=0.2, max_output_tokens=400)
            )
            
            text = response.text.strip().replace('```json', '').replace('```', '').strip()
            
            try:
                analysis = json.loads(text)
                print(f"\n🤖 AI Analysis:")
                print(f"Root Cause: {analysis.get('root_cause')}")
                print(f"Risk: {analysis.get('risk_assessment')}")
                return analysis
            except:
                return {'root_cause': text[:200], 'urgency': 'MEDIUM'}
                
        except Exception as e:
            print(f"AI analysis failed: {e}")
            return {
                'root_cause': f'Transaction volume {direction} of {abs(status["deviation_percent"]):.1f}%',
                'risk_assessment': 'Unknown',
                'urgency': 'HIGH' if abs(status['deviation_percent']) > 100 else 'MEDIUM',
                'recommended_actions': ['Investigate transaction logs', 'Check for duplicate processing', 'Review fraud patterns']
            }
    
    def _calculate_severity(self, deviation_pct):
        """Calculate alert severity."""
        if deviation_pct >= 150:
            return 'CRITICAL'
        elif deviation_pct >= 100:
            return 'HIGH'
        elif deviation_pct >= 50:
            return 'MEDIUM'
        else:
            return 'LOW'
