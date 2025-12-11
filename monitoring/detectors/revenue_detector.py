"""
Revenue Anomaly Detector
Monitors daily revenue and detects anomalies using statistical analysis + AI
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import google.generativeai as genai
from google.auth import default
from google.auth.transport.requests import Request
import os
import json
import statistics

class RevenueDetector:
    def __init__(self, project_id, dataset_id='financial_monitoring'):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def check_revenue_anomaly(self, current_date=None):
        """
        Check if today's revenue is anomalous compared to historical baseline.
        
        Args:
            current_date: Date to check (default: today)
        
        Returns:
            dict: Anomaly status with AI analysis
        """
        if current_date is None:
            current_date = datetime.now().date()
        
        print(f"\n{'='*60}")
        print(f"REVENUE ANOMALY CHECK - {current_date}")
        print(f"{'='*60}")
        
        # Get current revenue
        current_revenue = self._get_daily_revenue(current_date)
        
        # Get historical baseline (30-day moving average)
        baseline = self._calculate_baseline(current_date, lookback_days=30)
        
        if current_revenue is None or baseline is None:
            return {'error': 'Insufficient data for analysis'}
        
        # Calculate deviation
        deviation_pct = ((current_revenue - baseline['avg']) / baseline['avg']) * 100
        z_score = (current_revenue - baseline['avg']) / baseline['std_dev'] if baseline['std_dev'] > 0 else 0
        
        # Determine if anomaly
        is_anomaly = abs(z_score) > 2.5  # 2.5 standard deviations
        
        status = {
            'date': current_date.isoformat(),
            'current_revenue': current_revenue,
            'baseline_avg': baseline['avg'],
            'baseline_std_dev': baseline['std_dev'],
            'deviation_amount': current_revenue - baseline['avg'],
            'deviation_percent': deviation_pct,
            'z_score': z_score,
            'is_anomaly': is_anomaly
        }
        
        print(f"Current Revenue: ${current_revenue:,.2f}")
        print(f"30-Day Average: ${baseline['avg']:,.2f}")
        print(f"Deviation: {deviation_pct:+.1f}% (${status['deviation_amount']:+,.2f})")
        print(f"Z-Score: {z_score:.2f}")
        
        if is_anomaly:
            print(f"\n🚨 ANOMALY DETECTED!")
            
            # Get detailed breakdown
            breakdown = self._get_revenue_breakdown(current_date)
            status['breakdown'] = breakdown
            
            # Get historical context
            context = self._get_historical_context(current_date)
            status['context'] = context
            
            # AI Analysis
            ai_analysis = self._analyze_with_ai(status)
            status['ai_analysis'] = ai_analysis
            status['severity'] = self._calculate_severity(abs(deviation_pct))
        else:
            print("\n✓ Revenue within normal range")
            status['severity'] = 'NONE'
        
        return status
    
    def _get_daily_revenue(self, date):
        """Get total revenue for a specific date."""
        query = f"""
        SELECT SUM(revenue) as total_revenue
        FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
        WHERE DATE(transaction_date) = '{date}'
        """
        
        try:
            results = self.client.query(query).result()
            row = next(results, None)
            return float(row.total_revenue) if row and row.total_revenue else None
        except Exception as e:
            print(f"Error getting daily revenue: {e}")
            return None
    
    def _calculate_baseline(self, current_date, lookback_days=30):
        """Calculate baseline statistics from historical data."""
        start_date = current_date - timedelta(days=lookback_days)
        end_date = current_date - timedelta(days=1)  # Exclude current day
        
        query = f"""
        SELECT 
            DATE(transaction_date) as date,
            SUM(revenue) as daily_revenue
        FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
        WHERE DATE(transaction_date) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY date
        ORDER BY date
        """
        
        try:
            results = self.client.query(query).result()
            revenues = [float(row.daily_revenue) for row in results if row.daily_revenue]
            
            if len(revenues) < 7:  # Need at least a week of data
                return None
            
            return {
                'avg': statistics.mean(revenues),
                'std_dev': statistics.stdev(revenues) if len(revenues) > 1 else 0,
                'median': statistics.median(revenues),
                'min': min(revenues),
                'max': max(revenues),
                'sample_size': len(revenues)
            }
        except Exception as e:
            print(f"Error calculating baseline: {e}")
            return None
    
    def _get_revenue_breakdown(self, date):
        """Get revenue breakdown by category/product/region."""
        query = f"""
        SELECT 
            product_category,
            region,
            COUNT(*) as transaction_count,
            SUM(revenue) as category_revenue,
            AVG(revenue) as avg_transaction_value
        FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
        WHERE DATE(transaction_date) = '{date}'
        GROUP BY product_category, region
        ORDER BY category_revenue DESC
        LIMIT 10
        """
        
        try:
            results = self.client.query(query).result()
            breakdown = []
            for row in results:
                breakdown.append({
                    'category': row.product_category,
                    'region': row.region,
                    'transactions': row.transaction_count,
                    'revenue': float(row.category_revenue),
                    'avg_value': float(row.avg_transaction_value)
                })
            return breakdown
        except Exception as e:
            print(f"Error getting breakdown: {e}")
            return []
    
    def _get_historical_context(self, current_date):
        """Get historical context for the same day of week."""
        day_of_week = current_date.strftime('%A')
        
        query = f"""
        SELECT 
            DATE(transaction_date) as date,
            SUM(revenue) as daily_revenue
        FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
        WHERE FORMAT_DATE('%A', DATE(transaction_date)) = '{day_of_week}'
        AND DATE(transaction_date) < '{current_date}'
        AND DATE(transaction_date) >= DATE_SUB('{current_date}', INTERVAL 90 DAY)
        GROUP BY date
        ORDER BY date DESC
        LIMIT 12
        """
        
        try:
            results = self.client.query(query).result()
            same_day_revenues = [float(row.daily_revenue) for row in results]
            
            return {
                'day_of_week': day_of_week,
                'same_day_avg': statistics.mean(same_day_revenues) if same_day_revenues else 0,
                'same_day_count': len(same_day_revenues),
                'recent_same_days': same_day_revenues[:4]
            }
        except Exception as e:
            print(f"Error getting context: {e}")
            return {}
    
    def _analyze_with_ai(self, status):
        """Use Gemini AI to analyze revenue anomaly."""
        try:
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            credentials.refresh(Request())
            
            os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
            os.environ['GOOGLE_CLOUD_PROJECT'] = self.project_id
            os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'
            
            genai.configure(credentials=credentials)
            model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            prompt = f"""Analyze this revenue anomaly:

Current Revenue: ${status['current_revenue']:,.2f}
Expected (30-day avg): ${status['baseline_avg']:,.2f}
Deviation: {status['deviation_percent']:+.1f}% (${status['deviation_amount']:+,.2f})
Z-Score: {status['z_score']:.2f}

Top Revenue Sources:
{json.dumps(status.get('breakdown', [])[:5], indent=2)}

Historical Context:
{json.dumps(status.get('context', {}), indent=2)}

Provide analysis in JSON format:
{{
    "root_cause": "Most likely reason for the anomaly",
    "contributing_factors": ["factor1", "factor2"],
    "business_impact": "Impact assessment",
    "urgency": "LOW/MEDIUM/HIGH/CRITICAL",
    "recommended_actions": ["action1", "action2", "action3"],
    "requires_immediate_attention": true/false
}}

Return only valid JSON."""

            response = model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(temperature=0.2, max_output_tokens=600)
            )
            
            text = response.text.strip().replace('```json', '').replace('```', '').strip()
            
            try:
                analysis = json.loads(text)
                print(f"\n🤖 AI Analysis:")
                print(f"Root Cause: {analysis.get('root_cause')}")
                print(f"Impact: {analysis.get('business_impact')}")
                print(f"Urgency: {analysis.get('urgency')}")
                return analysis
            except:
                return {
                    'root_cause': text[:200],
                    'business_impact': 'Unable to parse AI response',
                    'urgency': 'MEDIUM'
                }
                
        except Exception as e:
            print(f"AI analysis failed: {e}")
            direction = 'decrease' if status['deviation_percent'] < 0 else 'increase'
            return {
                'root_cause': f'Revenue {direction} of {abs(status["deviation_percent"]):.1f}%',
                'business_impact': f'${abs(status["deviation_amount"]):,.2f} variance from expected',
                'urgency': 'HIGH' if abs(status['deviation_percent']) > 20 else 'MEDIUM',
                'recommended_actions': ['Investigate transaction data', 'Check for system issues', 'Review pricing changes']
            }
    
    def _calculate_severity(self, deviation_pct):
        """Calculate alert severity based on deviation percentage."""
        if deviation_pct >= 30:
            return 'CRITICAL'
        elif deviation_pct >= 20:
            return 'HIGH'
        elif deviation_pct >= 10:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def forecast_revenue(self, days_ahead=7):
        """Simple revenue forecasting using moving average."""
        query = f"""
        SELECT 
            DATE(transaction_date) as date,
            SUM(revenue) as daily_revenue
        FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
        WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY date
        ORDER BY date DESC
        """
        
        try:
            results = self.client.query(query).result()
            revenues = [float(row.daily_revenue) for row in results]
            
            if len(revenues) < 7:
                return None
            
            # Simple moving average forecast
            forecast_value = statistics.mean(revenues[:7])
            
            return {
                'forecast_daily_avg': forecast_value,
                'forecast_period': f'Next {days_ahead} days',
                'forecast_total': forecast_value * days_ahead,
                'confidence': 'Medium (based on 7-day moving average)'
            }
        except Exception as e:
            print(f"Error forecasting: {e}")
            return None
