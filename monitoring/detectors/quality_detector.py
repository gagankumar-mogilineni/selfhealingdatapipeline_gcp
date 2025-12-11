"""
Data Quality Degradation Detector
Tracks data quality metrics over time and detects degradation
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import google.generativeai as genai
from google.auth import default
from google.auth.transport.requests import Request
import os
import json

class QualityDetector:
    def __init__(self, project_id, dataset_id='financial_monitoring'):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def check_quality_degradation(self):
        """
        Check for data quality degradation over time.
        
        Returns:
            dict: Quality degradation status
        """
        print(f"\n{'='*60}")
        print(f"DATA QUALITY DEGRADATION CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*60}")
        
        # Check null value trends
        null_trends = self._check_null_trends()
        
        # Check duplicate trends
        duplicate_trends = self._check_duplicate_trends()
        
        degradations = []
        if null_trends:
            degradations.extend(null_trends)
        if duplicate_trends:
            degradations.extend(duplicate_trends)
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'degradations_detected': len(degradations),
            'degradations': degradations,
            'has_degradation': len(degradations) > 0
        }
        
        print(f"Quality Issues Detected: {len(degradations)}")
        
        if degradations:
            print(f"\n🔍 QUALITY DEGRADATION DETECTED!")
            for deg in degradations:
                print(f"  - {deg['field']}: {deg['description']}")
            
            # AI Analysis
            ai_analysis = self._analyze_with_ai(status)
            status['ai_analysis'] = ai_analysis
            status['severity'] = self._calculate_severity(degradations)
        else:
            print("\n✓ Data quality stable")
            status['severity'] = 'NONE'
        
        return status
    
    def _check_null_trends(self):
        """Check for increasing null values."""
        query = f"""
        WITH today_nulls AS (
          SELECT 
            'customer_id' as field,
            COUNTIF(customer_id IS NULL) / COUNT(*) * 100 as null_pct
          FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
          WHERE DATE(transaction_date) = CURRENT_DATE()
        ),
        baseline_nulls AS (
          SELECT 
            AVG(daily_null_pct) as avg_null_pct
          FROM (
            SELECT 
              DATE(transaction_date) as date,
              COUNTIF(customer_id IS NULL) / COUNT(*) * 100 as daily_null_pct
            FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
            WHERE DATE(transaction_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
              AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            GROUP BY date
          )
        )
        SELECT 
          t.field,
          t.null_pct as today_null_pct,
          b.avg_null_pct as baseline_null_pct,
          (t.null_pct - b.avg_null_pct) as increase
        FROM today_nulls t, baseline_nulls b
        WHERE (t.null_pct - b.avg_null_pct) > 1.0
        """
        
        try:
            results = self.client.query(query).result()
            degradations = []
            for row in results:
                degradations.append({
                    'type': 'NULL_INCREASE',
                    'field': row.field,
                    'description': f"NULL values increased from {row.baseline_null_pct:.1f}% to {row.today_null_pct:.1f}%",
                    'details': {
                        'current_pct': float(row.today_null_pct),
                        'baseline_pct': float(row.baseline_null_pct),
                        'increase': float(row.increase)
                    }
                })
            return degradations
        except Exception as e:
            print(f"Error checking null trends: {e}")
            return []
    
    def _check_duplicate_trends(self):
        """Check for increasing duplicate records."""
        query = f"""
        WITH today_duplicates AS (
          SELECT 
            COUNT(*) - COUNT(DISTINCT transaction_id) as duplicate_count,
            COUNT(*) as total_count
          FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
          WHERE DATE(transaction_date) = CURRENT_DATE()
        ),
        baseline_duplicates AS (
          SELECT 
            AVG(daily_dup_pct) as avg_dup_pct
          FROM (
            SELECT 
              DATE(transaction_date) as date,
              (COUNT(*) - COUNT(DISTINCT transaction_id)) / COUNT(*) * 100 as daily_dup_pct
            FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
            WHERE DATE(transaction_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
              AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            GROUP BY date
          )
        )
        SELECT 
          t.duplicate_count,
          (t.duplicate_count / t.total_count * 100) as today_dup_pct,
          b.avg_dup_pct as baseline_dup_pct,
          ((t.duplicate_count / t.total_count * 100) - b.avg_dup_pct) as increase
        FROM today_duplicates t, baseline_duplicates b
        WHERE ((t.duplicate_count / t.total_count * 100) - b.avg_dup_pct) > 0.5
        """
        
        try:
            results = self.client.query(query).result()
            row = next(results, None)
            if row:
                return [{
                    'type': 'DUPLICATE_INCREASE',
                    'field': 'transaction_id',
                    'description': f"Duplicate records increased from {row.baseline_dup_pct:.1f}% to {row.today_dup_pct:.1f}%",
                    'details': {
                        'duplicate_count': int(row.duplicate_count),
                        'current_pct': float(row.today_dup_pct),
                        'baseline_pct': float(row.baseline_dup_pct),
                        'increase': float(row.increase)
                    }
                }]
        except Exception as e:
            print(f"Error checking duplicates: {e}")
        
        return []
    
    def _analyze_with_ai(self, status):
        """Use Gemini AI to analyze quality degradation."""
        try:
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            credentials.refresh(Request())
            
            os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
            os.environ['GOOGLE_CLOUD_PROJECT'] = self.project_id
            os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'
            
            genai.configure(credentials=credentials)
            model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            prompt = f"""Analyze this data quality degradation:

Quality Issues: {status['degradations_detected']}

Details:
{json.dumps(status['degradations'], indent=2)}

Provide analysis in JSON format:
{{
    "root_cause": "Most likely reason for quality degradation",
    "data_usability_impact": "Impact on data usability",
    "urgency": "LOW/MEDIUM/HIGH/CRITICAL",
    "recommended_actions": ["action1", "action2"]
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
                print(f"Impact: {analysis.get('data_usability_impact')}")
                return analysis
            except:
                return {'root_cause': text[:200], 'urgency': 'MEDIUM'}
                
        except Exception as e:
            print(f"AI analysis failed: {e}")
            return {
                'root_cause': 'Data quality metrics have degraded',
                'data_usability_impact': 'Data may be unreliable for analysis',
                'urgency': 'HIGH',
                'recommended_actions': ['Investigate upstream data sources', 'Review ETL processes', 'Check data validation rules']
            }
    
    def _calculate_severity(self, degradations):
        """Calculate severity based on degradation count and type."""
        if len(degradations) >= 3:
            return 'CRITICAL'
        elif len(degradations) >= 2:
            return 'HIGH'
        elif len(degradations) >= 1:
            # Check if severe degradation
            for deg in degradations:
                if deg['details'].get('increase', 0) > 5:
                    return 'HIGH'
            return 'MEDIUM'
        else:
            return 'NONE'
