"""
Pattern Break Detector
Identifies unusual changes in data distribution patterns
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import google.generativeai as genai
from google.auth import default
from google.auth.transport.requests import Request
import os
import json

class PatternDetector:
    def __init__(self, project_id, dataset_id='financial_monitoring'):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def check_pattern_breaks(self):
        """
        Detect breaks in normal data patterns.
        
        Returns:
            dict: Pattern break status with AI analysis
        """
        print(f"\n{'='*60}")
        print(f"PATTERN BREAK CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*60}")
        
        pattern_breaks = []
        
        # Check geographic distribution
        geo_break = self._check_geographic_pattern()
        if geo_break:
            pattern_breaks.append(geo_break)
        
        # Check product mix
        product_break = self._check_product_pattern()
        if product_break:
            pattern_breaks.append(product_break)
        
        # Check time-of-day pattern
        time_break = self._check_time_pattern()
        if time_break:
            pattern_breaks.append(product_break)
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'pattern_breaks_detected': len(pattern_breaks),
            'breaks': pattern_breaks,
            'has_breaks': len(pattern_breaks) > 0
        }
        
        print(f"Pattern Breaks Detected: {len(pattern_breaks)}")
        
        if pattern_breaks:
            print(f"\n📊 PATTERN BREAKS FOUND!")
            for break_info in pattern_breaks:
                print(f"  - {break_info['type']}: {break_info['description']}")
            
            # AI Analysis
            ai_analysis = self._analyze_with_ai(status)
            status['ai_analysis'] = ai_analysis
            status['severity'] = self._calculate_severity(pattern_breaks)
        else:
            print("\n✓ All patterns normal")
            status['severity'] = 'NONE'
        
        return status
    
    def _check_geographic_pattern(self):
        """Check for unusual geographic distribution."""
        query = f"""
        WITH today AS (
          SELECT region, COUNT(*) as count
          FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
          WHERE DATE(transaction_date) = CURRENT_DATE()
          GROUP BY region
        ),
        baseline AS (
          SELECT region, AVG(daily_count) as avg_count
          FROM (
            SELECT DATE(transaction_date) as date, region, COUNT(*) as daily_count
            FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
            WHERE DATE(transaction_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
              AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            GROUP BY date, region
          )
          GROUP BY region
        )
        SELECT 
          t.region,
          t.count as today_count,
          b.avg_count as baseline_avg,
          ((t.count - b.avg_count) / b.avg_count * 100) as deviation_pct
        FROM today t
        LEFT JOIN baseline b ON t.region = b.region
        WHERE ABS((t.count - b.avg_count) / b.avg_count * 100) > 100
        ORDER BY ABS(deviation_pct) DESC
        LIMIT 1
        """
        
        try:
            results = self.client.query(query).result()
            row = next(results, None)
            if row:
                return {
                    'type': 'GEOGRAPHIC',
                    'description': f"{row.region} region: {row.deviation_pct:+.0f}% deviation",
                    'details': {
                        'region': row.region,
                        'today_count': int(row.today_count),
                        'baseline': float(row.baseline_avg),
                        'deviation_pct': float(row.deviation_pct)
                    }
                }
        except Exception as e:
            print(f"Error checking geographic pattern: {e}")
        
        return None
    
    def _check_product_pattern(self):
        """Check for unusual product mix."""
        query = f"""
        WITH today AS (
          SELECT product_category, COUNT(*) as count
          FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
          WHERE DATE(transaction_date) = CURRENT_DATE()
          GROUP BY product_category
        ),
        baseline AS (
          SELECT product_category, AVG(daily_count) as avg_count
          FROM (
            SELECT DATE(transaction_date) as date, product_category, COUNT(*) as daily_count
            FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
            WHERE DATE(transaction_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
              AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            GROUP BY date, product_category
          )
          GROUP BY product_category
        )
        SELECT 
          t.product_category,
          t.count as today_count,
          b.avg_count as baseline_avg,
          ((t.count - b.avg_count) / b.avg_count * 100) as deviation_pct
        FROM today t
        LEFT JOIN baseline b ON t.product_category = b.product_category
        WHERE ABS((t.count - b.avg_count) / b.avg_count * 100) > 80
        ORDER BY ABS(deviation_pct) DESC
        LIMIT 1
        """
        
        try:
            results = self.client.query(query).result()
            row = next(results, None)
            if row:
                return {
                    'type': 'PRODUCT_MIX',
                    'description': f"{row.product_category}: {row.deviation_pct:+.0f}% deviation",
                    'details': {
                        'category': row.product_category,
                        'today_count': int(row.today_count),
                        'baseline': float(row.baseline_avg),
                        'deviation_pct': float(row.deviation_pct)
                    }
                }
        except Exception as e:
            print(f"Error checking product pattern: {e}")
        
        return None
    
    def _check_time_pattern(self):
        """Check for unusual time-of-day patterns."""
        # Simplified - could be expanded
        return None
    
    def _analyze_with_ai(self, status):
        """Use Gemini AI to analyze pattern breaks."""
        try:
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            credentials.refresh(Request())
            
            os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
            os.environ['GOOGLE_CLOUD_PROJECT'] = self.project_id
            os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'
            
            genai.configure(credentials=credentials)
            model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            prompt = f"""Analyze these pattern breaks:

Pattern Breaks: {status['pattern_breaks_detected']}

Details:
{json.dumps(status['breaks'], indent=2)}

Provide analysis in JSON format:
{{
    "root_cause": "Most likely reason for pattern changes",
    "risk_level": "Fraud/Opportunity/Normal Variation/System Issue",
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
                print(f"Risk: {analysis.get('risk_level')}")
                return analysis
            except:
                return {'root_cause': text[:200], 'urgency': 'MEDIUM'}
                
        except Exception as e:
            print(f"AI analysis failed: {e}")
            return {
                'root_cause': 'Unusual pattern detected in data distribution',
                'risk_level': 'Unknown',
                'urgency': 'MEDIUM',
                'recommended_actions': ['Investigate data sources', 'Check for fraud patterns', 'Review recent changes']
            }
    
    def _calculate_severity(self, breaks):
        """Calculate severity based on number and type of breaks."""
        if len(breaks) >= 3:
            return 'CRITICAL'
        elif len(breaks) >= 2:
            return 'HIGH'
        elif len(breaks) >= 1:
            return 'MEDIUM'
        else:
            return 'NONE'
