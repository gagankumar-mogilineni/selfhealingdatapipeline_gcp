"""
SLA Breach Predictor
Predicts if processing will complete within SLA based on current velocity
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import google.generativeai as genai
from google.auth import default
from google.auth.transport.requests import Request
import os
import json

class SLADetector:
    def __init__(self, project_id, dataset_id='financial_monitoring'):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def predict_sla_breach(self, total_records, sla_hours=4):
        """
        Predict if processing will breach SLA based on current velocity.
        
        Args:
            total_records: Total records to process
            sla_hours: SLA time limit in hours
        
        Returns:
            dict: SLA prediction status
        """
        print(f"\n{'='*60}")
        print(f"SLA BREACH PREDICTION - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*60}")
        
        # Get current processing velocity
        velocity = self._calculate_processing_velocity()
        
        if velocity is None or velocity == 0:
            return {'error': 'Unable to calculate processing velocity'}
        
        # Calculate estimated completion time
        records_remaining = total_records - velocity['processed_count']
        hours_remaining = records_remaining / velocity['records_per_hour'] if velocity['records_per_hour'] > 0 else float('inf')
        
        # Check if will breach SLA
        will_breach = hours_remaining > sla_hours
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'total_records': total_records,
            'processed_count': velocity['processed_count'],
            'records_remaining': records_remaining,
            'processing_rate': velocity['records_per_hour'],
            'estimated_hours_remaining': hours_remaining,
            'sla_hours': sla_hours,
            'will_breach_sla': will_breach,
            'breach_margin_hours': hours_remaining - sla_hours
        }
        
        print(f"Total Records: {total_records:,}")
        print(f"Processed: {velocity['processed_count']:,}")
        print(f"Remaining: {records_remaining:,}")
        print(f"Processing Rate: {velocity['records_per_hour']:,.0f} records/hour")
        print(f"Estimated Time: {hours_remaining:.1f} hours")
        print(f"SLA: {sla_hours} hours")
        
        if will_breach:
            print(f"\n⏱️ SLA BREACH PREDICTED!")
            print(f"Will exceed SLA by {status['breach_margin_hours']:.1f} hours")
            
            # Calculate required scaling
            scaling = self._calculate_required_scaling(records_remaining, sla_hours, velocity['records_per_hour'])
            status['scaling_recommendation'] = scaling
            
            # AI Analysis
            ai_analysis = self._analyze_with_ai(status)
            status['ai_analysis'] = ai_analysis
            status['severity'] = self._calculate_severity(status['breach_margin_hours'])
        else:
            print(f"\n✓ On track to meet SLA (margin: {abs(status['breach_margin_hours']):.1f} hours)")
            status['severity'] = 'NONE'
        
        return status
    
    def _calculate_processing_velocity(self):
        """Calculate current processing rate."""
        query = f"""
        WITH recent_processing AS (
          SELECT 
            COUNT(*) as processed_count,
            TIMESTAMP_DIFF(MAX(transaction_date), MIN(transaction_date), MINUTE) as minutes_elapsed
          FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
          WHERE transaction_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        )
        SELECT 
          processed_count,
          (processed_count / NULLIF(minutes_elapsed, 0) * 60) as records_per_hour
        FROM recent_processing
        """
        
        try:
            results = self.client.query(query).result()
            row = next(results, None)
            if row:
                return {
                    'processed_count': int(row.processed_count),
                    'records_per_hour': float(row.records_per_hour) if row.records_per_hour else 0
                }
        except Exception as e:
            print(f"Error calculating velocity: {e}")
        
        return None
    
    def _calculate_required_scaling(self, records_remaining, sla_hours, current_rate):
        """Calculate how much to scale to meet SLA."""
        required_rate = records_remaining / sla_hours
        scaling_factor = required_rate / current_rate if current_rate > 0 else 0
        
        return {
            'current_rate': current_rate,
            'required_rate': required_rate,
            'scaling_factor': scaling_factor,
            'recommended_workers': int(scaling_factor) + 1
        }
    
    def _analyze_with_ai(self, status):
        """Use Gemini AI to analyze SLA breach risk."""
        try:
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            credentials.refresh(Request())
            
            os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
            os.environ['GOOGLE_CLOUD_PROJECT'] = self.project_id
            os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'
            
            genai.configure(credentials=credentials)
            model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            prompt = f"""Analyze this SLA breach risk:

Total Records: {status['total_records']:,}
Processed: {status['processed_count']:,}
Remaining: {status['records_remaining']:,}
Current Rate: {status['processing_rate']:,.0f} records/hour
Estimated Time: {status['estimated_hours_remaining']:.1f} hours
SLA: {status['sla_hours']} hours
Breach Margin: {status['breach_margin_hours']:+.1f} hours

Scaling Recommendation:
{json.dumps(status.get('scaling_recommendation', {}), indent=2)}

Provide analysis in JSON format:
{{
    "root_cause": "Why processing is slow",
    "bottleneck": "Identified bottleneck",
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
                print(f"Bottleneck: {analysis.get('bottleneck')}")
                return analysis
            except:
                return {'root_cause': text[:200], 'urgency': 'HIGH'}
                
        except Exception as e:
            print(f"AI analysis failed: {e}")
            return {
                'root_cause': 'Processing velocity too slow to meet SLA',
                'bottleneck': 'Insufficient processing capacity',
                'urgency': 'HIGH',
                'recommended_actions': [
                    f"Scale workers by {status.get('scaling_recommendation', {}).get('scaling_factor', 2):.0f}x",
                    'Optimize query performance',
                    'Check for resource constraints'
                ]
            }
    
    def _calculate_severity(self, breach_margin_hours):
        """Calculate severity based on breach margin."""
        if breach_margin_hours >= 4:
            return 'CRITICAL'
        elif breach_margin_hours >= 2:
            return 'HIGH'
        elif breach_margin_hours >= 1:
            return 'MEDIUM'
        else:
            return 'LOW'
