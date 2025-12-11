"""
Reconciliation Detector
Compares record counts across systems to detect mismatches
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import google.generativeai as genai
from google.auth import default
from google.auth.transport.requests import Request
import os
import json

class ReconciliationDetector:
    def __init__(self, project_id, dataset_id='financial_monitoring'):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def check_reconciliation(self, source_table, destination_table, date=None):
        """
        Check if source and destination record counts match.
        
        Args:
            source_table: Source table name
            destination_table: Destination table name
            date: Date to reconcile (default: today)
        
        Returns:
            dict: Reconciliation status
        """
        if date is None:
            date = datetime.now().date()
        
        print(f"\n{'='*60}")
        print(f"RECONCILIATION CHECK - {date}")
        print(f"{'='*60}")
        
        # Get source count
        source_count = self._get_record_count(source_table, date)
        
        # Get destination count
        dest_count = self._get_record_count(destination_table, date)
        
        if source_count is None or dest_count is None:
            return {'error': 'Unable to fetch record counts'}
        
        # Calculate discrepancy
        discrepancy = source_count - dest_count
        discrepancy_pct = (discrepancy / source_count * 100) if source_count > 0 else 0
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'date': date.isoformat(),
            'source_table': source_table,
            'destination_table': destination_table,
            'source_count': source_count,
            'destination_count': dest_count,
            'discrepancy': discrepancy,
            'discrepancy_percent': discrepancy_pct,
            'is_reconciled': discrepancy == 0
        }
        
        print(f"Source ({source_table}): {source_count:,} records")
        print(f"Destination ({destination_table}): {dest_count:,} records")
        print(f"Discrepancy: {discrepancy:+,} records ({discrepancy_pct:+.1f}%)")
        
        if discrepancy != 0:
            print(f"\n⚖️ RECONCILIATION FAILURE!")
            
            # Get time-based breakdown
            breakdown = self._get_hourly_breakdown(source_table, destination_table, date)
            status['hourly_breakdown'] = breakdown
            
            # AI Analysis
            ai_analysis = self._analyze_with_ai(status)
            status['ai_analysis'] = ai_analysis
            status['severity'] = self._calculate_severity(abs(discrepancy_pct))
        else:
            print("\n✓ Reconciliation successful")
            status['severity'] = 'NONE'
        
        return status
    
    def _get_record_count(self, table, date):
        """Get record count for a specific date."""
        query = f"""
        SELECT COUNT(*) as record_count
        FROM `{self.project_id}.{self.dataset_id}.{table}`
        WHERE DATE(transaction_date) = '{date}'
        """
        
        try:
            results = self.client.query(query).result()
            row = next(results, None)
            return int(row.record_count) if row else None
        except Exception as e:
            print(f"Error getting count from {table}: {e}")
            return None
    
    def _get_hourly_breakdown(self, source_table, dest_table, date):
        """Get hourly breakdown to identify when discrepancy occurred."""
        query = f"""
        WITH source_hourly AS (
          SELECT 
            EXTRACT(HOUR FROM transaction_date) as hour,
            COUNT(*) as count
          FROM `{self.project_id}.{self.dataset_id}.{source_table}`
          WHERE DATE(transaction_date) = '{date}'
          GROUP BY hour
        ),
        dest_hourly AS (
          SELECT 
            EXTRACT(HOUR FROM transaction_date) as hour,
            COUNT(*) as count
          FROM `{self.project_id}.{self.dataset_id}.{dest_table}`
          WHERE DATE(transaction_date) = '{date}'
          GROUP BY hour
        )
        SELECT 
          COALESCE(s.hour, d.hour) as hour,
          COALESCE(s.count, 0) as source_count,
          COALESCE(d.count, 0) as dest_count,
          (COALESCE(s.count, 0) - COALESCE(d.count, 0)) as discrepancy
        FROM source_hourly s
        FULL OUTER JOIN dest_hourly d ON s.hour = d.hour
        WHERE (COALESCE(s.count, 0) - COALESCE(d.count, 0)) != 0
        ORDER BY hour
        """
        
        try:
            results = self.client.query(query).result()
            breakdown = []
            for row in results:
                breakdown.append({
                    'hour': int(row.hour),
                    'source_count': int(row.source_count),
                    'dest_count': int(row.dest_count),
                    'discrepancy': int(row.discrepancy)
                })
            return breakdown
        except Exception as e:
            print(f"Error getting hourly breakdown: {e}")
            return []
    
    def _analyze_with_ai(self, status):
        """Use Gemini AI to analyze reconciliation failure."""
        try:
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            credentials.refresh(Request())
            
            os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
            os.environ['GOOGLE_CLOUD_PROJECT'] = self.project_id
            os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'
            
            genai.configure(credentials=credentials)
            model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            prompt = f"""Analyze this reconciliation failure:

Source: {status['source_count']:,} records
Destination: {status['destination_count']:,} records
Discrepancy: {status['discrepancy']:+,} records ({status['discrepancy_percent']:+.1f}%)

Hourly Breakdown:
{json.dumps(status.get('hourly_breakdown', []), indent=2)}

Provide analysis in JSON format:
{{
    "root_cause": "Most likely reason for discrepancy",
    "missing_or_duplicate": "MISSING/DUPLICATE/BOTH",
    "affected_time_window": "Time period with issues",
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
                print(f"Type: {analysis.get('missing_or_duplicate')}")
                return analysis
            except:
                return {'root_cause': text[:200], 'urgency': 'MEDIUM'}
                
        except Exception as e:
            print(f"AI analysis failed: {e}")
            issue_type = 'MISSING' if status['discrepancy'] > 0 else 'DUPLICATE'
            return {
                'root_cause': f'{abs(status["discrepancy"])} records {issue_type.lower()} in destination',
                'missing_or_duplicate': issue_type,
                'urgency': 'HIGH' if abs(status['discrepancy_percent']) > 5 else 'MEDIUM',
                'recommended_actions': ['Review ETL logs', 'Check for processing errors', 'Reprocess affected data']
            }
    
    def _calculate_severity(self, discrepancy_pct):
        """Calculate severity based on discrepancy percentage."""
        if discrepancy_pct >= 10:
            return 'CRITICAL'
        elif discrepancy_pct >= 5:
            return 'HIGH'
        elif discrepancy_pct >= 1:
            return 'MEDIUM'
        else:
            return 'LOW'
