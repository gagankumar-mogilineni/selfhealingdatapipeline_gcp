"""
Data Freshness Monitor
Tracks data staleness and alerts on delays
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import google.generativeai as genai
from google.auth import default
from google.auth.transport.requests import Request
import os
import json

class FreshnessDetector:
    def __init__(self, project_id, dataset_id='financial_monitoring'):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def check_data_freshness(self, max_age_minutes=30):
        """
        Check if data is fresh (recently updated).
        
        Args:
            max_age_minutes: Maximum acceptable data age in minutes
        
        Returns:
            dict: Freshness status with alerts
        """
        print(f"\n{'='*60}")
        print(f"DATA FRESHNESS CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*60}")
        
        # Check last update time for each data source
        sources = self._get_data_source_freshness()
        
        stale_sources = []
        fresh_sources = []
        
        for source in sources:
            age_minutes = source['age_minutes']
            if age_minutes > max_age_minutes:
                stale_sources.append(source)
            else:
                fresh_sources.append(source)
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'max_age_minutes': max_age_minutes,
            'total_sources': len(sources),
            'fresh_count': len(fresh_sources),
            'stale_count': len(stale_sources),
            'stale_sources': stale_sources,
            'is_stale': len(stale_sources) > 0
        }
        
        print(f"Total Data Sources: {len(sources)}")
        print(f"Fresh: {len(fresh_sources)}")
        print(f"Stale: {len(stale_sources)}")
        
        if stale_sources:
            print(f"\n⚠️ STALE DATA DETECTED!")
            for source in stale_sources:
                print(f"  - {source['source']}: {source['age_minutes']:.0f} min old (expected: <{max_age_minutes} min)")
            
            # AI Analysis
            ai_analysis = self._analyze_with_ai(status)
            status['ai_analysis'] = ai_analysis
            status['severity'] = self._calculate_severity(stale_sources, max_age_minutes)
        else:
            print("\n✓ All data sources are fresh")
            status['severity'] = 'NONE'
        
        return status
    
    def _get_data_source_freshness(self):
        """Get last update time for each data source."""
        sources = []
        
        # Check feed arrivals
        query_feeds = f"""
        SELECT 
          'feed_arrivals' as source,
          MAX(arrival_time) as last_update
        FROM `{self.project_id}.{self.dataset_id}.feed_arrivals`
        """
        
        # Check revenue data
        query_revenue = f"""
        SELECT 
          'daily_revenue' as source,
          MAX(transaction_date) as last_update
        FROM `{self.project_id}.{self.dataset_id}.daily_revenue`
        """
        
        for query in [query_feeds, query_revenue]:
            try:
                results = self.client.query(query).result()
                row = next(results, None)
                if row and row.last_update:
                    age = datetime.now(row.last_update.tzinfo) - row.last_update
                    age_minutes = age.total_seconds() / 60
                    sources.append({
                        'source': row.source,
                        'last_update': row.last_update.isoformat(),
                        'age_minutes': age_minutes,
                        'age_hours': age_minutes / 60
                    })
            except Exception as e:
                print(f"Error checking source: {e}")
        
        return sources
    
    def _analyze_with_ai(self, status):
        """Use Gemini AI to analyze data freshness issues."""
        try:
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            credentials.refresh(Request())
            
            os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
            os.environ['GOOGLE_CLOUD_PROJECT'] = self.project_id
            os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'
            
            genai.configure(credentials=credentials)
            model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            prompt = f"""Analyze this data freshness issue:

Stale Data Sources: {status['stale_count']}
Expected Max Age: {status['max_age_minutes']} minutes

Stale Sources:
{json.dumps(status['stale_sources'], indent=2)}

Provide analysis in JSON format:
{{
    "root_cause": "Most likely reason for stale data",
    "affected_systems": ["system1", "system2"],
    "downstream_impact": "Impact on dependent processes",
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
                print(f"Impact: {analysis.get('downstream_impact')}")
                return analysis
            except:
                return {'root_cause': text[:200], 'urgency': 'MEDIUM'}
                
        except Exception as e:
            print(f"AI analysis failed: {e}")
            return {
                'root_cause': f'{status["stale_count"]} data sources are stale',
                'downstream_impact': 'Reporting and analytics may be delayed',
                'urgency': 'HIGH',
                'recommended_actions': ['Check upstream systems', 'Verify network connectivity', 'Review data pipeline logs']
            }
    
    def _calculate_severity(self, stale_sources, max_age):
        """Calculate severity based on staleness."""
        if not stale_sources:
            return 'NONE'
        
        max_staleness = max(s['age_minutes'] for s in stale_sources)
        staleness_ratio = max_staleness / max_age
        
        if staleness_ratio >= 10:  # 10x expected age
            return 'CRITICAL'
        elif staleness_ratio >= 5:
            return 'HIGH'
        elif staleness_ratio >= 2:
            return 'MEDIUM'
        else:
            return 'LOW'
