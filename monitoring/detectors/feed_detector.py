"""
Missing Feed Detector
Monitors feed arrivals and detects missing feeds using AI analysis
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import google.generativeai as genai
from google.auth import default
from google.auth.transport.requests import Request
import os
import json

class FeedDetector:
    def __init__(self, project_id, dataset_id='financial_monitoring'):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def check_feed_status(self, expected_feeds, check_time='17:00'):
        """
        Check if all expected feeds have arrived by the specified time.
        
        Args:
            expected_feeds: List of expected feed IDs (e.g., ['FEED_001', 'FEED_002', ...])
            check_time: Time to check (default: 5 PM)
        
        Returns:
            dict: Status with missing feeds and AI analysis
        """
        print(f"\n{'='*60}")
        print(f"FEED STATUS CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*60}")
        
        # Query to get today's feeds
        query = f"""
        SELECT DISTINCT feed_id, MAX(arrival_time) as last_arrival
        FROM `{self.project_id}.{self.dataset_id}.feed_arrivals`
        WHERE DATE(arrival_time) = CURRENT_DATE()
        GROUP BY feed_id
        """
        
        try:
            results = self.client.query(query).result()
            arrived_feeds = {row.feed_id: row.last_arrival for row in results}
            
            # Identify missing feeds
            missing_feeds = [feed for feed in expected_feeds if feed not in arrived_feeds]
            
            status = {
                'timestamp': datetime.now().isoformat(),
                'expected_count': len(expected_feeds),
                'arrived_count': len(arrived_feeds),
                'missing_count': len(missing_feeds),
                'missing_feeds': missing_feeds,
                'arrived_feeds': list(arrived_feeds.keys())
            }
            
            print(f"Expected Feeds: {len(expected_feeds)}")
            print(f"Arrived Feeds: {len(arrived_feeds)}")
            print(f"Missing Feeds: {len(missing_feeds)}")
            
            if missing_feeds:
                print(f"\n⚠️  ALERT: {len(missing_feeds)} feeds missing!")
                print(f"Missing: {', '.join(missing_feeds)}")
                
                # Get historical context
                context = self._get_historical_context(missing_feeds)
                
                # AI Analysis
                ai_analysis = self._analyze_with_ai(status, context)
                status['ai_analysis'] = ai_analysis
                status['severity'] = self._calculate_severity(len(missing_feeds), len(expected_feeds))
            else:
                print("\n✓ All feeds arrived successfully!")
                status['severity'] = 'NONE'
            
            return status
            
        except Exception as e:
            print(f"Error checking feed status: {e}")
            return {'error': str(e)}
    
    def _get_historical_context(self, missing_feeds):
        """Get historical patterns for missing feeds."""
        feed_list = "', '".join(missing_feeds)
        
        query = f"""
        SELECT 
            feed_id,
            COUNT(*) as total_arrivals,
            COUNT(DISTINCT DATE(arrival_time)) as days_arrived,
            MAX(arrival_time) as last_seen,
            AVG(EXTRACT(HOUR FROM arrival_time)) as avg_arrival_hour
        FROM `{self.project_id}.{self.dataset_id}.feed_arrivals`
        WHERE feed_id IN ('{feed_list}')
        AND arrival_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        GROUP BY feed_id
        """
        
        try:
            results = self.client.query(query).result()
            context = {}
            for row in results:
                context[row.feed_id] = {
                    'total_arrivals': row.total_arrivals,
                    'days_arrived': row.days_arrived,
                    'last_seen': row.last_seen.isoformat() if row.last_seen else 'Never',
                    'avg_arrival_hour': f"{int(row.avg_arrival_hour)}:00" if row.avg_arrival_hour else 'N/A'
                }
            return context
        except Exception as e:
            print(f"Error getting historical context: {e}")
            return {}
    
    def _analyze_with_ai(self, status, context):
        """Use Gemini AI to analyze missing feeds."""
        try:
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            credentials.refresh(Request())
            
            os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
            os.environ['GOOGLE_CLOUD_PROJECT'] = self.project_id
            os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'
            
            genai.configure(credentials=credentials)
            model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            prompt = f"""Analyze this missing feed situation:

                    Current Status:
                    - Expected: {status['expected_count']} feeds
                    - Arrived: {status['arrived_count']} feeds  
                    - Missing: {status['missing_count']} feeds
                    - Missing Feed IDs: {', '.join(status['missing_feeds'])}

                    Historical Context:
                    {json.dumps(context, indent=2)}

                    Provide analysis in JSON format:
                    {{
                        "root_cause": "Most likely reason for missing feeds",
                        "impact": "Business impact assessment",
                        "urgency": "LOW/MEDIUM/HIGH/CRITICAL",
                        "recommended_actions": ["action1", "action2", "action3"],
                        "estimated_revenue_impact": "Dollar amount or percentage"
                    }}

                    Return only valid JSON.
                """

            response = model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(temperature=0.2, max_output_tokens=500)
            )
            
            text = response.text.strip().replace('```json', '').replace('```', '').strip()
            
            try:
                analysis = json.loads(text)
                print(f"\n🤖 AI Analysis:")
                print(f"Root Cause: {analysis.get('root_cause')}")
                print(f"Impact: {analysis.get('impact')}")
                print(f"Urgency: {analysis.get('urgency')}")
                return analysis
            except:
                return {
                    'root_cause': text[:200],
                    'impact': 'Unable to parse AI response',
                    'urgency': 'MEDIUM'
                }
                
        except Exception as e:
            print(f"AI analysis failed: {e}")
            return {
                'root_cause': f'{len(status["missing_feeds"])} feeds did not arrive',
                'impact': 'Data processing may be incomplete',
                'urgency': 'HIGH' if len(status['missing_feeds']) > 5 else 'MEDIUM',
                'recommended_actions': ['Check upstream systems', 'Contact data providers', 'Use backup data if available']
            }
    
    def _calculate_severity(self, missing_count, total_count):
        """Calculate alert severity based on missing feed percentage."""
        percentage = (missing_count / total_count) * 100
        
        if percentage >= 50:
            return 'CRITICAL'
        elif percentage >= 30:
            return 'HIGH'
        elif percentage >= 10:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def get_feed_trends(self, days=30):
        """Get feed arrival trends over time."""
        query = f"""
        SELECT 
            DATE(arrival_time) as date,
            COUNT(DISTINCT feed_id) as feed_count,
            COUNT(*) as total_arrivals
        FROM `{self.project_id}.{self.dataset_id}.feed_arrivals`
        WHERE arrival_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        GROUP BY date
        ORDER BY date DESC
        """
        
        try:
            results = self.client.query(query).result()
            trends = [{'date': row.date.isoformat(), 'feed_count': row.feed_count, 'total_arrivals': row.total_arrivals} for row in results]
            return trends
        except Exception as e:
            print(f"Error getting trends: {e}")
            return []
