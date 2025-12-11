"""
Alert Manager
Handles alert routing, deduplication, and notifications
"""

from datetime import datetime, timedelta
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests

class AlertManager:
    def __init__(self, config=None):
        """
        Initialize Alert Manager with configuration.
        
        Args:
            config: Dict with alert settings (slack_webhook, email_config, etc.)
        """
        self.config = config or {}
        self.alert_history = {}  # For deduplication
        
    def send_alert(self, alert_type, severity, title, details, recommendations=None):
        """
        Send alert through appropriate channels based on severity.
        
        Args:
            alert_type: 'FEED' or 'REVENUE' or 'CUSTOM'
            severity: 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
            title: Alert title
            details: Dict with alert details
            recommendations: List of recommended actions
        """
        # Check for duplicate alerts
        if self._is_duplicate(alert_type, title):
            print(f"Skipping duplicate alert: {title}")
            return
        
        # Format alert message
        alert = self._format_alert(alert_type, severity, title, details, recommendations)
        
        # Route based on severity
        if severity == 'CRITICAL':
            self._send_to_all_channels(alert)
        elif severity == 'HIGH':
            self._send_email(alert)
            self._send_slack(alert)
        elif severity == 'MEDIUM':
            self._send_slack(alert)
        else:  # LOW
            self._log_alert(alert)
        
        # Record alert
        self._record_alert(alert_type, title)
        
    def _format_alert(self, alert_type, severity, title, details, recommendations):
        """Format alert message."""
        emoji_map = {
            'CRITICAL': '🚨',
            'HIGH': '⚠️',
            'MEDIUM': '📊',
            'LOW': 'ℹ️'
        }
        
        emoji = emoji_map.get(severity, '📢')
        
        message = f"""
{emoji} **{severity}: {title}**

**Type:** {alert_type}
**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

**Details:**
"""
        
        # Add details
        for key, value in details.items():
            if isinstance(value, (int, float)):
                if 'revenue' in key.lower() or 'amount' in key.lower():
                    message += f"- {key}: ${value:,.2f}\n"
                elif 'percent' in key.lower():
                    message += f"- {key}: {value:+.1f}%\n"
                else:
                    message += f"- {key}: {value}\n"
            else:
                message += f"- {key}: {value}\n"
        
        # Add recommendations
        if recommendations:
            message += "\n**Recommended Actions:**\n"
            for i, action in enumerate(recommendations, 1):
                message += f"{i}. {action}\n"
        
        return {
            'severity': severity,
            'title': title,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'type': alert_type
        }
    
    def _send_slack(self, alert):
        """Send alert to Slack."""
        webhook_url = self.config.get('slack_webhook')
        
        if not webhook_url:
            print("Slack webhook not configured")
            return
        
        # Format for Slack
        color_map = {
            'CRITICAL': '#FF0000',
            'HIGH': '#FF6600',
            'MEDIUM': '#FFCC00',
            'LOW': '#00CC00'
        }
        
        payload = {
            'attachments': [{
                'color': color_map.get(alert['severity'], '#808080'),
                'title': alert['title'],
                'text': alert['message'],
                'footer': f"Financial Monitoring System | {alert['timestamp']}",
                'mrkdwn_in': ['text']
            }]
        }
        
        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
            if response.status_code == 200:
                print(f"✓ Slack alert sent: {alert['title']}")
            else:
                print(f"✗ Slack alert failed: {response.status_code}")
        except Exception as e:
            print(f"Error sending Slack alert: {e}")
    
    def _send_email(self, alert):
        """Send alert via email."""
        email_config = self.config.get('email', {})
        
        if not email_config.get('enabled'):
            print("Email alerts not configured")
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = email_config.get('from_address')
            msg['To'] = ', '.join(email_config.get('to_addresses', []))
            msg['Subject'] = f"[{alert['severity']}] {alert['title']}"
            
            # HTML email body
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <h2 style="color: {'#FF0000' if alert['severity'] == 'CRITICAL' else '#FF6600'};">
                    {alert['title']}
                </h2>
                <pre style="background-color: #f5f5f5; padding: 15px; border-radius: 5px;">
{alert['message']}
                </pre>
                <p style="color: #666; font-size: 12px;">
                    Sent by Financial Monitoring System at {alert['timestamp']}
                </p>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send email
            with smtplib.SMTP(email_config.get('smtp_server'), email_config.get('smtp_port', 587)) as server:
                server.starttls()
                if email_config.get('username') and email_config.get('password'):
                    server.login(email_config['username'], email_config['password'])
                server.send_message(msg)
            
            print(f"✓ Email alert sent: {alert['title']}")
        except Exception as e:
            print(f"Error sending email alert: {e}")
    
    def _send_to_all_channels(self, alert):
        """Send to all configured channels (for critical alerts)."""
        self._send_slack(alert)
        self._send_email(alert)
        self._log_alert(alert)
        
        # Could add: PagerDuty, SMS, phone call, etc.
        print(f"🚨 CRITICAL ALERT sent to all channels: {alert['title']}")
    
    def _log_alert(self, alert):
        """Log alert to file/database."""
        print(f"\n{'='*60}")
        print(f"ALERT LOGGED: {alert['severity']}")
        print(f"{'='*60}")
        print(alert['message'])
        print(f"{'='*60}\n")
    
    def _is_duplicate(self, alert_type, title):
        """Check if this is a duplicate alert within the last hour."""
        key = f"{alert_type}:{title}"
        
        if key in self.alert_history:
            last_sent = self.alert_history[key]
            if datetime.now() - last_sent < timedelta(hours=1):
                return True
        
        return False
    
    def _record_alert(self, alert_type, title):
        """Record alert to prevent duplicates."""
        key = f"{alert_type}:{title}"
        self.alert_history[key] = datetime.now()
    
    def create_feed_alert(self, feed_status):
        """Create alert from feed detector status."""
        if feed_status.get('severity') == 'NONE':
            return
        
        details = {
            'Expected Feeds': feed_status['expected_count'],
            'Arrived Feeds': feed_status['arrived_count'],
            'Missing Feeds': feed_status['missing_count'],
            'Missing IDs': ', '.join(feed_status['missing_feeds'][:10])
        }
        
        ai_analysis = feed_status.get('ai_analysis', {})
        recommendations = ai_analysis.get('recommended_actions', [
            'Check upstream data providers',
            'Verify network connectivity',
            'Review feed processing logs'
        ])
        
        self.send_alert(
            alert_type='FEED',
            severity=feed_status['severity'],
            title=f"Missing Feeds Detected: {feed_status['missing_count']} feeds",
            details=details,
            recommendations=recommendations
        )
    
    def create_revenue_alert(self, revenue_status):
        """Create alert from revenue detector status."""
        if revenue_status.get('severity') == 'NONE':
            return
        
        details = {
            'Current Revenue': revenue_status['current_revenue'],
            'Expected Revenue': revenue_status['baseline_avg'],
            'Deviation': f"{revenue_status['deviation_percent']:+.1f}%",
            'Dollar Impact': revenue_status['deviation_amount'],
            'Z-Score': f"{revenue_status['z_score']:.2f}"
        }
        
        ai_analysis = revenue_status.get('ai_analysis', {})
        recommendations = ai_analysis.get('recommended_actions', [
            'Investigate transaction data',
            'Check for system issues',
            'Review pricing changes'
        ])
        
        direction = 'Drop' if revenue_status['deviation_percent'] < 0 else 'Spike'
        
        self.send_alert(
            alert_type='REVENUE',
            severity=revenue_status['severity'],
            title=f"Revenue {direction}: {abs(revenue_status['deviation_percent']):.1f}% deviation",
            details=details,
            recommendations=recommendations
        )
