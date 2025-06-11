import os
import logging
from typing import Optional, Dict, Any
import traceback
from datetime import datetime

try:
    import brevo_python
    from brevo_python.rest import ApiException
    BREVO_AVAILABLE = True
except ImportError:
    BREVO_AVAILABLE = False
    brevo_python = None
    ApiException = Exception

logger = logging.getLogger(__name__)

class EmailNotifier:
    """Email notification service using Brevo API"""
    
    def __init__(self):
        self.api_key = os.getenv('BREVO_API_KEY')
        self.sender_email = os.getenv('BREVO_SENDER_EMAIL', 'noreply@galvestonscraper.com')
        self.sender_name = os.getenv('BREVO_SENDER_NAME', 'Galveston Scraper')
        self.recipient_email = os.getenv('NOTIFICATION_EMAIL')
        self.recipient_name = os.getenv('NOTIFICATION_NAME', 'Admin')
        
        self.api_instance = None
        
        if not BREVO_AVAILABLE:
            logger.error("Brevo Python SDK not installed. Install with: pip install brevo-python")
            return
            
        if not self.api_key:
            logger.error("BREVO_API_KEY environment variable not set")
            return
            
        if not self.recipient_email:
            logger.error("NOTIFICATION_EMAIL environment variable not set")
            return
            
        try:
            # Configure API key authorization
            configuration = brevo_python.Configuration()
            configuration.api_key['api-key'] = self.api_key
            
            # Create API instance
            self.api_instance = brevo_python.TransactionalEmailsApi(brevo_python.ApiClient(configuration))
            logger.info("Email notifier initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize email notifier: {str(e)}")
            self.api_instance = None
    
    def send_error_notification(self, error_message: str, error_details: Optional[str] = None, 
                              context: Optional[Dict[str, Any]] = None) -> bool:
        """Send error notification email"""
        if not self.api_instance:
            logger.warning("Email notifier not properly initialized, cannot send notification")
            return False
            
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
            
            # Build HTML content
            html_content = f"""
            <html>
            <body>
                <h2>🚨 Galveston Scraper Error Alert</h2>
                <p><strong>Time:</strong> {timestamp}</p>
                <p><strong>Error Message:</strong></p>
                <div style="background-color: #f8f8f8; padding: 10px; border-left: 4px solid #dc3545;">
                    <code>{error_message}</code>
                </div>
            """
            
            if error_details:
                html_content += f"""
                <p><strong>Error Details:</strong></p>
                <div style="background-color: #f8f8f8; padding: 10px; border-left: 4px solid #ffc107;">
                    <pre style="white-space: pre-wrap; font-family: monospace; font-size: 12px;">{error_details}</pre>
                </div>
                """
            
            if context:
                html_content += "<p><strong>Context Information:</strong></p><ul>"
                for key, value in context.items():
                    html_content += f"<li><strong>{key}:</strong> {value}</li>"
                html_content += "</ul>"
            
            html_content += """
                <hr>
                <p><em>This is an automated notification from the Galveston Tax Scraper system.</em></p>
            </body>
            </html>
            """
            
            # Prepare email
            subject = f"🚨 Galveston Scraper Error - {timestamp}"
            sender = {"name": self.sender_name, "email": self.sender_email}
            to = [{"email": self.recipient_email, "name": self.recipient_name}]
            
            send_smtp_email = brevo_python.SendSmtpEmail(
                to=to,
                html_content=html_content,
                sender=sender,
                subject=subject
            )
            
            # Send email
            api_response = self.api_instance.send_transac_email(send_smtp_email)
            logger.info(f"Error notification email sent successfully: {api_response.message_id}")
            return True
            
        except ApiException as e:
            logger.error(f"Failed to send error notification via Brevo API: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error while sending notification: {str(e)}")
            return False
    
    def send_completion_notification(self, total_records: int, patterns_completed: int, 
                                   execution_time: str, patterns_failed: int = 0, patterns_no_results: int = 0) -> bool:
        """Send completion notification"""
        if not self.api_instance:
            return False
            
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
            total_patterns_processed = patterns_completed + patterns_failed
            patterns_with_records = patterns_completed - patterns_no_results
            
            # Determine if completion was fully successful or partial
            status_emoji = "✅" if patterns_failed == 0 else "⚠️"
            status_text = "Completed Successfully" if patterns_failed == 0 else "Completed with Some Failures"
            
            html_content = f"""
            <html>
            <body>
                <h2>{status_emoji} Galveston Scraper {status_text}</h2>
                <p><strong>Completion Time:</strong> {timestamp}</p>
                <p><strong>Total Records Scraped:</strong> {total_records:,}</p>
                <p><strong>Search Patterns Completed:</strong> {patterns_completed:,}</p>
                <p><strong>&nbsp;&nbsp;• Patterns with Records:</strong> {patterns_with_records:,}</p>
                <p><strong>&nbsp;&nbsp;• Patterns with No Results:</strong> {patterns_no_results:,}</p>
                <p><strong>Search Patterns Failed:</strong> {patterns_failed:,}</p>
                <p><strong>Total Patterns Processed:</strong> {total_patterns_processed:,}</p>
                <p><strong>Total Execution Time:</strong> {execution_time}</p>
            """
            
            if patterns_failed > 0:
                success_rate = (patterns_completed / total_patterns_processed) * 100 if total_patterns_processed > 0 else 0
                html_content += f"""
                <div style="background-color: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; margin: 10px 0;">
                    <strong>Success Rate:</strong> {success_rate:.1f}%<br>
                    <strong>Note:</strong> {patterns_failed} search patterns failed after maximum retry attempts. 
                    These patterns will be skipped and can be manually reviewed if needed.
                </div>
                """
            
            html_content += """
                <hr>
                <p><em>The Galveston Tax Scraper has completed processing.</em></p>
            </body>
            </html>
            """
            
            subject_suffix = f"{total_records:,} records"
            if patterns_failed > 0:
                subject_suffix += f", {patterns_failed} patterns failed"
            
            subject = f"{status_emoji} Galveston Scraper Completed - {subject_suffix}"
            sender = {"name": self.sender_name, "email": self.sender_email}
            to = [{"email": self.recipient_email, "name": self.recipient_name}]
            
            send_smtp_email = brevo_python.SendSmtpEmail(
                to=to,
                html_content=html_content,
                sender=sender,
                subject=subject
            )
            
            api_response = self.api_instance.send_transac_email(send_smtp_email)
            logger.info(f"Completion notification email sent successfully: {api_response.message_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send completion notification: {str(e)}")
            return False

# Global notifier instance
_notifier = None

def get_email_notifier() -> EmailNotifier:
    """Get global email notifier instance"""
    global _notifier
    if _notifier is None:
        _notifier = EmailNotifier()
    return _notifier

def send_error_notification(error_message: str, error_details: Optional[str] = None, 
                          context: Optional[Dict[str, Any]] = None) -> bool:
    """Convenience function to send error notification"""
    notifier = get_email_notifier()
    return notifier.send_error_notification(error_message, error_details, context)

def send_completion_notification(total_records: int, patterns_completed: int, 
                               execution_time: str, patterns_failed: int = 0, patterns_no_results: int = 0) -> bool:
    """Convenience function to send completion notification"""
    notifier = get_email_notifier()
    return notifier.send_completion_notification(total_records, patterns_completed, execution_time, patterns_failed, patterns_no_results) 