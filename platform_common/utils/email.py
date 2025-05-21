from ..stream.kafka import KafkaConnector
from ..config_loader import get_config
from ..utils import logger
def send_email_notification(data):
    """
    This function acts as a wrapper for email notifications.
    It facilitates sending notifications to an email service via Kafka and ensures that email notifications are delivered to the intended recipients.
    Example usage:
    kafka_msg = {
        "to": user_email,
        "subject": f"Albanero-M3 Spec | {mapping_info}: Question status changed to {updated_status}",
        "html": f"  There is an update in the question status.<br>
                    Question: <b>{question}</b> <br>
                    Updated-status: <b>{updated_status}<b> <br>
                    Program-name: {document["program"]}<br>
                    Click <a href={mapping_url}>here</a> to view details.
                ",
    }
    """
    logger.debug(f"Sending email notification: {data}")
    KafkaConnector.send_message(
        get_config("ALBANERO_KAFKA_TOPIC_EMAIL_NOTIFICATION"), data
    )
Uncovered code
    logger.info("Email notification sent successfully.")