from kafka import KafkaProducer, KafkaConsumer
import json
from ..utils import datetime_encoder
from ..config_loader import get_config
from ..exceptions.custom_exceptions import ExistingKafkaConnection
from ..utils import logger, secrets_manager
from ..enums import Environments
import os
import tempfile
CONSUMER_POLL_INTERVAL_SEC = 0.5
kafka_producer = None
def create_temp_file(content: str) -> str:
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(content.encode("utf-8"))
        temp_file_name = temp_file.name
        return temp_file_name
def get_kafka_cert_path() -> str:
    platform_env = os.environ.get("PLATFORM_ENVIRONMENT_NAME")
    secret_name = f"{platform_env}/kafka-cert"
    kafka_secret = secrets_manager.get_secret_by_name(secret_name)
    cert_file_path = create_temp_file(kafka_secret["cert"])
    return cert_file_path
def get_kafka_config() -> dict:
    is_dev_mode = (
        os.environ.get("ALBANERO_SERVICE_ENVIRONMENT") == Environments.DEVELOPMENT
    )
    try:
        if is_dev_mode:
            kafka_brokers = get_config("ALBANERO_KAFKA_BROKERS").split(",")
            return {
                "bootstrap_servers": kafka_brokers,
            }
        else:
            kafka_brokers = get_config("PLATFORM_KAFKA_CLUSTER_BROKERS").split(",")
            security_protocol = get_config("PLATFORM_KAFKA_CLUSTER_SECURITY_PROTOCOL")
            sasl_mechanism = get_config("PLATFORM_KAFKA_CLUSTER_SASL_MECHANISM")
            sasl_plain_username = get_config("PLATFORM_KAFKA_CLUSTER_SASL_USERNAME")
            sasl_plain_password = get_config("PLATFORM_KAFKA_CLUSTER_SASL_PASSWORD")
            cert_file_path = get_kafka_cert_path()
            return {
                "bootstrap_servers": kafka_brokers,
                "security_protocol": security_protocol,
                "sasl_mechanism": sasl_mechanism,
                "sasl_plain_username": sasl_plain_username,
                "sasl_plain_password": sasl_plain_password,
                "ssl_cafile": cert_file_path,
            }
    except Exception as err:
        if isinstance(err, KeyError):
            kafka_brokers = get_config("ALBANERO_KAFKA_BROKERS").split(",")
            return {
                "bootstrap_servers": kafka_brokers,
            }
        else:
            raise err
class KafkaConnector:
    """
    A singleton class for connecting to and sending messages to Kafka.
    This class provides a singleton instance of a Kafka, ensuring that only one instance is created
    and used throughout the application. It utilizes the KafkaProducer to establish a connection to Kafka brokers
    and send messages to specified topics.
    Usage:
        Kafka.send_message(topic, message)
    Attributes:
        producer (KafkaProducer): The KafkaProducer instance used for sending messages to Kafka brokers.
    Methods:
        send_message(topic, message):
            Sends a message to the specified topic in Kafka.
        consumer(topics, process_status_message):
            Listens to all the messages and processes them accordingly in the function process_status_message.
    Note:
        It's recommended to carefully consider the use of the Singleton pattern and potential alternatives,
        as it can introduce global state and make code harder to test and reason about.
    """
    producer = None
    def __init__(self):
        if KafkaConnector.producer is not None:
            raise ExistingKafkaConnection()
        else:
            kafka_config = get_kafka_config()
            kafka_producer = KafkaProducer(
                value_serializer=lambda v: json.dumps(
                    v, cls=datetime_encoder.DateTimeEncoder
                ).encode("utf-8"),
                **kafka_config,
            )
            KafkaConnector.producer = kafka_producer
            logger.debug("Kafka connector established.")
    @staticmethod
    def send_message(topic, message, key=None):
        if KafkaConnector.producer is None:
            KafkaConnector()
        # Kafka producer expects the key to be of type bytes, bytearray, memoryview, or None. Hence changing key to bytes.
        if key is not None:
            key = key.encode("utf-8")
        KafkaConnector.producer.send(topic, value=message, key=key).get(timeout=60)
        logger.debug(f"Message sent to topic '{topic}': {message}")
    @staticmethod
    def get_consumer(
        topics: list,
        consumer_config: dict,
    ):
        """Consume messages from Kafka topics.
        Args:
            topics (list): List of topics to which the consumer subscribes.
            consumer_config (dict): Dictionary containing Kafka consumer configuration parameters.
                Important Parameters:
                    - 'group_id' (str): A unique identifier for the consumer group.
                    - 'auto_offset_reset' (str): Determines where to start consuming if no offset is stored.
                    - 'enable_auto_commit' (bool): If True, consumer offsets are committed automatically.
                    - 'auto_commit_interval_ms' (int): Interval at which offsets are committed.
                    - 'key_deserializer' (callable): Deserializer for the message key.
                    - 'value_deserializer' (callable): Deserializer for the message value.
                    - 'max_poll_records' (int): Maximum number of records to poll in each call.
                    - 'max_poll_interval_ms' (int): Maximum time between poll calls.
                    - 'session_timeout_ms' (int): Timeout to detect consumer failures.
        Returns:
            consumer
        """
        kafka_config = get_kafka_config()
        consumer_config.update(kafka_config)
        consumer = KafkaConsumer(*topics, **consumer_config)
        return consumer
