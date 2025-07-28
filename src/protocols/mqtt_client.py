"""
MQTT Protocol Client Implementation
Refactored modular MQTT client that inherits from BaseProtocolClient
"""

import asyncio
import json
import queue
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
import paho.mqtt.client as mqtt
from src.protocols.base_protocol_client import BaseProtocolClient, ProtocolClientConfig, ProtocolType, ConnectionState, LogLevel as logging


class MQTTClient(BaseProtocolClient):
    """
    Modular MQTT client implementation.

    Features:
    - Dynamic topic subscription management
    - Reconnection handling with exponential backoff
    - Message filtering and processing
    - QoS support
    - Structured logging
    - Thread-safe message handling
    """

    def __init__(self, config: ProtocolClientConfig):
        if config.protocol_type != ProtocolType.MQTT:
            raise ValueError("Config must be for MQTT protocol")

        super().__init__(config)

        # MQTT-specific attributes
        self.client: Optional[mqtt.Client] = None
        self.message_queue = queue.Queue()
        self.subscribed_topics = set()
        self.message_handlers: Dict[str, Callable] = {}
        self.loop = None
        self.mqtt_thread = None

        # Parse MQTT-specific configuration
        self._parse_mqtt_config()

    def _parse_mqtt_config(self):
        """Parse MQTT-specific configuration parameters."""
        params = self.config.connection_params

        self.broker_host = params.get('host', 'localhost')
        self.broker_port = params.get('port', 1883)
        self.client_id = params.get('client_id', f"mqtt_client_{int(datetime.now().timestamp())}")
        self.clean_session = params.get('clean_session', True)
        self.keepalive = params.get('keepalive', 60)
        self.qos = params.get('qos', 0)
        self.retain = params.get('retain', False)

        # Authentication
        self.username = params.get('username')
        self.password = params.get('password')

        # TLS/SSL
        self.use_tls = params.get('use_tls', False)
        self.ca_certs = params.get('ca_certs')
        self.certfile = params.get('certfile')
        self.keyfile = params.get('keyfile')

        # Topics from tags configuration
        self.topics = []
        for tag in self.config.tags:
            topic_info = {
                'topic': tag.get('topic', tag.get('tag')),
                'qos': tag.get('qos', self.qos),
                'handler': tag.get('handler'),
                'filter': tag.get('filter')
            }
            self.topics.append(topic_info)

    def _validate_config(self):
        """Validate MQTT-specific configuration."""
        if not self.broker_host:
            raise ValueError("MQTT broker host is required")

        if not isinstance(self.broker_port, int) or not (1 <= self.broker_port <= 65535):
            raise ValueError("MQTT broker port must be a valid port number")

        if not self.topics:
            self.logger.warning("No topics configured for subscription")

    async def _initialize_client(self):
        """Initialize the MQTT client."""
        try:
            # Create MQTT client instance
            self.client = mqtt.Client(
                client_id=self.client_id,
                clean_session=self.clean_session,
                protocol=mqtt.MQTTv311
            )

            # Set authentication if provided
            if self.username and self.password:
                self.client.username_pw_set(self.username, self.password)

            # Configure TLS if enabled
            if self.use_tls:
                self.client.tls_set(
                    ca_certs=self.ca_certs,
                    certfile=self.certfile,
                    keyfile=self.keyfile
                )

            # Set callbacks
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message
            self.client.on_subscribe = self._on_subscribe
            self.client.on_unsubscribe = self._on_unsubscribe
            self.client.on_log = self._on_log

            self.logger.info(f"MQTT client initialized with ID: {self.client_id}")

        except Exception as e:
            self.logger.error(f"Failed to initialize MQTT client: {e}")
            raise

    async def _connect(self):
        """Establish connection to MQTT broker."""
        try:
            self.logger.info(f"Connecting to MQTT broker at {self.broker_host}:{self.broker_port}")

            # Connect to broker
            result = self.client.connect(
                host=self.broker_host,
                port=self.broker_port,
                keepalive=self.keepalive
            )

            if result != mqtt.MQTT_ERR_SUCCESS:
                raise ConnectionError(f"MQTT connection failed with code: {result}")

            # Start the network loop in a separate thread
            self.client.loop_start()

            # Wait for connection to be established
            connection_timeout = self.config.timeout
            start_time = asyncio.get_event_loop().time()

            while not self.client.is_connected():
                if asyncio.get_event_loop().time() - start_time > connection_timeout:
                    raise TimeoutError(f"Connection timeout after {connection_timeout}s")
                await asyncio.sleep(0.1)

            self.logger.info("Successfully connected to MQTT broker")

        except Exception as e:
            self.logger.error(f"MQTT connection failed: {e}")
            if self.client:
                self.client.loop_stop()
            raise

    async def _disconnect(self):
        """Disconnect from MQTT broker."""
        try:
            if self.client and self.client.is_connected():
                self.logger.info("Disconnecting from MQTT broker")
                self.client.disconnect()
                self.client.loop_stop()
                self.subscribed_topics.clear()
                self.logger.info("Disconnected from MQTT broker")
        except Exception as e:
            self.logger.error(f"Error during MQTT disconnection: {e}")

    async def _setup_monitoring(self):
        """Setup topic subscriptions."""
        try:
            for topic_info in self.topics:
                await self._subscribe_to_topic(
                    topic_info['topic'], 
                    topic_info['qos'],
                    topic_info.get('handler')
                )

            self.logger.info(f"Setup monitoring for {len(self.topics)} topics")

        except Exception as e:
            self.logger.error(f"Error setting up monitoring: {e}")
            raise

    async def _process_data(self) -> List[Dict[str, Any]]:
        """Process messages from the queue."""
        processed_data = []

        try:
            # Process all messages in the queue
            while not self.message_queue.empty():
                try:
                    message_data = self.message_queue.get_nowait()
                    processed_message = await self._process_message(message_data)
                    if processed_message:
                        processed_data.append(processed_message)
                except queue.Empty:
                    break
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")

        except Exception as e:
            self.logger.error(f"Error in data processing: {e}")

        return processed_data

    async def _process_message(self, message_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single MQTT message."""
        try:
            topic = message_data['topic']
            payload = message_data['payload']
            qos = message_data['qos']
            retain = message_data['retain']
            timestamp = message_data['timestamp']

            # Try to parse JSON payload
            try:
                if isinstance(payload, bytes):
                    payload_str = payload.decode('utf-8')
                else:
                    payload_str = str(payload)

                # Try to parse as JSON
                try:
                    parsed_payload = json.loads(payload_str)
                except json.JSONDecodeError:
                    # If not JSON, use string payload
                    parsed_payload = payload_str

            except Exception as e:
                self.logger.warning(f"Error decoding payload: {e}")
                parsed_payload = str(payload)

            # Create processed message
            processed_message = {
                'topic': topic,
                'payload': parsed_payload,
                'qos': qos,
                'retain': retain,
                'timestamp': timestamp,
                'device_id': self.config.metadata.get('device_id', 'unknown'),
                **self.config.metadata
            }

            # Apply topic-specific processing if configured
            topic_handler = self.message_handlers.get(topic)
            if topic_handler:
                processed_message = await self._safe_callback(topic_handler, processed_message)

            return processed_message

        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            return None

    # MQTT Event Callbacks
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when client connects to broker."""
        if rc == 0:
            self.logger.info(f"Connected to MQTT broker with flags: {flags}")
            self.connection_state = ConnectionState.CONNECTED
        else:
            error_messages = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorised"
            }
            error_msg = error_messages.get(rc, f"Connection failed with code {rc}")
            self.logger.error(error_msg)
            self.connection_state = ConnectionState.ERROR

    def _on_disconnect(self, client, userdata, rc):
        """Callback for when client disconnects from broker."""
        if rc != 0:
            self.logger.warning(f"Unexpected disconnection from MQTT broker (code: {rc})")
            self.connection_state = ConnectionState.DISCONNECTED
        else:
            self.logger.info("Disconnected from MQTT broker")
            self.connection_state = ConnectionState.DISCONNECTED

    def _on_message(self, client, userdata, msg):
        """Callback for when a message is received."""
        try:
            message_data = {
                'topic': msg.topic,
                'payload': msg.payload,
                'qos': msg.qos,
                'retain': msg.retain,
                'timestamp': datetime.now().isoformat()
            }

            # Add to processing queue
            self.message_queue.put(message_data)

            # Log message reception (optional, can be disabled for high-throughput scenarios)
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"Received message on topic '{msg.topic}': {len(msg.payload)} bytes")

        except Exception as e:
            self.logger.error(f"Error handling received message: {e}")

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        """Callback for when subscription is acknowledged."""
        self.logger.info(f"Subscription acknowledged with QoS: {granted_qos}")

    def _on_unsubscribe(self, client, userdata, mid):
        """Callback for when unsubscription is acknowledged."""
        self.logger.info(f"Unsubscription acknowledged for message ID: {mid}")

    def _on_log(self, client, userdata, level, buf):
        """Callback for MQTT client logging."""
        # Map MQTT log levels to Python logging levels
        level_map = {
            mqtt.MQTT_LOG_DEBUG: logging.DEBUG,
            mqtt.MQTT_LOG_INFO: logging.INFO,
            mqtt.MQTT_LOG_NOTICE: logging.INFO,
            mqtt.MQTT_LOG_WARNING: logging.WARNING,
            mqtt.MQTT_LOG_ERR: logging.ERROR
        }

        python_level = level_map.get(level, logging.INFO)
        self.logger.log(python_level, f"MQTT: {buf}")

    # Public methods for dynamic topic management
    async def _subscribe_to_topic(self, topic: str, qos: int = 0, handler: Callable = None):
        """Subscribe to a specific topic."""
        try:
            if not self.client or not self.client.is_connected():
                raise ConnectionError("MQTT client is not connected")

            result, mid = self.client.subscribe(topic, qos)

            if result != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Failed to subscribe to topic '{topic}': {result}")

            self.subscribed_topics.add(topic)

            if handler:
                self.message_handlers[topic] = handler

            self.logger.info(f"Subscribed to topic '{topic}' with QoS {qos}")

        except Exception as e:
            self.logger.error(f"Error subscribing to topic '{topic}': {e}")
            raise

    async def unsubscribe_from_topic(self, topic: str):
        """Unsubscribe from a specific topic."""
        try:
            if not self.client or not self.client.is_connected():
                raise ConnectionError("MQTT client is not connected")

            result, mid = self.client.unsubscribe(topic)

            if result != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Failed to unsubscribe from topic '{topic}': {result}")

            self.subscribed_topics.discard(topic)
            self.message_handlers.pop(topic, None)

            self.logger.info(f"Unsubscribed from topic '{topic}'")

        except Exception as e:
            self.logger.error(f"Error unsubscribing from topic '{topic}': {e}")
            raise

    async def publish_message(self, topic: str, payload: Any, qos: int = 0, retain: bool = False):
        """Publish a message to a topic."""
        try:
            if not self.client or not self.client.is_connected():
                raise ConnectionError("MQTT client is not connected")

            # Serialize payload if necessary
            if isinstance(payload, (dict, list)):
                payload = json.dumps(payload)
            elif not isinstance(payload, (str, bytes)):
                payload = str(payload)

            result = self.client.publish(topic, payload, qos, retain)

            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Failed to publish message to topic '{topic}': {result.rc}")

            self.logger.info(f"Published message to topic '{topic}'")
            return result

        except Exception as e:
            self.logger.error(f"Error publishing message to topic '{topic}': {e}")
            raise

    def get_subscribed_topics(self) -> List[str]:
        """Get list of currently subscribed topics."""
        return list(self.subscribed_topics)

    def get_message_queue_size(self) -> int:
        """Get current message queue size."""
        return self.message_queue.qsize()