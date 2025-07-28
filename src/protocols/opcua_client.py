"""
OPC UA Protocol Client Implementation
Refactored modular OPC UA client that inherits from BaseProtocolClient
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from asyncua import Client, ua
from src.protocols.base_protocol_client import BaseProtocolClient, ProtocolClientConfig, ProtocolType, ConnectionState, LogLevel as logging


class OPCUAClient(BaseProtocolClient):
    """
    Modular OPC UA client implementation.

    Features:
    - Node browsing and subscription management
    - Reconnection handling with exponential backoff
    - Dynamic tag management
    - Time-based and condition-based triggers
    - Structured data logging
    - Session management
    """

    def __init__(self, config: ProtocolClientConfig):
        if config.protocol_type != ProtocolType.OPCUA:
            raise ValueError("Config must be for OPC UA protocol")

        super().__init__(config)

        # OPC UA-specific attributes
        self.client: Optional[Client] = None
        self.subscription = None
        self.tag_nodes = {}
        self.node_to_tag = {}
        self.log_state = {}
        self.last_logged = {}
        self.trigger_config = {}

        # Parse OPC UA-specific configuration
        self._parse_opcua_config()

    def _parse_opcua_config(self):
        """Parse OPC UA-specific configuration parameters."""
        params = self.config.connection_params

        self.endpoint_url = params.get('endpoint_url', params.get('url'))
        self.root_path = params.get('root_path', '')
        self.session_timeout = params.get('session_timeout', 60000)
        self.subscription_period = params.get('subscription_period', 1000)

        # Security configuration
        self.security_policy = params.get('security_policy')
        self.security_mode = params.get('security_mode')
        self.certificate_path = params.get('certificate_path')
        self.private_key_path = params.get('private_key_path')

        # Authentication
        self.username = params.get('username')
        self.password = params.get('password')

        # Parse trigger configuration for tags
        for tag in self.config.tags:
            tag_name = tag.get('tag')
            if tag_name:
                self.trigger_config[tag_name] = {
                    'trigger': tag.get('trigger', 'time'),
                    'condition': tag.get('condition'),
                    'interval': tag.get('interval', 1)
                }

    def _validate_config(self):
        """Validate OPC UA-specific configuration."""
        if not self.endpoint_url:
            raise ValueError("OPC UA endpoint URL is required")

        if not self.endpoint_url.startswith('opc.tcp://'):
            raise ValueError("OPC UA endpoint URL must start with 'opc.tcp://'")

        if not self.config.tags:
            self.logger.warning("No tags configured for OPC UA client")

    async def _initialize_client(self):
        """Initialize the OPC UA client."""
        try:
            self.client = Client(url=self.endpoint_url, timeout=self.config.timeout)

            # Set client properties
            self.client.application_uri = "urn:OPCClient:IndustrialProtocolFramework"
            self.client.product_uri = "urn:OPCClient:IndustrialProtocolFramework"
            self.client.name = "IndustrialProtocolClient"
            self.client.session_timeout = self.session_timeout

            # Configure security if specified
            if self.security_policy and self.security_mode:
                self.client.set_security(
                    self.security_policy,
                    self.security_mode,
                    certificate_path=self.certificate_path,
                    private_key_path=self.private_key_path
                )

            # Set authentication if provided
            if self.username and self.password:
                self.client.set_user(self.username)
                self.client.set_password(self.password)

            self.logger.info(f"OPC UA client initialized for endpoint: {self.endpoint_url}")

        except Exception as e:
            self.logger.error(f"Failed to initialize OPC UA client: {e}")
            raise

    async def _connect(self):
        """Establish connection to OPC UA server."""
        try:
            self.logger.info(f"Connecting to OPC UA server at {self.endpoint_url}")

            # Connect with timeout
            await asyncio.wait_for(self.client.connect(), timeout=self.config.timeout)

            self.logger.info("Successfully connected to OPC UA server")

        except asyncio.TimeoutError:
            self.logger.error(f"OPC UA connection timeout after {self.config.timeout}s")
            raise
        except Exception as e:
            self.logger.error(f"OPC UA connection failed: {e}")
            raise

    async def _disconnect(self):
        """Disconnect from OPC UA server."""
        try:
            if self.subscription:
                await self.subscription.delete()
                self.subscription = None

            if self.client:
                await self.client.disconnect()
                self.logger.info("Disconnected from OPC UA server")
        except Exception as e:
            self.logger.error(f"Error during OPC UA disconnection: {e}")

    async def _setup_monitoring(self):
        """Setup OPC UA subscriptions and find tag nodes."""
        try:
            # Find all variable nodes for configured tags
            await self._discover_tag_nodes()

            if not self.tag_nodes:
                self.logger.warning("No valid tag nodes found for subscription")
                return

            # Create subscription
            handler = self._SubscriptionHandler(self)
            self.subscription = await self.client.create_subscription(
                period=self.subscription_period, 
                handler=handler
            )

            # Subscribe to all found nodes
            variable_nodes = list(self.tag_nodes.values())
            await self.subscription.subscribe_data_change(variable_nodes)

            # Initialize log state with current values
            for tag_name, node in self.tag_nodes.items():
                try:
                    value = await node.read_value()
                    self.log_state[tag_name] = value
                except Exception as e:
                    self.logger.warning(f"Error reading initial value for tag '{tag_name}': {e}")
                    self.log_state[tag_name] = None

            self.logger.info(f"Setup monitoring for {len(self.tag_nodes)} tags")

        except Exception as e:
            self.logger.error(f"Error setting up OPC UA monitoring: {e}")
            raise

    async def _discover_tag_nodes(self):
        """Discover and validate tag nodes on the OPC UA server."""
        try:
            # Start from the objects folder
            base_node = self.client.get_node(ua.ObjectIds.ObjectsFolder)

            # Navigate to root path if specified
            if self.root_path:
                base_node = await self._resolve_node_by_path(base_node, self.root_path)

            # Find all variable nodes
            all_variable_nodes = await self._find_variable_nodes(base_node)

            # Match configured tags with discovered nodes
            tag_names = {tag.get('tag') for tag in self.config.tags if tag.get('tag')}

            for node in all_variable_nodes:
                try:
                    display_name = (await node.read_display_name()).Text
                    if display_name in tag_names:
                        self.tag_nodes[display_name] = node
                        self.node_to_tag[node] = display_name
                        self.logger.info(f"Found tag node: '{display_name}'")
                except Exception as e:
                    self.logger.warning(f"Error processing node: {e}")

            if not self.tag_nodes:
                available_tags = [node.get_display_name().Text for node in all_variable_nodes]
                self.logger.warning(f"No matching tags found. Available tags: {available_tags}")

        except Exception as e:
            self.logger.error(f"Error discovering tag nodes: {e}")
            raise

    async def _find_variable_nodes(self, base_node, max_depth: int = 3, current_depth: int = 0):
        """Recursively find all variable nodes under a base node."""
        variable_nodes = []

        if current_depth >= max_depth:
            return variable_nodes

        try:
            children = await base_node.get_children()

            for child in children:
                try:
                    node_class = await child.read_node_class()

                    if node_class == ua.NodeClass.Variable:
                        variable_nodes.append(child)
                    elif node_class == ua.NodeClass.Object:
                        # Recursively search in object nodes
                        sub_variables = await self._find_variable_nodes(
                            child, max_depth, current_depth + 1
                        )
                        variable_nodes.extend(sub_variables)

                except Exception as e:
                    # Skip nodes that can't be read
                    continue

        except Exception as e:
            self.logger.warning(f"Error browsing node children: {e}")

        return variable_nodes

    async def _resolve_node_by_path(self, base_node, path: str):
        """Navigate to a node using a dot-separated path."""
        components = path.split('.')
        current_node = base_node

        for component in components:
            try:
                current_node = await current_node.get_child([component])
            except Exception as e:
                raise ValueError(f"Failed to resolve path component '{component}': {e}")

        return current_node

    async def _process_data(self) -> List[Dict[str, Any]]:
        """Process OPC UA data based on configured triggers."""
        processed_data = []
        now_ts = time.time()

        try:
            for tag_entry in self.config.tags:
                tag_name = tag_entry.get('tag')
                if not tag_name or tag_name not in self.log_state:
                    continue

                # Check if data should be logged based on trigger conditions
                if self._should_log_data(tag_name, tag_entry, now_ts):
                    value = self.log_state.get(tag_name)

                    processed_item = {
                        'tag': tag_name,
                        'value': value,
                        'timestamp': datetime.now().isoformat(),
                        'interval': tag_entry.get('interval', 1),
                        'device_id': self.config.metadata.get('device_id', 'unknown'),
                        **self.config.metadata
                    }

                    processed_data.append(processed_item)
                    self.last_logged[tag_name] = now_ts

                    self.logger.debug(f"Processed data for tag '{tag_name}': {value}")

        except Exception as e:
            self.logger.error(f"Error processing OPC UA data: {e}")

        return processed_data

    def _should_log_data(self, tag_name: str, tag_entry: Dict[str, Any], now_ts: float) -> bool:
        """Determine if data should be logged based on trigger configuration."""
        try:
            interval = tag_entry.get('interval', 1)
            trigger_type = self.trigger_config.get(tag_name, {}).get('trigger', 'time')
            condition = self.trigger_config.get(tag_name, {}).get('condition')

            last_log_time = self.last_logged.get(tag_name, 0)
            value = self.log_state.get(tag_name)

            # Check time window if configured
            start_time_str = tag_entry.get('StartTime')
            end_time_str = tag_entry.get('EndTime') or tag_entry.get('StopTime')

            if start_time_str and end_time_str:
                if not self._is_in_time_window(start_time_str, end_time_str):
                    return False

            # Apply trigger logic
            if trigger_type == 'always':
                return True
            elif trigger_type == 'time':
                return now_ts - last_log_time >= interval
            elif trigger_type == 'condition' and condition:
                try:
                    return eval(condition, {}, {'value': value})
                except Exception as e:
                    self.logger.warning(f"Error evaluating condition for '{tag_name}': {e}")
                    return False

            return False

        except Exception as e:
            self.logger.error(f"Error checking trigger for tag '{tag_name}': {e}")
            return False

    def _is_in_time_window(self, start_time_str: str, end_time_str: str) -> bool:
        """Check if current time is within the specified time window."""
        try:
            now_time = datetime.now().time()
            start_time = datetime.strptime(start_time_str, "%H:%M:%S").time()
            end_time = datetime.strptime(end_time_str, "%H:%M:%S").time()

            if start_time <= end_time:
                # Same day window
                return start_time <= now_time <= end_time
            else:
                # Overnight window
                return now_time >= start_time or now_time <= end_time

        except Exception as e:
            self.logger.error(f"Error checking time window: {e}")
            return True  # Default to allowing logging if time check fails

    class _SubscriptionHandler:
        """Handler for OPC UA subscription events."""

        def __init__(self, client_instance):
            self.client_instance = client_instance

        def datachange_notification(self, node, val, data):
            """Handle data change notifications from OPC UA server."""
            try:
                tag_name = self.client_instance.node_to_tag.get(node, "Unknown")
                self.client_instance.log_state[tag_name] = val

                # Log data change if debug logging is enabled
                if self.client_instance.logger.isEnabledFor(logging.DEBUG):
                    self.client_instance.logger.debug(
                        f"Data change for tag '{tag_name}': {val}"
                    )

            except Exception as e:
                self.client_instance.logger.error(f"Error handling data change notification: {e}")

    # Public methods for dynamic tag management
    async def add_tag(self, tag_name: str, interval: int = 1, **kwargs):
        """Dynamically add a new tag for monitoring."""
        try:
            # Add to configuration
            new_tag = {
                'tag': tag_name,
                'interval': interval,
                **kwargs
            }
            self.config.tags.append(new_tag)

            # Update trigger configuration
            self.trigger_config[tag_name] = {
                'trigger': kwargs.get('trigger', 'time'),
                'condition': kwargs.get('condition'),
                'interval': interval
            }

            # Rediscover nodes and update subscription
            await self._discover_tag_nodes()

            if tag_name in self.tag_nodes:
                # Add to existing subscription
                if self.subscription:
                    await self.subscription.subscribe_data_change([self.tag_nodes[tag_name]])

                # Initialize log state
                value = await self.tag_nodes[tag_name].read_value()
                self.log_state[tag_name] = value

                self.logger.info(f"Added tag '{tag_name}' to monitoring")
                return True
            else:
                self.logger.warning(f"Tag '{tag_name}' not found on server")
                return False

        except Exception as e:
            self.logger.error(f"Error adding tag '{tag_name}': {e}")
            return False

    async def remove_tag(self, tag_name: str):
        """Dynamically remove a tag from monitoring."""
        try:
            # Remove from configuration
            self.config.tags = [tag for tag in self.config.tags if tag.get('tag') != tag_name]

            # Remove from internal state
            if tag_name in self.tag_nodes:
                node = self.tag_nodes[tag_name]
                del self.tag_nodes[tag_name]
                del self.node_to_tag[node]

                # Note: OPC UA doesn't support removing individual items from subscription
                # In a full implementation, you might recreate the subscription

            self.log_state.pop(tag_name, None)
            self.last_logged.pop(tag_name, None)
            self.trigger_config.pop(tag_name, None)

            self.logger.info(f"Removed tag '{tag_name}' from monitoring")
            return True

        except Exception as e:
            self.logger.error(f"Error removing tag '{tag_name}': {e}")
            return False

    def get_monitored_tags(self) -> List[str]:
        """Get list of currently monitored tags."""
        return list(self.tag_nodes.keys())

    def get_tag_value(self, tag_name: str) -> Any:
        """Get current value of a specific tag."""
        return self.log_state.get(tag_name)

    def get_all_tag_values(self) -> Dict[str, Any]:
        """Get current values of all monitored tags."""
        return self.log_state.copy()