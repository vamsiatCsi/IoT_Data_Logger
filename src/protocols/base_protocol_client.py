"""
Industrial Protocol Client Framework
Base abstract class and interfaces for industrial protocol clients
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Callable, Union
import asyncio
import logging
import signal
import time
from datetime import datetime
from enum import Enum
from contextlib import asynccontextmanager
import json


class ProtocolType(Enum):
    """Enumeration of supported protocol types."""
    MQTT = "mqtt"
    OPCUA = "opcua"


class ConnectionState(Enum):
    """Connection state enumeration."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


class LogLevel(Enum):
    """Logging level enumeration."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class ProtocolClientConfig:
    """Configuration class for protocol clients."""

    def __init__(self, 
                 protocol_type: ProtocolType,
                 connection_params: Dict[str, Any],
                 tags: List[Dict[str, Any]] = None,
                 metadata: Dict[str, Any] = None,
                 log_file: str = None,
                 max_retries: int = 5,
                 retry_delay: float = 1.0,
                 max_retry_delay: float = 60.0,
                 timeout: int = 30):
        self.protocol_type = protocol_type
        self.connection_params = connection_params
        self.tags = tags or []
        self.metadata = metadata or {}
        self.log_file = log_file
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.max_retry_delay = max_retry_delay
        self.timeout = timeout


class BaseProtocolClient(ABC):
    """
    Abstract base class for industrial protocol clients.

    Implements the Template Method pattern with common workflow and lifecycle management.
    Uses context manager protocol for proper resource management.
    """

    def __init__(self, config: ProtocolClientConfig):
        self.config = config
        self.logger = self._setup_logging()
        self.connection_state = ConnectionState.DISCONNECTED
        self.running = False
        self.retry_count = 0
        self.last_retry_time = 0
        self.data_callback: Optional[Callable] = None
        self.error_callback: Optional[Callable] = None

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger(f"{self.__class__.__name__}")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    # Template method - defines the algorithm skeleton
    async def start(self, data_callback: Callable = None, error_callback: Callable = None):
        """
        Template method that defines the standard client lifecycle.
        This is the main entry point that orchestrates the connection process.
        """
        self.data_callback = data_callback
        self.error_callback = error_callback
        self.running = True

        try:
            self.logger.info(f"Starting {self.config.protocol_type.value} client...")

            # Step 1: Validate configuration
            self._validate_config()

            # Step 2: Initialize client
            await self._initialize_client()

            # Step 3: Connect with retry logic
            await self._connect_with_retry()

            # Step 4: Setup subscriptions/monitoring
            await self._setup_monitoring()

            # Step 5: Start main processing loop
            await self._run_main_loop()

        except Exception as e:
            self.logger.error(f"Error in client lifecycle: {e}")
            if self.error_callback:
                await self._safe_callback(self.error_callback, e)
        finally:
            await self._cleanup()

    async def stop(self):
        """Stop the client gracefully."""
        self.logger.info("Stopping client...")
        self.running = False
        await self._cleanup()

    # Context manager protocol
    async def __aenter__(self):
        """Async context manager entry."""
        await self._initialize_client()
        await self._connect_with_retry()
        await self._setup_monitoring()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._cleanup()
        return False

    # Abstract methods that subclasses must implement (Strategy pattern)
    @abstractmethod
    async def _initialize_client(self):
        """Initialize the protocol-specific client."""
        pass

    @abstractmethod
    async def _connect(self):
        """Establish connection to the protocol server/broker."""
        pass

    @abstractmethod
    async def _disconnect(self):
        """Disconnect from the protocol server/broker."""
        pass

    @abstractmethod
    async def _setup_monitoring(self):
        """Setup subscriptions, topics, or monitoring configuration."""
        pass

    @abstractmethod
    async def _process_data(self) -> List[Dict[str, Any]]:
        """Process and return data from the protocol."""
        pass

    @abstractmethod
    def _validate_config(self):
        """Validate protocol-specific configuration."""
        pass

    # Common implementations that can be overridden
    async def _connect_with_retry(self):
        """Connect with exponential backoff retry strategy."""
        self.retry_count = 0

        while self.running and self.retry_count < self.config.max_retries:
            try:
                self.connection_state = ConnectionState.CONNECTING
                await self._connect()
                self.connection_state = ConnectionState.CONNECTED
                self.retry_count = 0
                self.logger.info("Successfully connected")
                return

            except Exception as e:
                self.retry_count += 1
                self.connection_state = ConnectionState.RECONNECTING

                if self.retry_count >= self.config.max_retries:
                    self.connection_state = ConnectionState.ERROR
                    raise ConnectionError(f"Failed to connect after {self.config.max_retries} attempts: {e}")

                # Exponential backoff with jitter
                delay = min(
                    self.config.retry_delay * (2 ** (self.retry_count - 1)),
                    self.config.max_retry_delay
                )

                self.logger.warning(f"Connection attempt {self.retry_count} failed: {e}. Retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)

    async def _run_main_loop(self):
        """Main processing loop template."""
        self.logger.info("Starting main processing loop...")

        while self.running:
            try:
                # Check connection health
                if self.connection_state != ConnectionState.CONNECTED:
                    await self._connect_with_retry()

                # Process data
                data = await self._process_data()

                # Handle data if callback is provided
                if data and self.data_callback:
                    await self._safe_callback(self.data_callback, data)

                # Log data if configured
                if data and self.config.log_file:
                    await self._log_data(data)

                # Small delay to prevent tight loop
                await asyncio.sleep(0.1)

            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
                if self.error_callback:
                    await self._safe_callback(self.error_callback, e)

                # Brief pause before retrying
                await asyncio.sleep(1.0)

    async def _safe_callback(self, callback: Callable, *args, **kwargs):
        """Safely execute callback functions."""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(*args, **kwargs)
            else:
                callback(*args, **kwargs)
        except Exception as e:
            self.logger.error(f"Error in callback: {e}")

    async def _log_data(self, data: List[Dict[str, Any]]):
        """Log data to file."""
        try:
            timestamp = datetime.now().isoformat()
            for item in data:
                log_entry = {
                    "timestamp": timestamp,
                    "protocol": self.config.protocol_type.value,
                    "data": item
                }

                # Write to file (in a real implementation, consider async file I/O)
                with open(self.config.log_file, 'a') as f:
                    f.write(json.dumps(log_entry) + "\n")

        except Exception as e:
            self.logger.error(f"Error logging data: {e}")

    async def _cleanup(self):
        """Cleanup resources."""
        try:
            self.logger.info("Cleaning up resources...")
            await self._disconnect()
            self.connection_state = ConnectionState.DISCONNECTED
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    # Utility methods
    def get_connection_state(self) -> ConnectionState:
        """Get current connection state."""
        return self.connection_state

    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self.connection_state == ConnectionState.CONNECTED

    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        return {
            "protocol_type": self.config.protocol_type.value,
            "connection_state": self.connection_state.value,
            "retry_count": self.retry_count,
            "running": self.running,
            "uptime": time.time() - getattr(self, '_start_time', time.time())
        }