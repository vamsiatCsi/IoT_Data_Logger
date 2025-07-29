"""
Centralised exception definitions for the Industrial IoT Data-Logging system.
All custom exceptions should inherit from IoTDataLoggerError.
"""

class IoTDataLoggerError(Exception):
    """Base class for every custom exception thrown by this project."""

class ConfigurationError(IoTDataLoggerError):
    """Raised when configuration files or environment variables are invalid."""

class ProtocolError(IoTDataLoggerError):
    """Generic failure inside a protocol client (OPC-UA, MQTT, …)."""

class TriggerError(IoTDataLoggerError):
    """Raised when a trigger cannot be parsed or evaluated."""
