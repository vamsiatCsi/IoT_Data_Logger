# src/core/__init__.py
"""Core infrastructure components for the IoT data logging system."""

# Import order: most fundamental to most specific
from .exceptions import (
    IoTDataLoggerError,
    ConfigurationError,
    ConnectionError
)

from .patterns.state_machine import StateMachine, ClientState
from .patterns.circuit_breaker import CircuitBreaker, BreakerConfig
from .patterns.observer import (
    ChangeType,
    ConfigurationObserver, 
    ConfigurationChangeEvent,
    get_event_bus
)

# Define public API
__all__ = [
    # Exceptions
    'IoTDataLoggerError',
    'ConfigurationError', 
    'ConnectionError',
    
    # Patterns
    'StateMachine',
    'ClientState',
    'CircuitBreaker',
    'BreakerConfig',
    'ChangeType',
    'ConfigurationObserver',
    'ConfigurationChangeEvent',
    'get_event_bus'
]

# Package metadata
__version__ = '1.0.0'
