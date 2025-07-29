# src/core/__init__.py
"""Core infrastructure components for the IoT data logging system."""

# Import order: most fundamental to most specific

from .exceptions import ConfigurationError, ProtocolError

from .patterns.state_machine import StateMachine, ClientState
from .patterns.circuit_breaker import CircuitBreaker, BreakerConfig
from .patterns.observer import (
    ChangeType,
    ConfigurationObserver, 
    ConfigurationChangeEvent,
    get_event_bus
)


__all__ = [
    "StateMachine",
    "CircuitBreaker",
    "ConfigurationObserver",
    "IoTDataLoggerError",        # make available at package root
    "ConfigurationError",
    "ProtocolError",
    "TriggerError",
]

# Package metadata
__version__ = '1.0.0'
