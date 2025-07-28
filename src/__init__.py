"""Industrial IoT Data Logging System - Main Package"""

__version__ = '1.0.0'
__author__ = 'Your Team'
__description__ = 'Fault-tolerant IoT data logging with dynamic reconfiguration'

# Re-export main components for easy access
from .core import StateMachine, CircuitBreaker
from .models import Device, ProtocolConfig
from .services import FrappeService, DataLoggingOrchestrator

__all__ = [
    'StateMachine',
    'CircuitBreaker', 
    'Device',
    'ProtocolConfig',
    'FrappeService',
    'DataLoggingOrchestrator'
]
