"""Industrial IoT Data Logging System - Main Package"""

__version__ = '1.0.0'
__author__ = 'Your Team'
__description__ = 'Fault-tolerant IoT data logging with dynamic reconfiguration'

# Core patterns - most fundamental
from .core import StateMachine, CircuitBreaker, ConfigurationObserver

# Models - domain objects
from .models import Device, ProtocolConfig, LoggingTrigger, ColumnMapping

# Services - business logic
from .services import FrappeService

# New orchestration (preferred over old orchestration_service)
from .orchestration import NewOrchestrator as DataLoggingOrchestrator

# Protocols
from .protocols import ProtocolFactory

# Triggers
from .triggers import TriggerStrategyFactory

# Mapping
from .mapping import ColumnMapperFactory

__all__ = [
    # Core
    'StateMachine',
    'CircuitBreaker',
    'ConfigurationObserver',
    
    # Models  
    'Device',
    'ProtocolConfig',
    'LoggingTrigger',
    'ColumnMapping',
    
    # Services
    'FrappeService',
    'DataLoggingOrchestrator',  # Now points to new implementation
    
    # Factories
    'ProtocolFactory',
    'TriggerStrategyFactory', 
    'ColumnMapperFactory'
]
