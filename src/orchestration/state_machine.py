from enum import Enum, auto
from typing import Dict, Set, Optional
import logging

class OrchestrationState(Enum):
    INITIALIZING = auto()
    DEVICE_DISCOVERY = auto()
    CLIENT_CREATION = auto()
    CONFIGURATION_ENRICHMENT = auto()
    LOGGING_STARTUP = auto()
    OPERATIONAL = auto()
    ERROR_RECOVERY = auto()
    SHUTDOWN = auto()

class OrchestrationStateMachine:
    """Manages the overall orchestration state transitions"""
    
    def __init__(self):
        self.current_state = OrchestrationState.INITIALIZING
        self.logger = logging.getLogger(self.__class__.__name__)
        self.valid_transitions = {
            OrchestrationState.INITIALIZING: {OrchestrationState.DEVICE_DISCOVERY, OrchestrationState.SHUTDOWN},
            OrchestrationState.DEVICE_DISCOVERY: {OrchestrationState.CLIENT_CREATION, OrchestrationState.ERROR_RECOVERY},
            OrchestrationState.CLIENT_CREATION: {OrchestrationState.CONFIGURATION_ENRICHMENT, OrchestrationState.ERROR_RECOVERY},
            OrchestrationState.CONFIGURATION_ENRICHMENT: {OrchestrationState.LOGGING_STARTUP, OrchestrationState.ERROR_RECOVERY},
            OrchestrationState.LOGGING_STARTUP: {OrchestrationState.OPERATIONAL, OrchestrationState.ERROR_RECOVERY},
            OrchestrationState.OPERATIONAL: {OrchestrationState.ERROR_RECOVERY, OrchestrationState.SHUTDOWN},
            OrchestrationState.ERROR_RECOVERY: {OrchestrationState.DEVICE_DISCOVERY, OrchestrationState.SHUTDOWN},
            OrchestrationState.SHUTDOWN: set()
        }
    
    def can_transition_to(self, new_state: OrchestrationState) -> bool:
        return new_state in self.valid_transitions.get(self.current_state, set())
    
    def transition_to(self, new_state: OrchestrationState) -> bool:
        if self.can_transition_to(new_state):
            self.logger.info(f"State transition: {self.current_state.name} -> {new_state.name}")
            self.current_state = new_state
            return True
        else:
            self.logger.error(f"Invalid state transition: {self.current_state.name} -> {new_state.name}")
            return False