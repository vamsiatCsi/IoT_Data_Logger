# src/triggers/base_trigger.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import asyncio

class TriggerStrategy(ABC):
    """Abstract base class for all trigger strategies"""
    
    def __init__(self, trigger_config: Dict[str, Any]):
        self.config = trigger_config
        self.last_execution: Optional[datetime] = None
        self.execution_count: int = 0
    
    @abstractmethod
    async def should_trigger(self, data_sample: Dict[str, Any]) -> bool:
        """Determine if trigger condition is met"""
        pass
    
    @abstractmethod
    def get_next_check_interval(self) -> float:
        """Return seconds until next check should occur"""
        pass
    
    @abstractmethod
    def reset_state(self) -> None:
        """Reset trigger internal state"""
        pass
    
    def get_execution_metadata(self) -> Dict[str, Any]:
        """Return metadata about trigger execution"""
        return {
            "last_execution": self.last_execution,
            "execution_count": self.execution_count,
            "trigger_type": self.__class__.__name__
        }