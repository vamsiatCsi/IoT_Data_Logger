import time
from typing import Any, Dict, Optional
from .base_trigger import TriggerStrategy

class TimeBasedTriggerStrategy(TriggerStrategy):
    """Time-interval based trigger implementation"""
    
    def __init__(self, trigger_config: Dict[str, Any]):
        super().__init__(trigger_config)
        self.interval_seconds = trigger_config.get("interval_seconds", 60)
        self.last_trigger_time: Optional[float] = None
    
    async def should_trigger(self, data_sample: Dict[str, Any]) -> bool:
        current_time = time.time()
        
        if self.last_trigger_time is None:
            self.last_trigger_time = current_time
            self.execution_count += 1
            return True
        
        if (current_time - self.last_trigger_time) >= self.interval_seconds:
            self.last_trigger_time = current_time
            self.execution_count += 1
            return True
        
        return False
    
    def get_next_check_interval(self) -> float:
        if self.last_trigger_time is None:
            return 0.1  # Check immediately
        
        elapsed = time.time() - self.last_trigger_time
        remaining = max(0.1, self.interval_seconds - elapsed)
        return remaining
    
    def reset_state(self) -> None:
        self.last_trigger_time = None
        self.execution_count = 0