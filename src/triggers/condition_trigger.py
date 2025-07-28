from typing import Any, Dict, Optional
from .base_trigger import TriggerStrategy
import logging

class ConditionBasedTriggerStrategy(TriggerStrategy):
    """Condition-based trigger with edge detection"""
    
    def __init__(self, trigger_config: Dict[str, Any]):
        super().__init__(trigger_config)
        self.condition_expression = trigger_config.get("condition", "True")
        self.last_condition_state: Optional[bool] = None
        self.edge_type = trigger_config.get("edge_type", "both")  # "rising", "falling", "both"
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def should_trigger(self, data_sample: Dict[str, Any]) -> bool:
        try:
            # Evaluate condition safely
            current_state = self._evaluate_condition(data_sample)
            
            if self.last_condition_state is None:
                self.last_condition_state = current_state
                if current_state:  # Trigger on initial True state
                    self.execution_count += 1
                    return True
                return False
            
            # Edge detection logic
            edge_detected = self._detect_edge(self.last_condition_state, current_state)
            self.last_condition_state = current_state
            
            if edge_detected:
                self.execution_count += 1
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error evaluating condition '{self.condition_expression}': {e}")
            return False
    
    def _evaluate_condition(self, data_sample: Dict[str, Any]) -> bool:
        """Safely evaluate condition expression"""
        try:
            # Use safe evaluation - replace eval with ast.literal_eval or custom parser
            # For demonstration, using eval but in production use safer alternatives
            return bool(eval(self.condition_expression, {"__builtins__": {}}, data_sample))
        except:
            return False
    
    def _detect_edge(self, previous_state: bool, current_state: bool) -> bool:
        """Detect edge transitions based on configuration"""
        if self.edge_type == "rising":
            return not previous_state and current_state
        elif self.edge_type == "falling":
            return previous_state and not current_state
        else:  # "both"
            return previous_state != current_state
    
    def get_next_check_interval(self) -> float:
        return 1.0  # Check every second for condition changes
    
    def reset_state(self) -> None:
        self.last_condition_state = None
        self.execution_count = 0