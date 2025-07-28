from typing import Dict, List, Type, Any
from .base_trigger import TriggerStrategy
from .time_trigger import TimeBasedTriggerStrategy
from .condition_trigger import ConditionBasedTriggerStrategy

class TriggerStrategyFactory:
    """Factory for creating trigger strategy instances"""
    
    _strategy_registry: Dict[str, Type[TriggerStrategy]] = {
        "time_based": TimeBasedTriggerStrategy,
        "condition_based": ConditionBasedTriggerStrategy,
    }
    
    @classmethod
    def register_strategy(cls, strategy_type: str, strategy_class: Type[TriggerStrategy]):
        """Register new trigger strategy type"""
        cls._strategy_registry[strategy_type] = strategy_class
    
    @classmethod
    def create_strategies(cls, trigger_config: Dict[str, Any]) -> List[TriggerStrategy]:
        """Create list of trigger strategies from configuration"""
        strategies = []
        
        # Handle time-based triggers
        if "time_based_triggers" in trigger_config:
            for time_config in trigger_config["time_based_triggers"]:
                strategy = cls._strategy_registry["time_based"](time_config)
                strategies.append(strategy)
        
        # Handle condition-based triggers
        if "condition_based_triggers" in trigger_config:
            for condition_config in trigger_config["condition_based_triggers"]:
                strategy = cls._strategy_registry["condition_based"](condition_config)
                strategies.append(strategy)
        
        if not strategies:
            raise ValueError("No valid trigger strategies could be created from configuration")
        
        return strategies
    
    @classmethod
    def get_available_strategies(cls) -> List[str]:
        """Get list of available strategy types"""
        return list(cls._strategy_registry.keys())