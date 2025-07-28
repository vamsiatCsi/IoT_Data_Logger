from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import logging

from mapping.mapper_factory import MappingError

class DataTransformation(ABC):
    """Base class for data transformation steps"""
    
    @abstractmethod
    def transform(self, data: Any, context: Dict[str, Any]) -> Any:
        """Transform data and return result"""
        pass
    
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate if data can be transformed"""
        pass

class MappingPipeline:
    """Pipeline for executing data mapping transformations"""
    
    def __init__(self, transformations: List[DataTransformation]):
        self.transformations = transformations
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def process(self, input_data: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Process data through transformation pipeline"""
        if context is None:
            context = {}
        
        current_data = input_data.copy()
        
        for i, transformation in enumerate(self.transformations):
            try:
                if not transformation.validate(current_data):
                    self.logger.warning(f"Transformation {i} validation failed for data: {current_data}")
                    continue
                
                current_data = transformation.transform(current_data, context)
                self.logger.debug(f"Applied transformation {i}: {transformation.__class__.__name__}")
                
            except Exception as e:
                self.logger.error(f"Transformation {i} failed: {e}")
                raise MappingError(f"Pipeline failed at step {i}: {e}")
        
        return current_data