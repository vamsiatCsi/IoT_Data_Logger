from typing import Dict, Any, List
from .base_mapper import MappingPipeline, DataTransformation
from .transformations import (
    SchemaRenameTransformation,
    DataTypeConversionTransformation,
    ValidationTransformation,
    TagPathMappingTransformation
)

class ColumnMapperFactory:
    """Factory for creating column mapping pipelines"""
    
    @staticmethod
    def create_mapper(device_id: str, mapping_config: Dict[str, Any]) -> MappingPipeline:
        """Create mapping pipeline from configuration"""
        transformations: List[DataTransformation] = []
        
        # 1. Tag path to column mapping (if specified)
        if 'tag_mappings' in mapping_config:
            transformations.append(
                TagPathMappingTransformation(mapping_config['tag_mappings'])
            )
        
        # 2. Schema renaming
        if 'column_mappings' in mapping_config:
            transformations.append(
                SchemaRenameTransformation(mapping_config['column_mappings'])
            )
        
        # 3. Data type conversions
        if 'type_mappings' in mapping_config:
            transformations.append(
                DataTypeConversionTransformation(mapping_config['type_mappings'])
            )
        
        # 4. Validation rules
        if 'validation_rules' in mapping_config:
            transformations.append(
                ValidationTransformation(mapping_config['validation_rules'])
            )
        
        return MappingPipeline(transformations)

# src/mapping/exceptions.py
class MappingError(Exception):
    """Base exception for mapping operations"""
    pass

class ValidationError(MappingError):
    """Exception raised for validation failures"""
    pass

# Usage Example in Protocol Client:
"""
# In your protocol client (e.g., OPCUAClient)
from src.mapping.mapper_factory import ColumnMapperFactory

class OPCUAClient:
    def __init__(self, config):
        self.config = config
        self.device_mappers = {}  # device_id -> MappingPipeline
    
    def set_device_configurations(self, device_configs):
        for device_id, device_config in device_configs.items():
            # Create mapping pipeline for each device
            mapping_config = device_config['column_mappings']
            mapper = ColumnMapperFactory.create_mapper(device_id, mapping_config)
            self.device_mappers[device_id] = mapper
    
    async def process_device_data(self, device_id: str, raw_data: Dict[str, Any]):
        if device_id in self.device_mappers:
            mapper = self.device_mappers[device_id]
            try:
                mapped_data = await mapper.process(raw_data)
                return mapped_data
            except MappingError as e:
                self.logger.error(f"Mapping failed for device {device_id}: {e}")
                return None
        return raw_data
"""