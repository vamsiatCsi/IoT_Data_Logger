import logging
import re
from typing import Any, Dict
from datetime import datetime
from .base_mapper import DataTransformation


class SchemaRenameTransformation(DataTransformation):
    """Rename columns based on mapping configuration"""
    
    def __init__(self, column_mappings: Dict[str, str]):
        self.column_mappings = column_mappings  # {source_column: target_column}
    
    def validate(self, data: Any) -> bool:
        return isinstance(data, dict)
    
    def transform(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Rename columns according to mapping"""
        transformed = {}
        
        for source_key, value in data.items():
            target_key = self.column_mappings.get(source_key, source_key)
            transformed[target_key] = value
        
        # Add any columns that weren't in source but are required
        for source_key, target_key in self.column_mappings.items():
            if source_key not in data and target_key not in transformed:
                transformed[target_key] = None
        
        return transformed

class DataTypeConversionTransformation(DataTransformation):
    """Convert data types based on target schema"""
    
    def __init__(self, type_mappings: Dict[str, str]):
        self.type_mappings = type_mappings  # {column: target_type}
        self.converters = {
            'int': int,
            'float': float,
            'str': str,
            'bool': lambda x: bool(x) if x is not None else False,
            'datetime': self._convert_to_datetime
        }
    
    def validate(self, data: Any) -> bool:
        return isinstance(data, dict)
    
    def transform(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data types for mapped columns"""
        transformed = data.copy()
        
        for column, target_type in self.type_mappings.items():
            if column in transformed and transformed[column] is not None:
                try:
                    converter = self.converters.get(target_type)
                    if converter:
                        transformed[column] = converter(transformed[column])
                except (ValueError, TypeError) as e:
                    # Log conversion error but keep original value
                    logging.warning(f"Failed to convert {column} to {target_type}: {e}")
        
        return transformed
    
    def _convert_to_datetime(self, value: Any) -> datetime:
        """Convert various formats to datetime"""
        if isinstance(value, datetime):
            return value
        elif isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)
        elif isinstance(value, str):
            # Try common datetime formats
            formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d',
                '%d/%m/%Y %H:%M:%S'
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Unable to parse datetime: {value}")
        else:
            raise ValueError(f"Unsupported datetime type: {type(value)}")

class ValidationTransformation(DataTransformation):
    """Validate data against business rules"""
    
    def __init__(self, validation_rules: Dict[str, Dict[str, Any]]):
        self.validation_rules = validation_rules
        # Example rules: {"temperature": {"min": -50, "max": 100, "type": "float"}}
    
    def validate(self, data: Any) -> bool:
        return isinstance(data, dict)
    
    def transform(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and potentially clean data"""
        validated = data.copy()
        validation_errors = []
        
        for column, rules in self.validation_rules.items():
            if column not in validated:
                continue
            
            value = validated[column]
            
            # Check range validation
            if 'min' in rules and value is not None and value < rules['min']:
                validation_errors.append(f"{column} value {value} below minimum {rules['min']}")
                validated[column] = rules['min']  # Clamp to minimum
            
            if 'max' in rules and value is not None and value > rules['max']:
                validation_errors.append(f"{column} value {value} above maximum {rules['max']}")
                validated[column] = rules['max']  # Clamp to maximum
            
            # Check required fields
            if rules.get('required', False) and (value is None or value == ''):
                validation_errors.append(f"{column} is required but missing")
        
        if validation_errors:
            context['validation_errors'] = validation_errors
            logging.warning(f"Validation issues: {validation_errors}")
        
        return validated

class TagPathMappingTransformation(DataTransformation):
    """Map device tag paths to standardized column names"""
    
    def __init__(self, tag_mappings: Dict[str, str]):
        self.tag_mappings = tag_mappings  # {tag_path: column_name}
    
    def validate(self, data: Any) -> bool:
        return isinstance(data, dict)
    
    def transform(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Transform tag-based data to column-based data"""
        transformed = {}
        
        for tag_path, value in data.items():
            column_name = self.tag_mappings.get(tag_path)
            if column_name:
                transformed[column_name] = value
            else:
                # Keep unmapped tags with sanitized names
                sanitized_name = self._sanitize_tag_name(tag_path)
                transformed[sanitized_name] = value
        
        return transformed
    
    def _sanitize_tag_name(self, tag_path: str) -> str:
        """Convert tag path to valid column name"""
        # Replace invalid characters and convert to snake_case
        sanitized = re.sub(r'[^\w\s]', '_', tag_path)
        sanitized = re.sub(r'\s+', '_', sanitized)
        sanitized = re.sub(r'_+', '_', sanitized)
        return sanitized.lower().strip('_')