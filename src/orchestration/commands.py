from abc import ABC, abstractmethod
from typing import Any, Dict
import asyncio
import logging

class OrchestrationCommand(ABC):
    """Base class for orchestration commands"""
    
    def __init__(self, context: Dict[str, Any]):
        self.context = context
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    async def execute(self) -> Dict[str, Any]:
        """Execute the command and return results"""
        pass
    
    @abstractmethod
    async def rollback(self) -> None:
        """Rollback command effects if possible"""
        pass

class DeviceDiscoveryCommand(OrchestrationCommand):
    """Command to discover and validate devices from Frappe backend"""
    
    async def execute(self) -> Dict[str, Any]:
        frappe_service = self.context.get("frappe_service")
        if not frappe_service:
            raise ValueError("FrappeService not found in context")
        
        try:
            devices = await frappe_service.get_devices()
            active_devices = [d for d in devices if d.is_active]
            
            devices_by_protocol = {}
            for device in active_devices:
                protocol_type = device.protocol_type
                if protocol_type not in devices_by_protocol:
                    devices_by_protocol[protocol_type] = []
                devices_by_protocol[protocol_type].append(device)
            
            self.logger.info(f"Discovered {len(active_devices)} active devices across {len(devices_by_protocol)} protocols")
            
            return {
                "devices": active_devices,
                "devices_by_protocol": devices_by_protocol,
                "success": True
            }
        except Exception as e:
            self.logger.error(f"Device discovery failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def rollback(self) -> None:
        # Device discovery doesn't need rollback
        pass

class ClientCreationCommand(OrchestrationCommand):
    """Command to create protocol clients"""
    
    async def execute(self) -> Dict[str, Any]:
        devices_by_protocol = self.context.get("devices_by_protocol", {})
        frappe_service = self.context.get("frappe_service")
        
        created_clients = {}
        
        for protocol_type, devices in devices_by_protocol.items():
            try:
                # Get protocol configuration
                protocol_config = await frappe_service.get_protocol_config(protocol_type)
                
                # Create client using factory
                from src.protocols.factory.protocol_factory import ProtocolFactory
                client = ProtocolFactory.create_client(protocol_type, protocol_config, devices)
                
                created_clients[protocol_type] = client
                self.logger.info(f"Created client for protocol: {protocol_type}")
                
            except Exception as e:
                self.logger.error(f"Failed to create client for {protocol_type}: {e}")
                # Rollback previously created clients
                await self._cleanup_clients(created_clients)
                return {"success": False, "error": str(e)}
        
        return {
            "clients": created_clients,
            "success": True
        }
    
    async def rollback(self) -> None:
        clients = self.context.get("clients", {})
        await self._cleanup_clients(clients)
    
    async def _cleanup_clients(self, clients: Dict[str, Any]):
        for protocol_type, client in clients.items():
            try:
                if hasattr(client, 'disconnect'):
                    await client.disconnect()
                self.logger.info(f"Cleaned up client: {protocol_type}")
            except Exception as e:
                self.logger.error(f"Error cleaning up client {protocol_type}: {e}")

class ConfigurationEnrichmentCommand(OrchestrationCommand):
    """Command to enrich clients with trigger and mapping configurations"""
    
    async def execute(self) -> Dict[str, Any]:
        clients = self.context.get("clients", {})
        frappe_service = self.context.get("frappe_service")
        
        enriched_clients = {}
        
        for protocol_type, client in clients.items():
            try:
                # Get devices for this client
                devices = getattr(client, 'devices', [])
                device_configs = {}
                
                for device in devices:
                    # Fetch trigger and mapping configurations
                    trigger_config = await frappe_service.get_logging_trigger(device.device_id)
                    column_mappings = await frappe_service.get_column_mapping(device.device_id)
                    
                    if trigger_config and column_mappings:
                        # Create trigger strategies
                        from src.triggers.trigger_factory import TriggerStrategyFactory
                        strategies = TriggerStrategyFactory.create_strategies(trigger_config)
                        
                        device_configs[device.device_id] = {
                            "device": device,
                            "trigger_strategies": strategies,
                            "column_mappings": column_mappings
                        }
                
                # Enrich client with configurations
                if hasattr(client, 'set_device_configurations'):
                    client.set_device_configurations(device_configs)
                    enriched_clients[protocol_type] = client
                    self.logger.info(f"Enriched client {protocol_type} with {len(device_configs)} device configs")
                
            except Exception as e:
                self.logger.error(f"Failed to enrich client {protocol_type}: {e}")
                return {"success": False, "error": str(e)}
        
        return {
            "enriched_clients": enriched_clients,
            "success": True
        }
    
    async def rollback(self) -> None:
        # Reset client configurations
        clients = self.context.get("clients", {})
        for client in clients.values():
            if hasattr(client, 'clear_device_configurations'):
                client.clear_device_configurations()