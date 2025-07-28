# device_service.py - Updated to use FrappeService as dependency injection

from src.protocols.protocol_factory import ProtocolFactory
from src.protocols.base_protocol_client import ProtocolType, ProtocolClientConfig
from src.services.frappe_service import FrappeService
from collections import defaultdict
import asyncio
import traceback
import pprint
import json
import logging


class DeviceProtocolMapper:
    """
    Device Protocol Mapper that handles grouping devices by protocol
    and creating protocol clients. All Frappe interactions are delegated
    to the injected FrappeService instance.
    """
    
    def __init__(self, frappe_service: FrappeService):
        """
        Initialize with dependency injection of FrappeService
        
        Args:
            frappe_service: Singleton FrappeService instance
        """
        self.frappe = frappe_service
        self.log = logging.getLogger(self.__class__.__name__)
    
    def _extract_server_address(self, connection_params, protocol_used):
        """Helper to extract broker/server address for logging."""
        if protocol_used.upper() == "MQTT":
            return (
                connection_params.get('broker') or
                connection_params.get('host') or
                connection_params.get('hostname') or
                'unknown_broker'
            )
        elif protocol_used.upper() == "OPCUA":
            return (
                connection_params.get('endpoint_url') or
                connection_params.get('url') or
                'unknown_server'
            )
        else:
            return (
                connection_params.get('host') or
                connection_params.get('server') or
                connection_params.get('address') or
                'unknown_server'
            )
    
    def make_protocol_config(self, protocol_used, connection_params, tags, metadata):
        """Create protocol client config instance."""
        protocol_type_enum = (
            ProtocolType.MQTT if protocol_used.strip().upper() == "MQTT"
            else ProtocolType.OPCUA if protocol_used.strip().upper() == "OPCUA"
            else ProtocolType(protocol_used.strip().lower())
        )
        
        return ProtocolClientConfig(
            protocol_type=protocol_type_enum,
            connection_params=connection_params,
            tags=tags,
            metadata=metadata,
            log_file=None,
            max_retries=5,
            retry_delay=1.0,
            max_retry_delay=60.0,
            timeout=30,
        )
    
    async def create_protocol_clients(self):
        """
        Fetch devices via FrappeService
        Return list of successfully connected clients
        """
        self.log.info("🔧 Starting create_protocol_clients...")
        
        # Step 1: Fetch all devices using FrappeService
        self.log.info("📡 Fetching device details from Frappe backend...")
        try:
            devices = await self.frappe.get_devices()
            self.log.info(f"✅ Fetched {len(devices)} devices from backend")
        except Exception as e:
            self.log.error(f"❌ Failed to fetch devices: {e}")
            return []
        
        if not devices:
            self.log.warning("⚠️ No devices found in backend")
            return []
        
        # Step 2: Group devices by protocol configuration
        self.log.info("📊 Grouping devices by protocol configuration...")
        devices_by_protocol_type = defaultdict(list)
        protocol_types_map = {}
        
        for device in devices:
            if (hasattr(device, 'protocol_type') and hasattr(device, 'protocol_used') 
                and device.protocol_type and device.protocol_used and device.is_active):
                
                devices_by_protocol_type[device.protocol_type].append(device)
                protocol_types_map[device.protocol_type] = device.protocol_used
                self.log.debug(f" 📋 Device {getattr(device, 'device_id', 'n/a')}: "
                             f"{device.protocol_type} ({device.protocol_used})")
            else:
                self.log.warning(f" ⚠️ Skipping device {getattr(device, 'device_id', 'n/a')}: "
                               f"Missing protocol_type, protocol_used, or inactive")
        
        self.log.info(f"✅ Grouped devices into {len(devices_by_protocol_type)} protocol configurations")
        
        # Step 3: Create and connect protocol clients
        connected_clients = []
        
        for protocol_type, device_list in devices_by_protocol_type.items():
            self.log.info(f"\n🔌 Processing protocol configuration: {protocol_type}")
            
            try:
                # Step 3a: Fetch protocol configuration using FrappeService
                self.log.info(f" 📡 Fetching protocol configuration for {protocol_type}...")
                protocol_config = await self.frappe.get_protocol_config(protocol_type)
                
                # Step 3b: Extract connection parameters
                connection_params = protocol_config.connection_parameters or {}
                if isinstance(connection_params, str):
                    connection_params = json.loads(connection_params)
                
                protocol_used = protocol_types_map[protocol_type]
                self.log.info(f" 📋 Protocol: {protocol_used}")
                self.log.debug(f" 🔧 Connection params: {connection_params}")
                
                # Step 3c: Create metadata and empty tags
                device_metadata = {
                    "protocol_type": protocol_type,
                    "protocol_used": protocol_used,
                    "device_count": len(device_list),
                    "device_ids": [getattr(device, 'device_id', 'n/a') for device in device_list]
                }
                self.log.debug(f" 📝 Device metadata: {device_metadata}")
                
                empty_tags = []  # Placeholder empty tags list
                
                # Step 3d: Create protocol configuration object
                protocol_config_obj = self.make_protocol_config(
                    protocol_used, connection_params, empty_tags, device_metadata
                )
                self.log.debug(" 🏭 Creating protocol client configuration object:")
                self.log.debug(pprint.pformat(vars(protocol_config_obj)))
                
                # Step 3e: Use ProtocolFactory to create client
                self.log.info(f" 🏭 Creating protocol client using ProtocolFactory...")
                client = ProtocolFactory.create(
                    protocol_type=protocol_config_obj.protocol_type,
                    config=protocol_config_obj,
                    tags=empty_tags,
                    metadata=device_metadata,
                    trigger_config={}
                )
                self.log.info(f" ✅ Created {protocol_used} client for {protocol_type}")
                
                # Step 3f: Attempt to connect and validate connection
                self.log.info(f" 🔗 Attempting to connect {protocol_used} client...")
                connection_success = await self._connect_client(client)
                
                # Step 3g: Log connection result and add to successful connections
                if connection_success:
                    server_address = self._extract_server_address(connection_params, protocol_used)
                    if protocol_used.upper() == "MQTT":
                        self.log.info(f" ✅ Connected to MQTT broker at {server_address}")
                    elif protocol_used.upper() == "OPCUA":
                        self.log.info(f" ✅ Connected to OPC UA server at {server_address}")
                    else:
                        self.log.info(f" ✅ Connected to {protocol_used} server at {server_address}")
                    
                    connected_clients.append(client)
                else:
                    self.log.error(f" ❌ Failed to connect {protocol_used} client for {protocol_type}")
                    
            except Exception as e:
                self.log.error(f" ❌ Error creating/connecting client for {protocol_type}: {e}")
                self.log.debug(traceback.format_exc())
        
        # Step 4: Return results
        self.log.info(f"\n🎉 Protocol client creation complete!")
        self.log.info(f"✅ Successfully connected {len(connected_clients)} clients")
        self.log.info(f"❌ Failed to connect {len(devices_by_protocol_type) - len(connected_clients)} clients")
        
        return connected_clients
    
    async def _connect_client(self, client):
        """
        Attempt to connect a protocol client with different connection methods.
        
        Args:
            client: Protocol client instance
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if hasattr(client, 'connect_with_retry'):
                # For OPCUA clients with retry logic
                return await client.connect_with_retry(max_attempts=3)
            elif hasattr(client, 'connect'):
                # For other clients with simple connect method
                await client.connect()
                return True
            elif hasattr(client, 'start'):
                # For clients using BaseProtocolClient start method
                await client._initialize_client()
                await client._connect()
                return True
            else:
                self.log.warning(f" ⚠️ Client has no recognizable connect method")
                return False
                
        except Exception as connect_error:
            self.log.error(f" ❌ Connection failed: {connect_error}")
            return False
    
    async def get_devices_by_protocol_type(self, protocol_type: str):
        """
        Get all devices for a specific protocol type.
        
        Args:
            protocol_type: The protocol type to filter by
            
        Returns:
            List of devices matching the protocol type
        """
        try:
            # Use FrappeService filtered query
            devices = await self.frappe.get_filtered("device", {"protocol_type": protocol_type})
            return [device for device in devices if device.is_active]
        except Exception as e:
            self.log.error(f"Failed to fetch devices for protocol {protocol_type}: {e}")
            return []
    
    async def get_active_protocol_types(self):
        """
        Get all unique protocol types that have active devices.
        
        Returns:
            Set of protocol types with active devices
        """
        try:
            devices = await self.frappe.get_devices()
            active_protocols = set()
            for device in devices:
                if (device.is_active and hasattr(device, 'protocol_type') 
                    and device.protocol_type):
                    active_protocols.add(device.protocol_type)
            return active_protocols
        except Exception as e:
            self.log.error(f"Failed to fetch active protocol types: {e}")
            return set()
    
    async def validate_device_configurations(self):
        """
        Validate that all active devices have proper protocol configurations.
        
        Returns:
            Dict with validation results
        """
        validation_results = {
            "valid_devices": [],
            "invalid_devices": [],
            "missing_protocol_configs": [],
            "total_devices": 0
        }
        
        try:
            devices = await self.frappe.get_devices()
            validation_results["total_devices"] = len(devices)
            
            # Get all available protocol configurations
            protocol_configs = await self.frappe.get_all("protocol_config")
            available_protocols = {config.name1 for config in protocol_configs}
            
            for device in devices:
                if not device.is_active:
                    continue
                    
                if (hasattr(device, 'protocol_type') and hasattr(device, 'protocol_used')
                    and device.protocol_type and device.protocol_used):
                    
                    if device.protocol_type in available_protocols:
                        validation_results["valid_devices"].append(device.device_id)
                    else:
                        validation_results["missing_protocol_configs"].append({
                            "device_id": device.device_id,
                            "protocol_type": device.protocol_type
                        })
                else:
                    validation_results["invalid_devices"].append({
                        "device_id": getattr(device, 'device_id', 'unknown'),
                        "issue": "Missing protocol_type or protocol_used"
                    })
                    
        except Exception as e:
            self.log.error(f"Device configuration validation failed: {e}")
            
        return validation_results


# Example usage with dependency injection:
"""
# In your main application or orchestrator:
frappe_service = FrappeService(url, user, pwd)
device_mapper = DeviceProtocolMapper(frappe_service)
clients = await device_mapper.create_protocol_clients()
"""



# from Factories.protocol_factory import ProtocolFactory
# from Models.classes import DeviceDetails, ProtocolConfiguration
# from frappeclient import FrappeClient
# from collections import defaultdict
# from Protocols.base_protocol_client import ProtocolType, ProtocolClientConfig
# import asyncio
# import traceback
# import pprint
# import json

# class DeviceProtocolMapper:
#     def __init__(self):
#         self.endpoint_url = "http://192.168.1.63:8000"
#         self.username = "Administrator"
#         self.password = "manik0204"
#         self.frappe_client = FrappeClient(self.endpoint_url, self.username, self.password)

#     def fetch_all_device_details(self):
#         """Fetch all devices from Frappe backend"""
#         devices_raw = self.frappe_client.get_list(
#             "Device Details",
#             fields=["device_id", "protocol_type", "protocol_used"]
#         )
#         return [DeviceDetails(d) for d in devices_raw]

#     def fetch_protocol_config(self, protocol_type):
#         """Fetch protocol configuration from Frappe backend"""
#         raw = self.frappe_client.get_doc("Protocol Configuration", protocol_type)
#         print(f"Fetched protocol config for {protocol_type}: {raw}")
#         return ProtocolConfiguration(raw)

#     def _extract_server_address(self, connection_params, protocol_used):
#         """Helper method to extract server/upper() == "MQTT":"""
#         if protocol_used.upper() == "MQTT":
#             return (connection_params.get('broker') or 
#                     connection_params.get('host') or 
#                     connection_params.get('hostname') or 
#                     'unknown_broker')
#         elif protocol_used.upper() == "OPCUA":
#             endpoint = (connection_params.get('endpoint_url') or 
#                     connection_params.get('url') or 
#                     'unknown_server')
#             return endpoint
#         else:
#             return (connection_params.get('host') or 
#                     connection_params.get('server') or 
#                     connection_params.get('address') or 
#                     'unknown_server')

#     def make_protocol_config(self, protocol_used, connection_params, tags, metadata):
#         # ...existing code...
#     # def make_protocol_config(protocol_used, connection_params, tags, metadata):
#         # Normalize protocol type
#         protocol_type_enum = (
#             ProtocolType.MQTT if protocol_used.strip().upper() == "MQTT" 
#             else ProtocolType.OPCUA if protocol_used.strip().upper() == "OPCUA"
#             else ProtocolType(protocol_used.strip().lower())
#             # Update this as needed for other protocols
#         )
#         return ProtocolClientConfig(
#             protocol_type=protocol_type_enum,
#             connection_params=connection_params,
#             tags=tags,
#             metadata=metadata,
#             log_file=None,             # Or set if available
#             max_retries=5,
#             retry_delay=1.0,
#             max_retry_delay=60.0,
#             timeout=30,
#         )


#     async def create_protocol_clients(self):
#         """
#         Simplified protocol client creation method.
        
#         This method:
#         1. Fetches all devices from Frappe backend
#         2. Groups devices by protocol_type (client configuration name)
#         3. Creates one protocol client per unique (protocol_type, protocol_used) pair
#         4. Attempts to connect each client and validates connection
#         5. Returns list of successfully connected clients
        
#         Returns:
#             List[BaseProtocolClient]: List of connected protocol clients
#         """
#         print("🔧 Starting create_protocol_clients...")
        
#         # Step 1: Fetch all devices from Frappe backend
#         print("📡 Fetching device details from Frappe backend...")
#         try:
#             devices = self.fetch_all_device_details()
#             print(f"✅ Fetched {len(devices)} devices from backend")
#         except Exception as e:
#             print(f"❌ Failed to fetch devices: {e}")
#             return []
        
#         if not devices:
#             print("⚠️ No devices found in backend")
#             return []
        
#         # Step 2: Group devices by protocol_type (client configuration name)
#         print("📊 Grouping devices by protocol configuration...")
#         devices_by_protocol_type = defaultdict(list)
#         protocol_types_map = {}  # protocol_type -> protocol_used
        
#         for device in devices:
#             if device.protocol_type and device.protocol_used:
#                 devices_by_protocol_type[device.protocol_type].append(device)
#                 protocol_types_map[device.protocol_type] = device.protocol_used
#                 print(f"  📋 Device {device.device_id}: {device.protocol_type} ({device.protocol_used})")
#             else:
#                 print(f"  ⚠️ Skipping device {device.device_id}: Missing protocol_type or protocol_used")
        
#         print(f"✅ Grouped devices into {len(devices_by_protocol_type)} protocol configurations")
        
#         # Step 3: Create and connect protocol clients
#         connected_clients = []
        
#         for protocol_type, device_list in devices_by_protocol_type.items():
#             print(f"\n🔌 Processing protocol configuration: {protocol_type}")
            
#             try:
#                 # Step 3a: Fetch protocol configuration
#                 print(f"  📡 Fetching protocol configuration for {protocol_type}...")
#                 protocol_config = self.fetch_protocol_config(protocol_type)
                
#                 # Step 3b: Extract connection parameters
#                 connection_params = protocol_config.connection_parameters or {}
#                 if isinstance(connection_params, str):
#                     connection_params = json.loads(connection_params)
#                 protocol_used = protocol_types_map[protocol_type]
                
#                 print(f"  📋 Protocol: {protocol_used}")
#                 print(f"  🔧 Connection params: {connection_params}")
#                 # print(f"  🔧 Connection params: {list(connection_params.keys())}")
                
#                 # Step 3c: Create dummy metadata and empty tags (as requested)
#                 device_metadata = {
#                     "protocol_type": protocol_type,
#                     "protocol_used": protocol_used,
#                     "device_count": len(device_list),
#                     "device_ids": [device.device_id for device in device_list]
#                 }
#                 print(f"  📝 Device metadata: {device_metadata}")
#                 empty_tags = []  # Placeholder empty tags list as requested

#                 # make_protocol_config(protocol_used, connection_params, tags, metadata)
#                 protocol_config_obj = self.make_protocol_config (protocol_used, connection_params, empty_tags, device_metadata)        
#                 print("  🏭 Creating protocol client configuration object:")
#                 pprint.pprint(vars(protocol_config_obj))

#                 # Step 3d: Use ProtocolFactory to create client
#                 print(f"  🏭 Creating protocol client using ProtocolFactory...")

#                 client = ProtocolFactory.create(
#                     protocol_type=protocol_config_obj.protocol_type,
#                     config=protocol_config_obj,   
#                     tags=empty_tags,
#                     metadata=device_metadata,
#                     trigger_config={}
# )
                
#                 print(f"  ✅ Created {protocol_used} client for {protocol_type}")
                
#                 # Step 3e: Attempt to connect and validate connection
#                 print(f"  🔗 Attempting to connect {protocol_used} client...")
                
#                 # Connection logic depends on the client type
#                 connection_success = False
                
#                 try:
#                     if hasattr(client, 'connect_with_retry'):
#                         # For OPCUA clients with retry logic
#                         connection_success = await client.connect_with_retry(max_attempts=3)
#                     elif hasattr(client, 'connect'):
#                         # For other clients with simple connect method
#                         await client.connect()
#                         connection_success = True
#                     elif hasattr(client, 'start'):
#                         # For clients using BaseProtocolClient start method
#                         await client._initialize_client()
#                         await client._connect()
#                         connection_success = True
#                     else:
#                         print(f"  ⚠️ Client has no recognizable connect method")
#                         connection_success = False
                        
#                 except Exception as connect_error:
#                     print(f"  ❌ Connection failed: {connect_error}")
#                     connection_success = False
                
#                 # Step 3f: Log connection result
#                 if connection_success:
#                     # Extract server/broker address for logging
#                     server_address = self._extract_server_address(connection_params, protocol_used)
                    
#                     if protocol_used.upper() == "MQTT":
#                         print(f"  ✅ Connected to MQTT broker at {server_address}")
#                     elif protocol_used.upper() == "OPCUA":
#                         print(f"  ✅ Connected to OPC UA server at {server_address}")
#                     else:
#                         print(f"  ✅ Connected to {protocol_used} server at {server_address}")
                    
#                     # Add client to successful connections list
#                     connected_clients.append(client)
#                 else:
#                     print(f"  ❌ Failed to connect {protocol_used} client for {protocol_type}")
                    
#             except Exception as e:
#                 print(f"  ❌ Error creating/connecting client for {protocol_type}: {e}")
#                 traceback.print_exc()
        
#         # Step 4: Return connected clients
#         print(f"\n🎉 Protocol client creation complete!")
#         print(f"✅ Successfully connected {len(connected_clients)} clients")
#         print(f"❌ Failed to connect {len(devices_by_protocol_type) - len(connected_clients)} clients")
        
#         return connected_clients
