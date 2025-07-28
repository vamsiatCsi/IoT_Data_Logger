from src.protocols.opcua_client import OPCUAClient
from src.protocols.mqtt_client import MQTTClient
from src.protocols.base_protocol_client import BaseProtocolClient, ProtocolType
import pprint

class ProtocolFactory:

    _registry = {
        ProtocolType.OPCUA : OPCUAClient,
        ProtocolType.MQTT : MQTTClient,
        # Add other protocol clients as needed
    }    

    @classmethod  
    def create(cls, protocol_type, config, tags=None, metadata=None, trigger_config=None):
        """
        Create a protocol client.
        
        Args:
            protocol_type (str): 'OPCUA', 'MQTT', etc.
            config (dict): Connection parameters
            tags (list): List of tags/nodes to monitor (can be empty)
            metadata (dict): Client metadata including device information  
            trigger_config (dict): Trigger configuration (can be empty)
        
        Returns:
            BaseProtocolClient: Configured protocol client instance
        """

        pprint.pprint(f"[ProtocolFactory.create] Called with:")
        pprint.pprint(f"  protocol_type: {protocol_type}")
        pprint.pprint(f"  config: {config}")
        pprint.pprint(f"  tags: {tags}")
        pprint.pprint(f"  metadata: {metadata}")
        pprint.pprint(f"  trigger_config: {trigger_config}")

        handler = cls._registry.get(protocol_type)
        if not handler: 
            raise ValueError(f"No handler registered for protocol: {protocol_type}")
        
        # Create client with new parameters
        client = handler(config)
        
        # Set additional properties if the client supports them
        if hasattr(client, 'tags'):
            client.tags = tags or []
        
        if hasattr(client, 'metadata'):
            client.metadata = metadata or {}
        
        if hasattr(client, 'trigger_config'):  
            client.trigger_config = trigger_config or {}
        
        return client
