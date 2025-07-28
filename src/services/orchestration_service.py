"""Central coordinator: startup, enrichment, monitoring + hot-reload."""
from __future__ import annotations
import asyncio, logging, json
from collections import defaultdict
from src.core.patterns import (
    ChangeType, ConfigurationObserver, ConfigurationChangeEvent,
    CircuitBreaker,
    StateMachine, ClientState
)
from src.protocols.protocol_factory import ProtocolFactory
from src.protocols.base_protocol_client import ProtocolClientConfig, ProtocolType
from src.services.frappe_service import FrappeService
from typing import Dict, List
from config.app_config import settings
from src.services.device_service import DeviceProtocolMapper
# from src.triggers import TriggerStrategyFactory
import asyncio
import logging


class DataLoggingOrchestrator(ConfigurationObserver):
    def __init__(self, frappe: FrappeService):
        self.frappe  = frappe
        self.log     = logging.getLogger(self.__class__.__name__)
        self.clients: Dict[str, any] = {}           # protocol_type -> client
        self.states:  Dict[str, StateMachine] = {}
        self.cbs:     Dict[str, CircuitBreaker] = {}

    # --------------------------------------------------------------------- #
    #  Public API
    # --------------------------------------------------------------------- #
    async def startup(self):
        await self._create_and_connect_clients()
        await self._enrich_clients()
        await self._start_logging()
        self.log.info("orchestrator ready (%d clients)", len(self.clients))

    # --------------------------------------------------------------------- #
    #  Observer interface
    # --------------------------------------------------------------------- #
    def get_observer_id(self): return "orchestrator"

    def get_interested_changes(self):           # all change types
        return list(ChangeType)

    async def notify(self, event: ConfigurationChangeEvent):
        self.log.info("cfg-change: %s %s", event.change_type, event.entity_id)
        await self._handle_change(event)

    # --------------------------------------------------------------------- #
    #  Private helpers
    # --------------------------------------------------------------------- #
    async def _create_and_connect_clients(self):
        devs = await self.frappe.get_devices()
        devs_by_proto = defaultdict(list)
        proto_used    = {}
        for d in devs:
            if d.is_active:
                devs_by_proto[d.protocol_type].append(d)
                proto_used[d.protocol_type] = d.protocol_used

        print(f"devices by protocol: {devs_by_proto}")
        for proto_type, lst in devs_by_proto.items():
            pcfg = await self.frappe.get_protocol_config(proto_type)
            conn = pcfg.connection_parameters
            cfg  = ProtocolClientConfig(
                protocol_type = ProtocolType(proto_used[proto_type].lower()),
                connection_params = conn,
                metadata = {"protocol_type": proto_type,
                            "device_ids": [d.device_id for d in lst]},
            )
            cb   = CircuitBreaker()
            # client = await cb(lambda: ProtocolFactory.create(cfg.protocol_type, cfg))  # type: ignore
            client = ProtocolFactory.create(cfg.protocol_type, cfg)

            self.clients[proto_type] = client
            self.states[proto_type]  = StateMachine(ClientState.DISCONNECTED)
            self.cbs[proto_type]     = cb

            # connect in background
            asyncio.create_task(client.start())

    async def _enrich_clients(self):
        # fetch triggers + mappings and inject – implementation elided for brevity
        pass

    async def _start_logging(self):
        # ensure clients already running; nothing to do because start() begins loop
        pass

    async def _handle_change(self, evt: ConfigurationChangeEvent):
        # re-fetch config, stop + restart affected client
        proto_type = evt.metadata.get("protocol_type") if evt.metadata else None
        if not proto_type or proto_type not in self.clients:
            return
        client = self.clients[proto_type]
        await client.stop()
        await client.start()


    
async def setup_data_logging():
    # Create singleton FrappeService with configuration
    frappe_service = FrappeService(
        url=settings.FRAPPE_URL,
        user=settings.FRAPPE_USER,
        pwd=settings.FRAPPE_PWD,
        ttl=settings.CACHE_TTL
    )
    
    # Inject FrappeService into DeviceProtocolMapper
    device_mapper = DeviceProtocolMapper(frappe_service)
    
    # Create and connect protocol clients
    connected_clients = await device_mapper.create_protocol_clients()
    
    # Validate device configurations
    validation_results = await device_mapper.validate_device_configurations()
    
    return connected_clients, validation_results