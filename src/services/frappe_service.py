
# frappe_service.py

import json, asyncio, functools, time
from typing import List, Dict, Any, Optional
from frappeclient import FrappeClient
from src.models import doctype_models
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DOCTYPES: Dict[str, dict] = {
    "device": {
        "doctype": "Device Details",
        "fields": ["*"], 
        "primary_key": "device_id", 
        "has_children": False,
    },
    "protocol_config": {
        "doctype": "Protocol Configuration",
        "fields": ["*"],
        "primary_key": "name1",
        "has_children": False,
    },
    "logging_trigger": {
        "doctype": "Logging Trigger",
        "fields": ["*"],
        "primary_key": "trigger_name",
        "has_children": True,
        "children": {
            "time_based_table": "Time Based",
            "condition_based_table": "Condition Based"
        }
    },
    "column_mapping": {
        "doctype": "Column Mapping",
        "fields": ["*"],
        "primary_key": "select_device_id",
        "has_children": False,
    }
}




class FrappeService:
    _singleton = None

    def __new__(cls, url: str, user: str, pwd: str, *, ttl: int = 300):
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)
            cls._singleton._init(url, user, pwd, ttl)
        return cls._singleton

    def _init(self, url: str, user: str, pwd: str, ttl: int):
        self.client       = FrappeClient(url, user, pwd)
        self.cache_ttl    = ttl
        self._cache_store = {}

    async def get_all(self, logical_doctype: str) -> List[Any]:
        """Fetch all objects by logical doctype name (from registry)."""
        config = DOCTYPES[logical_doctype]
        key = f"{logical_doctype}_all"
        return await self._cached(key, lambda: self._fetch_all(config, logical_doctype))

    async def get_by_id(self, logical_doctype: str, value: str) -> Any:
        """Fetch single object by logical doctype and id."""
        config = DOCTYPES[logical_doctype]
        key = f"{logical_doctype}::{value}"
        return await self._cached(key, lambda: self._fetch_by_id(config, value, logical_doctype))

    async def get_filtered(self, logical_doctype: str, filters: Dict[str, Any]) -> List[Any]:
        """Fetch list of objects with filter."""
        config = DOCTYPES[logical_doctype]
        key = f"{logical_doctype}::filter::{json.dumps(filters,sort_keys=True)}"
        return await self._cached(key, lambda: self._fetch_filtered(config, filters, logical_doctype))

    # ---- Caching helper ----
    async def _cached(self, key: str, supplier):
        ts, value = self._cache_store.get(key, (0, None))
        if time.time() - ts > self.cache_ttl or value is None:
            value = await supplier()
            self._cache_store[key] = (time.time(), value)
        return value

    # ---- Fetchers ----
    async def _fetch_all(self, config: dict, logical_doctype: str):
        logger.info(f"Fetching ALL from Frappe: doctype={config['doctype']}")
        rows = self.client.get_list(config['doctype'], fields=config.get("fields", ["*"]), limit_page_length=0)
        logger.info(f"Fetched {len(rows)} rows for {logical_doctype}")
        return [self._row_to_obj(logical_doctype, row) for row in rows]

    async def _fetch_filtered(self, config, filters, logical_doctype):
        logger.info(f"Fetching FILTERED from Frappe: doctype={config['doctype']} | filters={filters}")
        if config.get("has_children"):
            rows = self.client.get_list(config['doctype'], filters=filters, fields=["name"])
            docs = [self.client.get_doc(config['doctype'], row["name"]) for row in rows]
            logger.info(f"Fetched {len(docs)} documents (with children) for {logical_doctype}")
            return [self._row_to_obj(logical_doctype, doc) for doc in docs]

        rows = self.client.get_list(config['doctype'], filters=filters, fields=config.get('fields', ["*"]))
        logger.info(f"Fetched {len(rows)} rows for {logical_doctype} with filters")
        return [self._row_to_obj(logical_doctype, row) for row in rows]

    async def _fetch_by_id(self, config: dict, value: str, logical_doctype: str):
        logger.info(f"Fetching BY ID from Frappe: doctype={config['doctype']} | id={value}")
        doc = self.client.get_doc(config['doctype'], value)
        logger.info(f"Fetched document for {logical_doctype}: {doc}")
        return self._row_to_obj(logical_doctype, doc)


    def _row_to_obj(self, logical_doctype: str, row: Dict[str, Any]) -> Any:
        # Map logical doctype key to correct model class from domain_models
        model_lookup = {
            "device": doctype_models.Device,
            "protocol_config": doctype_models.ProtocolConfig,
            "logging_trigger": doctype_models.LoggingTrigger,
        }
        if logical_doctype not in model_lookup:
            raise ValueError(f"No model class defined for logical_doctype '{logical_doctype}'")
        return model_lookup[logical_doctype].from_row(row)

    # Example convenience aliases for your app:
    async def get_devices(self):              # list[Device]
        return await self.get_all("device")
    async def get_protocol_config(self, name):  # ProtocolConfig
        return await self.get_by_id("protocol_config", name)
    async def get_logging_trigger(self, device_id):  # ALL LoggingTriggers for a device
        return await self.get_filtered("logging_trigger", {"device_id": device_id})

