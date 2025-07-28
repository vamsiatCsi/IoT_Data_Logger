from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Any, Dict, List, Optional
import json


###############################################################################
# 1. DEVICE -------------------------------------------------------------------
###############################################################################

@dataclass(frozen=True, slots=True)
class Device:
    """Immutable projection of the *Device Details* Doctype."""
    device_id: str
    name: str
    protocol_type: str                # e.g. "Client 1 (OPC UA)"
    protocol_used: str                # e.g. "OPCUA" / "MQTT"
    is_active: bool
    status: str
    model_number: Optional[str] = None
    description: Optional[str] = None
    area: Optional[str] = None
    location: Optional[str] = None
    installation_date: Optional[datetime] = None
    customerplant: Optional[str] = None
    manufacturer: Optional[str] = None
    serial_number: Optional[str] = None
    maintenance_schedule: Optional[str] = None
    select_doctype: Optional[str] = None

    # ---------- factory --------------------------------------------------- #
    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "Device":
        return cls(
            device_id          = row["device_id"],
            name               = row.get("device_name") or row.get("name"),
            protocol_type      = row["protocol_type"],
            protocol_used      = row["protocol_used"],
            is_active          = bool(row.get("is_active", 0)),
            status             = row.get("status", "Unknown"),
            model_number       = row.get("model_number"),
            description        = row.get("description"),
            area               = row.get("area"),
            location           = row.get("location"),
            installation_date  = _parse_dt(row.get("installation_date")),
            customerplant      = row.get("customerplant"),
            manufacturer       = row.get("manufacturer"),
            serial_number      = row.get("serial_number"),
            maintenance_schedule = row.get("maintenance_schedule"),
            select_doctype     = row.get("select_doctype")
        )

###############################################################################
# 2. PROTOCOL CONFIG ----------------------------------------------------------
###############################################################################

@dataclass(frozen=True, slots=True)
class ProtocolConfig:
    """Immutable projection of the *Protocol Configuration* Doctype."""
    name: str                          # e.g. "Client 2 (OPC UA)"
    protocol_name: str                 # "OPCUA" / "MQTT" / …
    connection_parameters: Dict[str, Any]
    owner: Optional[str] = None
    creation: Optional[datetime] = None
    modified: Optional[datetime] = None

    # ---------- factory --------------------------------------------------- #
    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "ProtocolConfig":
        raw_params = row.get("connection_parameters") or {}
        if isinstance(raw_params, str):
            import json
            raw_params = json.loads(raw_params)
        return cls(
            name                 = row["name"],
            protocol_name        = row["protocol_name"],
            connection_parameters = raw_params,
            owner                = row.get("owner"),
            creation             = _parse_dt(row.get("creation")),
            modified             = _parse_dt(row.get("modified"))
        )

###############################################################################
# 3. CHILD TABLES: TIME & CONDITION TRIGGERS ----------------------------------
###############################################################################

@dataclass(frozen=True, slots=True)
class TimeTrigger:
    """Row from *Time Based* child table of *Logging Trigger*."""
    start_time: time
    stop_time: time
    sunday: bool
    monday: bool
    tuesday: bool
    wednesday: bool
    thursday: bool
    friday: bool
    saturday: bool
    log_all_on_start: bool
    log_all_on_stop: bool
    every_day: bool
    week_days: bool
    week_end: bool
    row_id: str

    # ---------- factory --------------------------------------------------- #
    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "TimeTrigger":
        return cls(
            start_time        = _parse_time(row["start_time"]),
            stop_time         = _parse_time(row["stop_time"]),
            sunday            = bool(row.get("sunday", 0)),
            monday            = bool(row.get("monday", 0)),
            tuesday           = bool(row.get("tuesday", 0)),
            wednesday         = bool(row.get("wednesday", 0)),
            thursday          = bool(row.get("thursday", 0)),
            friday            = bool(row.get("friday", 0)),
            saturday          = bool(row.get("saturday", 0)),
            log_all_on_start  = bool(row.get("log_all_on_start", 0)),
            log_all_on_stop   = bool(row.get("log_all_on_stop", 0)),
            every_day         = bool(row.get("everyday", 0)),
            week_days         = bool(row.get("weekdays", 0)),
            week_end          = bool(row.get("weekend", 0)),
            row_id            = row["name"]
        )


@dataclass(frozen=True, slots=True)
class ConditionTrigger:
    """Row from *Condition Based* child table of *Logging Trigger*."""
    device_field: str
    start_condition_expression: str
    stop_condition_expression: str
    row_id: str

    # ---------- factory --------------------------------------------------- #
    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "ConditionTrigger":
        return cls(
            device_field               = row["device_field"],
            start_condition_expression = row["start_condition_expression"],
            stop_condition_expression  = row["stop_condition_expression"],
            row_id                     = row["name"]
        )

@dataclass(frozen=True, slots=True)
class AlwaysTrigger:
    static_interval: int
    update_rate_units: str

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "AlwaysTrigger":
        return cls(
            static_interval=int(row["static_interval"]),
            update_rate_units=row["update_rate_units"],
        )

###############################################################################
# 4. TRIGGER SET (PARENT) -----------------------------------------------------
###############################################################################
@dataclass(frozen=True, slots=True)
class LoggingTrigger:
    name: str
    device_id: str
    table_format: str
    log_all_items: bool
    time_based: bool
    condition_based: bool
    always_trigger: bool

    # Only one of the following should be populated based on trigger type
    time_trigger: Optional[TimeTrigger] = None
    condition_trigger: Optional[ConditionTrigger] = None
    always_trigger_details: Optional[AlwaysTrigger] = None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "LoggingTrigger":
        time_based = bool(row.get("time_based", 0))
        condition_based = bool(row.get("condition_based", 0))
        always_trigger = bool(row.get("always_trigger", 0))

        time_trigger = (
            TimeTrigger.from_row(row["time_based_table"][0])
            if time_based and row.get("time_based_table")
            else None
        )
        condition_trigger = (
            ConditionTrigger.from_row(row["condition_based_table"][0])
            if condition_based and row.get("condition_based_table")
            else None
        )
        always_trigger_details = (
            AlwaysTrigger.from_row(row["always_trigger_table"][0])
            if always_trigger and row.get("always_trigger_table")
            else None
        )

        return cls(
            name=row["name"],
            device_id=row["device_id"],
            table_format=row["table_format"],
            log_all_items=bool(row.get("log_all_items", 0)),
            time_based=time_based,
            condition_based=condition_based,
            always_trigger=always_trigger,
            time_trigger=time_trigger,
            condition_trigger=condition_trigger,
            always_trigger_details=always_trigger_details,
        )
    

###############################################################################
# 5. COLUMN MAPPING -----------------------------------------------------
###############################################################################
@dataclass(frozen=True, slots=True)
class ColumnMapping:
    name: str
    owner: str
    creation: str  # can be changed to Optional[datetime] if you parse
    modified: str  # can be changed to Optional[datetime] if you parse
    modified_by: str
    docstatus: int
    idx: int
    device_id: Optional[str]
    device_category: str
    category_tag_json: Dict[str, Any]  # parsed from JSON string

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "ColumnMapping":
        category_tag_json = row.get("category_tag_json", "{}")
        if isinstance(category_tag_json, str):
            try:
                category_tag_json = json.loads(category_tag_json)
            except Exception:
                category_tag_json = {}
        return cls(
            name=row["name"],
            owner=row["owner"],
            creation=row["creation"],
            modified=row["modified"],
            modified_by=row["modified_by"],
            docstatus=int(row.get("docstatus", 0)),
            idx=int(row.get("idx", 0)),
            device_id=row.get("device_id"),
            device_category=row.get("device_category", ""),
            category_tag_json=category_tag_json,
        )


###############################################################################
# 6. HELPER PARSERS -----------------------------------------------------------
###############################################################################

def _parse_dt(value: Any) -> Optional[datetime]:
    """Convert various Frappe datetime strings into `datetime` objects."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    # Frappe standard: "YYYY-MM-DD HH:MM:SS[.ffffff]"
    try:
        return datetime.strptime(value.split(".")[0], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _parse_time(value: Any) -> time:
    """Parse 'HH:MM:SS' strings into `time`."""
    if not value:
        return time(0, 0, 0)
    if isinstance(value, time):
        return value
    return datetime.strptime(value, "%H:%M:%S").time()
