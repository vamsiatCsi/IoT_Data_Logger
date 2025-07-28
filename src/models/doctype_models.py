from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Any, Dict, List, Optional


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

###############################################################################
# 4. TRIGGER SET (PARENT) -----------------------------------------------------
###############################################################################

@dataclass(frozen=True, slots=True)
class LoggingTrigger:
    """
    Immutable projection of the *Logging Trigger* Doctype, bundling both
    time-based and condition-based child rows.
    """
    name: str
    device_id: str
    table_format: str                   # "Narrow" / "Wide"
    log_on_static_interval: bool
    static_interval: int                # Meaning depends on `update_rate_units`
    update_rate_units: str              # "Seconds", "Minutes", …
    log_all_items: bool
    time_based: bool
    condition_based: bool
    time_based_table: List[TimeTrigger] = field(default_factory=list)
    condition_based_table: List[ConditionTrigger] = field(default_factory=list)

    # ---------- factory --------------------------------------------------- #
    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "LoggingTrigger":
        return cls(
            name                  = row["name"],
            device_id             = row["device_id"],
            table_format          = row["table_format"],
            log_on_static_interval= bool(row.get("log_on_static_interval", 0)),
            static_interval       = int(row.get("static_interval", 0)),
            update_rate_units     = row.get("update_rate_units", "Seconds"),
            log_all_items         = bool(row.get("log_all_items", 0)),
            time_based            = bool(row.get("time_based", 0)),
            condition_based       = bool(row.get("condition_based", 0)),
            time_based_table      = [
                TimeTrigger.from_row(t) for t in row.get("time_based_table", [])
            ],
            condition_based_table = [
                ConditionTrigger.from_row(c) for c in row.get("condition_based_table", [])
            ]
        )

###############################################################################
# 5. HELPER PARSERS -----------------------------------------------------------
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
