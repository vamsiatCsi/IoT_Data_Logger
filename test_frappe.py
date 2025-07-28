import os
import asyncio
from src.services.frappe_service import FrappeService

async def main():
    # Substitute credentials from env, config, or hardcode here for testingv
    FRAPPE_URL       = os.getenv("FRAPPE_URL", "http://192.168.1.63:8000")
    FRAPPE_USER      = os.getenv("FRAPPE_USER", "Administrator")
    FRAPPE_PWD       = os.getenv("FRAPPE_PWD",  "manik0204")
    
    service = FrappeService(FRAPPE_URL, FRAPPE_USER, FRAPPE_PWD)

    # Try fetching all devices
    devices = await service.get_all("device")
    print(f"Devices ({len(devices)}):")
    for d in devices[:2]:
        print(" ", d)

    # Try fetching all protocol configs
    protocols = await service.get_all("protocol_config")
    print(f"Protocol Configs ({len(protocols)}):")
    for p in protocols[:2]:
        print(" ", p)

    # Try fetching all logging triggers
    triggers = await service.get_all("logging_trigger")
    print(f"Logging Triggers ({len(triggers)}):")
    for t in triggers[:2]:
        print(" ", t)

    # Try fetching all column mappings
    columns = await service.get_all("column_mapping")
    print(f"Column Mappings ({len(columns)}):")
    for c in columns[:2]:
        print(" ", c)

if __name__ == "__main__":
    asyncio.run(main())
