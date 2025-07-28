#!/usr/bin/env python3
import asyncio, sys
from config.logging_config import configure
from config.app_config import settings
from src.services.frappe_service import FrappeService
from src.services.orchestration_service import DataLoggingOrchestrator

async def async_main():
    configure()
    frappe = FrappeService(
        url=settings.FRAPPE_URL,
        user=settings.FRAPPE_USER,
        pwd=settings.FRAPPE_PWD,
        ttl=settings.CACHE_TTL,
    )
    orchestrator = DataLoggingOrchestrator(frappe)
    await orchestrator.startup()
    # keep process alive
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        sys.exit("🌙  graceful shutdown")
