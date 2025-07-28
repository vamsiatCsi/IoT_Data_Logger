"""Rich-handler logging preset."""
import logging
from rich.logging import RichHandler
from .app_config import settings

def configure():
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format="%(asctime)s │ %(name)-38s │ %(levelname)-8s │ %(message)s",
        datefmt="%H:%M:%S",
        handlers=[RichHandler(rich_tracebacks=True, markup=True)],
    )
