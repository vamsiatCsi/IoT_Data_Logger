"""
Observer Pattern Implementation for Configuration Changes

This module implements the Observer pattern to handle dynamic configuration
changes in the industrial data logging system.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum


class ChangeType(Enum):
    """Types of configuration changes."""
    DEVICE_ADDED = "device_added"
    DEVICE_REMOVED = "device_removed"
    DEVICE_MODIFIED = "device_modified"
    TRIGGER_MODIFIED = "trigger_modified"
    MAPPING_MODIFIED = "mapping_modified"
    PROTOCOL_CONFIG_MODIFIED = "protocol_config_modified"


@dataclass
class ConfigurationChangeEvent:
    """Event data for configuration changes."""
    change_type: ChangeType
    entity_id: str
    old_data: Optional[Dict[str, Any]] = None
    new_data: Optional[Dict[str, Any]] = None
    timestamp: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class ConfigurationObserver(ABC):
    """Abstract base class for configuration observers."""
    
    @abstractmethod
    async def notify(self, event: ConfigurationChangeEvent) -> None:
        """Handle configuration change notification."""
        pass
    
    @abstractmethod
    def get_observer_id(self) -> str:
        """Get unique identifier for this observer."""
        pass
    
    @abstractmethod
    def get_interested_changes(self) -> List[ChangeType]:
        """Get list of change types this observer is interested in."""
        pass


class ConfigurationSubject:
    """Subject that notifies observers of configuration changes."""
    
    def __init__(self):
        self._observers: List[ConfigurationObserver] = []
        self._logger = logging.getLogger(self.__class__.__name__)
        self._notification_lock = asyncio.Lock()
    
    async def subscribe(self, observer: ConfigurationObserver) -> None:
        """Subscribe an observer to configuration changes."""
        async with self._notification_lock:
            if observer not in self._observers:
                self._observers.append(observer)
                self._logger.info(f"Subscribed observer: {observer.get_observer_id()}")
            else:
                self._logger.warning(f"Observer already subscribed: {observer.get_observer_id()}")
    
    async def unsubscribe(self, observer: ConfigurationObserver) -> None:
        """Unsubscribe an observer from configuration changes."""
        async with self._notification_lock:
            if observer in self._observers:
                self._observers.remove(observer)
                self._logger.info(f"Unsubscribed observer: {observer.get_observer_id()}")
            else:
                self._logger.warning(f"Observer not found for unsubscription: {observer.get_observer_id()}")
    
    async def notify_observers(self, event: ConfigurationChangeEvent) -> None:
        """Notify all interested observers of a configuration change."""
        interested_observers = [
            observer for observer in self._observers
            if event.change_type in observer.get_interested_changes()
        ]
        
        if not interested_observers:
            self._logger.debug(f"No observers interested in change type: {event.change_type}")
            return
        
        self._logger.info(f"Notifying {len(interested_observers)} observers of {event.change_type}")
        
        # Notify observers concurrently
        notification_tasks = []
        for observer in interested_observers:
            task = asyncio.create_task(self._safe_notify_observer(observer, event))
            notification_tasks.append(task)
        
        # Wait for all notifications to complete
        if notification_tasks:
            await asyncio.gather(*notification_tasks, return_exceptions=True)
    
    async def _safe_notify_observer(self, observer: ConfigurationObserver, event: ConfigurationChangeEvent) -> None:
        """Safely notify a single observer, catching and logging any exceptions."""
        try:
            await observer.notify(event)
            self._logger.debug(f"Successfully notified observer: {observer.get_observer_id()}")
        except Exception as e:
            self._logger.error(f"Error notifying observer {observer.get_observer_id()}: {e}", exc_info=True)
    
    def get_observer_count(self) -> int:
        """Get the number of registered observers."""
        return len(self._observers)
    
    def get_observer_ids(self) -> List[str]:
        """Get list of all registered observer IDs."""
        return [observer.get_observer_id() for observer in self._observers]


class FilteredConfigurationObserver(ConfigurationObserver):
    """Observer that can filter changes based on entity IDs."""
    
    def __init__(self, observer_id: str, interested_changes: List[ChangeType], 
                 entity_filter: Optional[Callable[[str], bool]] = None):
        self._observer_id = observer_id
        self._interested_changes = interested_changes
        self._entity_filter = entity_filter
        self._logger = logging.getLogger(f"{self.__class__.__name__}_{observer_id}")
    
    def get_observer_id(self) -> str:
        return self._observer_id
    
    def get_interested_changes(self) -> List[ChangeType]:
        return self._interested_changes
    
    async def notify(self, event: ConfigurationChangeEvent) -> None:
        """Handle configuration change with optional entity filtering."""
        # Apply entity filter if configured
        if self._entity_filter and not self._entity_filter(event.entity_id):
            self._logger.debug(f"Filtered out change for entity: {event.entity_id}")
            return
        
        await self.handle_filtered_change(event)
    
    @abstractmethod
    async def handle_filtered_change(self, event: ConfigurationChangeEvent) -> None:
        """Handle the filtered configuration change."""
        pass


class AsyncEventBus:
    """Enhanced event bus for configuration changes with async processing."""
    
    def __init__(self, max_queue_size: int = 1000):
        self._subject = ConfigurationSubject()
        self._event_queue = asyncio.Queue(maxsize=max_queue_size)
        self._processing_task: Optional[asyncio.Task] = None
        self._running = False
        self._logger = logging.getLogger(self.__class__.__name__)
    
    async def start(self) -> None:
        """Start the event processing loop."""
        if self._running:
            self._logger.warning("Event bus is already running")
            return
        
        self._running = True
        self._processing_task = asyncio.create_task(self._process_events())
        self._logger.info("Event bus started")
    
    async def stop(self) -> None:
        """Stop the event processing loop."""
        if not self._running:
            self._logger.warning("Event bus is not running")
            return
        
        self._running = False
        
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
        
        self._logger.info("Event bus stopped")
    
    async def publish(self, event: ConfigurationChangeEvent) -> None:
        """Publish a configuration change event."""
        try:
            await self._event_queue.put(event)
            self._logger.debug(f"Published event: {event.change_type} for {event.entity_id}")
        except asyncio.QueueFull:
            self._logger.error(f"Event queue full, dropping event: {event.change_type}")
    
    async def subscribe(self, observer: ConfigurationObserver) -> None:
        """Subscribe an observer to configuration changes."""
        await self._subject.subscribe(observer)
    
    async def unsubscribe(self, observer: ConfigurationObserver) -> None:
        """Unsubscribe an observer from configuration changes."""
        await self._subject.unsubscribe(observer)
    
    async def _process_events(self) -> None:
        """Process events from the queue."""
        self._logger.info("Started event processing loop")
        
        while self._running:
            try:
                # Wait for event with timeout to allow checking _running flag
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
                await self._subject.notify_observers(event)
                self._event_queue.task_done()
            except asyncio.TimeoutError:
                # Normal timeout, continue loop
                continue
            except Exception as e:
                self._logger.error(f"Error processing event: {e}", exc_info=True)
        
        self._logger.info("Event processing loop stopped")
    
    def get_queue_size(self) -> int:
        """Get current event queue size."""
        return self._event_queue.qsize()
    
    def get_observer_count(self) -> int:
        """Get number of registered observers."""
        return self._subject.get_observer_count()


# Global event bus instance
global_event_bus: Optional[AsyncEventBus] = None


async def get_event_bus() -> AsyncEventBus:
    """Get or create the global event bus instance."""
    global global_event_bus
    if global_event_bus is None:
        global_event_bus = AsyncEventBus()
        await global_event_bus.start()
    return global_event_bus


async def cleanup_event_bus() -> None:
    """Cleanup the global event bus."""
    global global_event_bus
    if global_event_bus:
        await global_event_bus.stop()
        global_event_bus = None
