from typing import Dict, List, Any
import asyncio
import logging
from .state_machine import OrchestrationStateMachine, OrchestrationState
from .commands import (
    OrchestrationCommand,
    DeviceDiscoveryCommand,
    ClientCreationCommand,
    ConfigurationEnrichmentCommand
)

class DataLoggingOrchestrator:
    """Main orchestrator using command pattern and state machine"""
    
    def __init__(self, frappe_service):
        self.frappe_service = frappe_service
        self.state_machine = OrchestrationStateMachine()
        self.context: Dict[str, Any] = {"frappe_service": frappe_service}
        self.executed_commands: List[OrchestrationCommand] = []
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def startup(self) -> bool:
        """Execute startup sequence using command pattern"""
        try:
            # Define startup command sequence
            command_sequence = [
                (DeviceDiscoveryCommand, OrchestrationState.DEVICE_DISCOVERY),
                (ClientCreationCommand, OrchestrationState.CLIENT_CREATION),
                (ConfigurationEnrichmentCommand, OrchestrationState.CONFIGURATION_ENRICHMENT),
            ]
            
            for command_class, target_state in command_sequence:
                # Transition state
                if not self.state_machine.transition_to(target_state):
                    raise RuntimeError(f"Failed to transition to {target_state}")
                
                # Execute command
                command = command_class(self.context)
                result = await command.execute()
                
                if not result.get("success", False):
                    await self._rollback_commands()
                    return False
                
                # Update context with command results
                self.context.update(result)
                self.executed_commands.append(command)
            
            # Transition to operational state
            self.state_machine.transition_to(OrchestrationState.OPERATIONAL)
            self.logger.info("Orchestration startup completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Orchestration startup failed: {e}")
            await self._rollback_commands()
            self.state_machine.transition_to(OrchestrationState.ERROR_RECOVERY)
            return False
    
    async def _rollback_commands(self):
        """Rollback executed commands in reverse order"""
        for command in reversed(self.executed_commands):
            try:
                await command.rollback()
            except Exception as e:
                self.logger.error(f"Error during rollback: {e}")
        
        self.executed_commands.clear()
    
    async def shutdown(self):
        """Graceful shutdown"""
        self.state_machine.transition_to(OrchestrationState.SHUTDOWN)
        await self._rollback_commands()
        self.logger.info("Orchestration shutdown completed")