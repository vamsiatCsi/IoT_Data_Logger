from enum import Enum, auto
from typing import Dict, List

class ClientState(Enum):
    DISCONNECTED  = auto()
    CONNECTING    = auto()
    CONNECTED     = auto()
    CONFIGURING   = auto()
    LOGGING       = auto()
    RECONNECTING  = auto()
    ERROR         = auto()
    SHUTDOWN      = auto()

class StateMachine:
    def __init__(self, initial: ClientState):
        self._state = initial
        self._trans: Dict[ClientState, List[ClientState]] = {
            ClientState.DISCONNECTED: [ClientState.CONNECTING, ClientState.SHUTDOWN],
            ClientState.CONNECTING:   [ClientState.CONNECTED,  ClientState.ERROR],
            ClientState.CONNECTED:    [ClientState.CONFIGURING, ClientState.LOGGING,
                                       ClientState.DISCONNECTED],
            ClientState.CONFIGURING:  [ClientState.LOGGING, ClientState.ERROR,
                                       ClientState.CONNECTED],
            ClientState.LOGGING:      [ClientState.CONFIGURING, ClientState.RECONNECTING,
                                       ClientState.DISCONNECTED],
            ClientState.RECONNECTING: [ClientState.CONNECTED, ClientState.ERROR,
                                       ClientState.DISCONNECTED],
            ClientState.ERROR:        [ClientState.CONNECTING, ClientState.SHUTDOWN],
            ClientState.SHUTDOWN:     [],
        }

    @property
    def state(self) -> ClientState: return self._state

    def can(self, nxt: ClientState) -> bool: return nxt in self._trans[self._state]

    def transition(self, nxt: ClientState) -> bool:
        if self.can(nxt):
            self._state = nxt
            return True
        return False
