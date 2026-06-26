# src/crypto_alerts/feed/base.py
#
# AbstractFeed defines the contract that every feed implementation must
# satisfy. The ipeline coordinator depends on this interface, not on 
# BinanceFeed directly. This is the Dependency Inversion Principle
# Here, high-level components depend on abstractions, not concretion

import asyncio
from abc import ABC, abstractmethod

from crypto_alerts.models.price_tick import PriceTick

class AbstractFeed(ABC):
    """
    Base class for all price feed implementations.
    
    A feed has one job: produce PriceTick objects and put them onto the 
    provided queue. How it does this (Websocket, REST polling, mock file 
    replay) is entirely up to the implementation.

    """

    def __init__(self, outbound_queue: asyncio.Queue[PriceTick]) -> None:
        self._queue = outbound_queue

    @abstractmethod # every class inheriting from this blueprint must 
                    # write its own version of this exact method
    async def run(self) -> None:
        """
        Starts the feed. Runs indefinitely until the task is cancelled.
        Implementation must handle their own reconnection logic
        """

    @abstractmethod
    async def subscribe(self, symbols: set[str]) -> None:
        """
        Subscribes to price updates for the given set of symbols.
        Can be called before or after run() has started.
        """
        ...