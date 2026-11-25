# encoding: utf-8
import asyncio
import logging
import time

from htnd.HtndClient import HtndClient
# pipenv run python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/rpc.proto ./protos/messages.proto ./protos/p2p.proto
from htnd.HtndThread import HtndCommunicationError


_logger = logging.getLogger(__name__)

# Seconds between two "no usable node" warnings.
NO_NODE_WARNING_INTERVAL = 30


class HtndMultiClient(object):
    def __init__(self, hosts: list[str]):
        self.htnds = [HtndClient(*h.split(":")) for h in hosts]
        self.__last_no_node_warning = -NO_NODE_WARNING_INTERVAL

    def __get_htnd(self):
        for k in self.htnds:
            if k.is_utxo_indexed:
                return k

    async def initialize_all(self):
        tasks = [asyncio.create_task(k.ping()) for k in self.htnds]

        for t in tasks:
            await t

    def __warn_no_node(self):
        now = time.monotonic()
        if now - self.__last_no_node_warning >= NO_NODE_WARNING_INTERVAL:
            self.__last_no_node_warning = now
            _logger.warning("No usable node: none of %s answers getInfo as UTXO-indexed. Asking again.",
                            ", ".join(f"{k.htnd_host}:{k.htnd_port}" for k in self.htnds))

    async def __request(self, command, params=None, timeout=30):
        htnd = self.__get_htnd()
        if htnd is None:
            # No node has answered a ping yet, for example because none was reachable when
            # the filler started. Nothing else pings a node that has never answered, so
            # ask again here; otherwise every request returns None for good.
            await self.initialize_all()
            htnd = self.__get_htnd()
            if htnd is None:
                self.__warn_no_node()
        if htnd is not None:
            return await htnd.request(command, params, timeout=timeout)

    async def request(self, command, params=None, timeout=30):
        try:
            return await self.__request(command, params, timeout=timeout)
        except HtndCommunicationError:
            await self.initialize_all()
            return await self.__request(command, params, timeout=timeout)

    async def notify(self, command, params, callback):
        htnd = self.__get_htnd()
        if htnd is not None:
            # Delegate to the selected client, avoid recursive self.notify
            return await htnd.notify(command, params, callback)
