# encoding: utf-8
import asyncio
import os
import logging
import time
from typing import List

from sqlalchemy import select
from dbsession import session_maker
from helper import KeyValueStore
from models.Block import Block
from models.Transaction import Transaction

_logger = logging.getLogger(__name__)

# How many recent DB blocks to probe on the node when the VCP start hash is unknown to it
VCP_RECOVERY_MAX_CANDIDATES = int(os.getenv("VCP_RECOVERY_MAX_CANDIDATES", "50"))

# The node returns the whole chain from start_hash to the tip, so an old start hash can take minutes
VCP_TIMEOUT_SECONDS = float(os.getenv("VCP_TIMEOUT_SECONDS", "600"))
VCP_RETRY_DELAY_SECONDS = float(os.getenv("VCP_RETRY_DELAY_SECONDS", "30"))
# Chain blocks to update per DB commit
VCP_BATCH_SIZE = int(os.getenv("VCP_BATCH_SIZE", "1500"))

class VirtualChainProcessor(object):
    """
    VirtualChainProcessor polls the command getVirtualSelectedParentChainFromBlockRequest and updates transactions
    with is_accepted False or True.

    To make sure all blocks are already in database, the VirtualChain processor has a prepare function, which is
    basically a temporary storage. This buffer should be processed AFTER the blocks and transactions are added.
    """

    def __init__(self, client, start_hash, start_block=None):
        """
        start_hash: the string hash to start VCP from (preferred).
        start_block: optional legacy dict (only used to extract hash if start_hash is None).
        """
        self.virtual_chain_response = None
        self.client = client

        # Prefer explicit start_hash (string). Fall back to extracting from legacy start_block dict.
        if start_hash:
            self.start_hash = start_hash
        elif isinstance(start_block, dict):
            # legacy path: try to get hash from the block dict
            self.start_hash = start_block.get("verboseData", {}).get("hash") or start_block.get("hash")
            _logger.debug(f"VCP using legacy start_block dict to set start_hash={self.start_hash}")
        else:
            self.start_hash = None
            if start_block is not None:
                _logger.warning(f"VCP __init__: start_block param was not a dict and no start_hash given. "
                                f"Got type {type(start_block)}. start_hash remains None until set.")

    async def set_start_block(self, block_or_hash, block_hash=None):
        """
        Set the starting point for VCP.
        Accepts either:
          - a full block dict (legacy) + optional block_hash
          - or just the block_hash string (preferred, as start_block should be string)
        """
        if isinstance(block_or_hash, dict):
            # legacy dict path
            self.start_hash = block_hash or block_or_hash.get("verboseData", {}).get("hash") \
                              or block_or_hash.get("hash")
            _logger.debug(f"VCP start_hash set from block dict to {self.start_hash[:8] if self.start_hash else None}...")
        elif isinstance(block_or_hash, str):
            self.start_hash = block_or_hash
            _logger.debug(f"VCP start_hash set to {self.start_hash[:8]}...")
        else:
            _logger.warning(f"VCP set_start_block: unexpected type {type(block_or_hash)}. "
                            f"block_hash={block_hash[:8] if block_hash else None}")
            return

        # Clear any stale block dict reference (we no longer store the full dict)
        if hasattr(self, 'start_block'):
            self.start_block = None

    @staticmethod
    def _blocks_in_db(block_hashes, chunk_size=500):
        in_db = set()
        with session_maker() as s:
            for i in range(0, len(block_hashes), chunk_size):
                chunk = block_hashes[i:i + chunk_size]
                rows = s.query(Block).filter(Block.hash.in_(chunk)).with_entities(Block.hash).all()
                in_db.update(x[0] for x in rows)
        return in_db

    @staticmethod
    def _reject_blocks(rejected_blocks, chunk_size=500):
        # set is_accepted to False, when blocks were removed from virtual parent chain
        total = 0
        with session_maker() as s:
            for i in range(0, len(rejected_blocks), chunk_size):
                chunk = rejected_blocks[i:i + chunk_size]
                # use synchronize_session=False for bulk update
                total += s.query(Transaction).filter(Transaction.accepting_block_hash.in_(chunk)) \
                    .update({'is_accepted': False, 'accepting_block_hash': None}, synchronize_session=False)
            s.commit()
        return total

    @staticmethod
    def _accept_batch(batch, new_start_hash, chunk_size=500):
        count_tx = 0
        with session_maker() as s:
            for accepting_block_hash, accepted_tx_ids in batch:
                if not accepted_tx_ids:
                    continue
                for i in range(0, len(accepted_tx_ids), chunk_size):
                    chunk = accepted_tx_ids[i:i + chunk_size]
                    count_tx += s.query(Transaction).filter(Transaction.transaction_id.in_(chunk)) \
                        .update({'is_accepted': True, 'accepting_block_hash': accepting_block_hash},
                                synchronize_session=False)
            s.commit()
        KeyValueStore.set("vspc_last_start_hash", new_start_hash)
        return count_tx

    async def __update_transactions_in_db(self):
        """
        goes through one parentChainResponse and updates the is_accepted field in the database.
        """
        accepted_ids = []
        rejected_blocks = []
        last_known_chain_block = None

        parent_chain_response = self.virtual_chain_response
        # Find parent chain blocks.
        parent_chain_blocks = []
        if parent_chain_response is not None:
            _logger.debug("Updating transactions in db")
            accepted_tx_list = parent_chain_response.get('acceptedTransactionIds', [])
            _logger.info(f'VCP: received {len(accepted_tx_list)} acceptedTransactionIds; building parent chain list')
            if 'acceptedTransactionIds' in parent_chain_response and len(parent_chain_response['acceptedTransactionIds']) > 0:
                for transaction in parent_chain_response['acceptedTransactionIds']:
                    if 'acceptingBlockHash' in transaction:
                        parent_chain_blocks.append(transaction['acceptingBlockHash'])

                # find intersection of database blocks and virtual parent chain
                _logger.info(f'VCP: chain in response runs from {parent_chain_blocks[0]} '
                             f'to {parent_chain_blocks[-1]} (start hash {self.start_hash})')

                # DB work runs in a worker thread so it doesn't block the event loop (blocks processor, gRPC)
                parent_chain_blocks_in_db = await asyncio.to_thread(self._blocks_in_db, parent_chain_blocks)

                # go through all acceptedTransactionIds and stop at the first block not yet in database,
                # so the start point never moves past chain blocks the blocks processor hasn't stored yet
                for tx_accept_dict in parent_chain_response['acceptedTransactionIds']:
                    accepting_block_hash = tx_accept_dict['acceptingBlockHash']

                    if accepting_block_hash not in parent_chain_blocks_in_db:
                        _logger.info(f'VCP: chain block {accepting_block_hash} not in database yet, '
                                     f'stopping here')
                        break

                    accepted_ids.append((accepting_block_hash, tx_accept_dict.get("acceptedTransactionIds", [])))

                _logger.info(f'VCP: {len(accepted_ids)} of {len(parent_chain_blocks)} chain blocks are in database '
                             f'({len(parent_chain_blocks_in_db)} in total incl. after first gap)')

                # add rejected blocks if needed
                rejected_blocks.extend(parent_chain_response.get('removedChainBlockHashes', []))
                _logger.info(f'VCP: {len(rejected_blocks)} removedChainBlockHashes to process')

                if rejected_blocks:
                    total = await asyncio.to_thread(self._reject_blocks, rejected_blocks)
                    _logger.info(f'Set is_accepted=False for {total} TXs')

                # set is_accepted to True and add accepting_block_hash, committing and saving
                # the start point per batch so progress survives restarts
                for b in range(0, len(accepted_ids), VCP_BATCH_SIZE):
                    batch = accepted_ids[b:b + VCP_BATCH_SIZE]
                    last_known_chain_block = batch[-1][0]
                    count_tx = await asyncio.to_thread(self._accept_batch, batch, last_known_chain_block)
                    self.start_hash = last_known_chain_block
                    _logger.info(f'Set is_accepted=True for {count_tx} transactions '
                                 f'({b + len(batch)}/{len(accepted_ids)} chain blocks). '
                                 f'New start point {last_known_chain_block}')

                # Clear the current response
                self.virtual_chain_response = None

                if last_known_chain_block:
                    await asyncio.sleep(30)


    async def yield_to_database(self):
        """
        Perform VCP update using the current start_hash (string).
        The start_block dict is no longer required (start_block should be / is treated as string hash).
        We always attempt the request if we have a start_hash; header-only edge cases are handled by the node.
        """
        if not self.start_hash:
            _logger.debug("VCP: start_hash is None/empty, skipping yield_to_database this round.")
            return

        _logger.info(f'VCP requested with start hash {self.start_hash}')
        resp = await self.client.request(
            "getVirtualSelectedParentChainFromBlockRequest",
            {"startHash": self.start_hash, "includeAcceptedTransactionIds": True},
            timeout=VCP_TIMEOUT_SECONDS
        )

        if resp is None:
            _logger.warning(f"VCP: empty response from getVirtualSelectedParentChainFromBlockRequest. "
                            f"Retrying in {VCP_RETRY_DELAY_SECONDS}s.")
            await asyncio.sleep(VCP_RETRY_DELAY_SECONDS)
            return

        vcp_resp = resp.get("getVirtualSelectedParentChainFromBlockResponse", {})
        error = vcp_resp.get("error", None)

        if error is None:
            _logger.info(f'Got VCP response with: '
                        f'{len(vcp_resp.get("acceptedTransactionIds", []))} acceptedTransactionIds, '
                        f'{len(vcp_resp.get("addedChainBlockHashes", []))} addedChainBlockHashes, '
                        f'{len(vcp_resp.get("removedChainBlockHashes", []))} removedChainBlockHashes')

            self.virtual_chain_response = vcp_resp
            if self.virtual_chain_response is not None:
                await self.__update_transactions_in_db()
        else:
            _logger.debug('getVirtualSelectedParentChain error response:')
            message = error.get("message", str(error)) if isinstance(error, dict) else str(error)
            _logger.info(message)
            self.virtual_chain_response = None
            if "does not exist" in message or "not found" in message:
                # The node doesn't know the start block (pruned, orphaned or from another node).
                # Retrying with the same hash would never succeed, so pick a new start point.
                await self.__recover_start_hash()
            else:
                await asyncio.sleep(10)

    async def __block_exists_on_node(self, block_hash):
        resp = await self.client.request("getBlockRequest",
                                         {"hash": block_hash, "includeTransactions": False},
                                         timeout=10)
        if resp is None:
            return False
        block_resp = resp.get("getBlockResponse", {})
        if block_resp.get("error") is not None:
            return False
        block = block_resp.get("block")
        if not block:
            return False
        return block.get("verboseData", {}).get("isHeaderOnly") is not True

    async def __recover_start_hash(self):
        """
        Find a start hash the node can build the virtual chain from.
        Walks back through blocks in the database (from the failing block's blue score downwards),
        falling back to the node's pruning point, then the virtual parent.
        """
        bad_hash = self.start_hash
        new_hash = None

        with session_maker() as s:
            bad_score = s.query(Block.blue_score).filter(Block.hash == bad_hash).scalar()
            q = s.query(Block.hash).filter(Block.hash != bad_hash)
            if bad_score is not None:
                q = q.filter(Block.blue_score <= bad_score)
            candidates = [row[0] for row in q.order_by(Block.blue_score.desc())
                          .limit(VCP_RECOVERY_MAX_CANDIDATES).all()]

        for candidate in candidates:
            if await self.__block_exists_on_node(candidate):
                new_hash = candidate
                break

        if new_hash is None:
            daginfo = await self.client.request("getBlockDagInfoRequest", {})
            info = (daginfo or {}).get("getBlockDagInfoResponse", {})
            new_hash = info.get("pruningPointHash") or (info.get("virtualParentHashes") or [None])[0]

        if new_hash is None:
            _logger.warning(f'VCP: could not find a replacement for start hash {bad_hash}. Retrying later.')
            await asyncio.sleep(10)
            return

        _logger.warning(f'VCP: start hash {bad_hash} not found on node. Switching to {new_hash}.')
        self.start_hash = new_hash
        KeyValueStore.set("vspc_last_start_hash", new_hash)