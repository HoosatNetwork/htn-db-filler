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
                with session_maker() as s:
                    parent_chain_blocks_in_db = []
                    CHUNK = 500
                    if parent_chain_blocks:
                        for i in range(0, len(parent_chain_blocks), CHUNK):
                            chunk = parent_chain_blocks[i:i + CHUNK]
                            rows = s.query(Block).filter(Block.hash.in_(chunk)).with_entities(Block.hash).all()
                            parent_chain_blocks_in_db.extend([x[0] for x in rows])

                # parent_chain_blocks_in_db = parent_chain_blocks_in_db[:200]

                # go through all acceptedTransactionIds and stop if a block hash is not in database
                for tx_accept_dict in parent_chain_response['acceptedTransactionIds']:
                    accepting_block_hash = tx_accept_dict['acceptingBlockHash']

                    if accepting_block_hash not in parent_chain_blocks_in_db:
                        continue  # Stop once we reached a non-existing block

                    accepted_ids.append((tx_accept_dict['acceptingBlockHash'], tx_accept_dict["acceptedTransactionIds"]))

                    last_known_chain_block = accepting_block_hash
                    if len(accepted_ids) >= 1500:
                        _logger.info(f'Length of accepted ids {len(accepted_ids)}')
                        break

                # add rejected blocks if needed
                rejected_blocks.extend(parent_chain_response.get('removedChainBlockHashes', []))
                _logger.info(f'VCP: {len(rejected_blocks)} removedChainBlockHashes to process')

                with session_maker() as s:
                    # set is_accepted to False, when blocks were removed from virtual parent chain
                    CHUNK = 500
                    if rejected_blocks:
                        total = 0
                        for i in range(0, len(rejected_blocks), CHUNK):
                            chunk = rejected_blocks[i:i + CHUNK]
                            # use synchronize_session=False for bulk update
                            c = s.query(Transaction).filter(Transaction.accepting_block_hash.in_(chunk)) \
                                .update({'is_accepted': False, 'accepting_block_hash': None}, synchronize_session=False)
                            total += c
                        _logger.info(f'Set is_accepted=False for {total} TXs')
                        s.commit()

                    count_tx = 0

                    # set is_accepted to True and add accepting_block_hash
                    for accepting_block_hash, accepted_tx_ids in accepted_ids:
                        if not accepted_tx_ids:
                            continue
                        for i in range(0, len(accepted_tx_ids), CHUNK):
                            chunk = accepted_tx_ids[i:i + CHUNK]
                            c = s.query(Transaction).filter(Transaction.transaction_id.in_(chunk)) \
                                .update({'is_accepted': True, 'accepting_block_hash': accepting_block_hash}, synchronize_session=False)
                            count_tx += c

                    _logger.info(f'Set is_accepted=True for {count_tx} transactions.')
                    s.commit()
                
                # Clear the current response
                self.virtual_chain_response = None

                # Mark last known/processed as start point for the next query
                if last_known_chain_block:
                    _logger.info(f'Setting new start point {last_known_chain_block} for VCP')
                    KeyValueStore.set("vspc_last_start_hash", last_known_chain_block)
                    self.start_hash = last_known_chain_block
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
            timeout=30
        )

        if resp is None:
            _logger.warning("VCP: empty response from getVirtualSelectedParentChainFromBlockRequest")
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
            _logger.info(error.get("message", error))
            self.virtual_chain_response = None
            await asyncio.sleep(10)