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

    def __init__(self, client, start_block, start_hash):
        self.virtual_chain_response = None
        self.start_hash = start_hash
        self.client = client
        self.start_block = start_block

    async def set_start_block(self, block, block_hash):
        """Safely set the start block. Only accepts dicts."""
        if isinstance(block, dict):
            self.start_block = block
            self.start_hash = block_hash
            _logger.debug(f"VCP start_block set to block {block_hash[:8]}...")
        else:
            _logger.warning(f"Ignoring bad start_block type: {type(block)}. Expected dict. "
                            f"block_hash={block_hash[:8] if block_hash else None}")

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
        Add known blocks to database.
        Now safely handles the case where start_block might not be a proper dict.
        """
        # === DEFENSIVE CHECK ===
        if self.start_block is None:
            _logger.debug("VCP: start_block is None, skipping yield_to_database this round.")
            return

        if not isinstance(self.start_block, dict):
            _logger.error(f"VCP BUG: start_block has wrong type {type(self.start_block)} "
                        f"(expected dict). This usually means it was passed incorrectly "
                        f"at construction time. Value (first 200 chars): {str(self.start_block)[:200]}")
            return

        # Original logic, now safe
        if self.start_block.get("verboseData", {}).get("isHeaderOnly") != True:
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
        else:
            _logger.debug("VCP: start_block is header-only, skipping this round.")

    # async def yield_to_database(self, max_retries=5000000):
    #     """
    #     Add known blocks to database by iteratively finding a valid start_hash.
        
    #     Args:
    #         max_retries (int): Maximum number of retry attempts to find a valid start_hash.
    #     """
    #     _logger.info(f'VCP requested with start hash {self.start_hash}')
    #     current_hash = self.start_hash
    #     retries = 0

    #     while retries < max_retries:
    #         # Send getVirtualSelectedParentChainFromBlockRequest
    #         resp = await self.client.request(
    #             "getVirtualSelectedParentChainFromBlockRequest",
    #             {"startHash": current_hash, "includeAcceptedTransactionIds": True},
    #             timeout=240
    #         )
            
    #         # Check for error in response
    #         error = resp["getVirtualSelectedParentChainFromBlockResponse"].get("error", None)
    #         if error is None:
    #             # Success: Process the response
    #             _logger.info(
    #                 f'Got VCP response with: '
    #                 f'{len(resp["getVirtualSelectedParentChainFromBlockResponse"].get("acceptedTransactionIds", []))} '
    #                 f'acceptedTransactionIds, '
    #                 f'{len(resp["getVirtualSelectedParentChainFromBlockResponse"].get("addedChainBlockHashes", []))} '
    #                 f'addedChainBlockHashes, '
    #                 f'{len(resp["getVirtualSelectedParentChainFromBlockResponse"].get("removedChainBlockHashes", []))} '
    #                 f'removedChainBlockHashes'
    #             )
    #             self.virtual_chain_response = resp["getVirtualSelectedParentChainFromBlockResponse"]
    #             self.start_hash = current_hash  # Update start_hash to the successful one
    #             await self.__update_transactions_in_db()
    #             return self.virtual_chain_response

    #         # Error: Log and try to find a new start_hash
    #         _logger.debug('getVirtualSelectedParentChain error response:')
    #         _logger.info(error["message"])
            
    #         # Request blocks data starting from the current hash
    #         resp = await self.client.request(
    #             "getBlocksRequest",
    #             params={
    #                 "lowHash": current_hash,
    #                 "includeTransactions": False,
    #                 "includeBlocks": True
    #             },
    #             timeout=60
    #         )
            
    #         block_hashes = resp["getBlocksResponse"].get("blockHashes", [])
    #         _logger.info(f'Received {len(block_hashes)} blocks from getBlocksResponse')
    #         blocks = resp["getBlocksResponse"].get("blocks", [])
            
    #         if not blocks:
    #             _logger.warning(f"No blocks in getBlocksResponse for lowHash {current_hash}")
    #             return []

    #         # Get children hashes from the last block to skip forward
    #         last_block = blocks[-1]
    #         children = last_block.get("verboseData", {}).get("childrenHashes", [])
    #         if not children:
    #             _logger.warning(f"No children found for hash {last_block.get('hash')}")
    #             return []

    #         # Try the first child hash of the last block in the next iteration
    #         current_hash = children[0]
    #         retries += len(blocks)
    #         _logger.info(f'Retrying with new start_hash {current_hash} (attempt {retries + 1}/{max_retries})')

    #     # Exhausted retries or no children available
    #     _logger.error(f"Failed to find a valid start_hash after {max_retries} retries")
    #     self.virtual_chain_response = None
    #     return []