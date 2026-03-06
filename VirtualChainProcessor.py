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
        self.start_block = block
        self.start_hash = block_hash

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
            if 'acceptedTransactionIds' in parent_chain_response and len(parent_chain_response['acceptedTransactionIds']) > 0:
                for transaction in parent_chain_response['acceptedTransactionIds']:
                    if 'acceptingBlockHash' in transaction:
                        parent_chain_blocks.append(transaction['acceptingBlockHash'])

                # find intersection of database blocks and virtual parent chain
                with session_maker() as s:
                    parent_chain_blocks_in_db = s.query(Block) \
                        .filter(Block.hash.in_(parent_chain_blocks)) \
                        .with_entities(Block.hash).all()
                    parent_chain_blocks_in_db = [x[0] for x in parent_chain_blocks_in_db]

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

                with session_maker() as s:
                    # set is_accepted to False, when blocks were removed from virtual parent chain
                    if rejected_blocks:
                        count = s.query(Transaction).filter(Transaction.accepting_block_hash.in_(rejected_blocks)) \
                            .update({'is_accepted': False, 'accepting_block_hash': None})
                        _logger.info(f'Set is_accepted=False for {count} TXs')
                        s.commit()

                    count_tx = 0    

                    # set is_accepted to True and add accepting_block_hash
                    for accepting_block_hash, accepted_tx_ids in accepted_ids:
                        s.query(Transaction).filter(Transaction.transaction_id.in_(accepted_tx_ids)) \
                            .update({'is_accepted': True, 'accepting_block_hash': accepting_block_hash})
                        count_tx += len(accepted_tx_ids)

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
        Add known blocks to database
        """
        if self.start_block is not None and self.start_block["verboseData"].get("isHeaderOnly") != True:
            _logger.info(f'VCP requested with start hash {self.start_hash}')
            resp = await self.client.request("getVirtualSelectedParentChainFromBlockRequest",
                                            {"startHash": self.start_hash,
                                            "includeAcceptedTransactionIds": True},
                                            timeout=30)
            # if there is a response, add to queue and set new startpoint
            error = resp["getVirtualSelectedParentChainFromBlockResponse"].get("error", None)
            if error is None:
                _logger.info(f'Got VCP response with: '
                            f'{len(resp["getVirtualSelectedParentChainFromBlockResponse"].get("acceptedTransactionIds", []))}'
                            f' acceptedTransactionIds, '
                            f'{len(resp["getVirtualSelectedParentChainFromBlockResponse"].get("addedChainBlockHashes", []))}'
                            f' addedChainBlockHashes, '
                            f'{len(resp["getVirtualSelectedParentChainFromBlockResponse"].get("removedChainBlockHashes", []))}'
                            f' removedChainBlockHashes')
                self.virtual_chain_response = resp["getVirtualSelectedParentChainFromBlockResponse"]
                if self.virtual_chain_response is not None:
                    await self.__update_transactions_in_db()
            else:
                _logger.debug('getVirtualSelectedParentChain error response:')
                _logger.info(error["message"])
                self.virtual_chain_response = None
                await asyncio.sleep(10)

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