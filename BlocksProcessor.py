
import asyncio
import logging
import sys
from datetime import datetime
import time

from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import SQLAlchemyError

from dbsession import session_maker
from models.Block import Block
from models.Transaction import Transaction, TransactionOutput, TransactionInput
from utils.Event import Event

_logger = logging.getLogger(__name__)

# For 5 BPS
# CLUSTER_SIZE = 4
# CLUSTER_WAIT_SECONDS = 1

# For 1 BPS
CLUSTER_SIZE = 5
CLUSTER_WAIT_SECONDS = 15

B_TREE_SIZE = 2500

task_runner = None

class BlocksProcessor(object):
    """
    BlocksProcessor polls hoosat for blocks and adds the meta information and it's transactions into database.
    """

    def __init__(self, client, vcp_instance, balance, batch_processing = False, env_enable_balance = False):
        self.client = client
        self.blocks_to_add = []
        self.balance = balance
        self.addresses_to_update = []

        self.txs = {}
        self.txs_output = []
        self.txs_input = []
        self.vcp = vcp_instance
        self.env_enable_balance = env_enable_balance
        self.batch_processing = batch_processing
        self.start_hash_set = False

        # Did the loop already see the DAG tip
        self.synced = False

    async def loop(self, start_point):
        # go through each block added to DAG
        _logger.info('Start processing blocks from %s', start_point)
        async for block_hash, block in self.blockiter(start_point):
            # prepare add block and tx to database
            await self.__add_block_to_queue(block_hash, block)
            await self.__add_tx_to_queue(block_hash, block)
            if block["verboseData"].get("isHeaderOnly") != True and self.start_hash_set == False: 
                await self.vcp.set_start_block(block, block_hash)
                self.start_hash_set = True
            # if cluster size is reached, insert to database
            cluster_size = CLUSTER_SIZE
            if not self.synced:
                cluster_size = 403
            if len(self.blocks_to_add) >= cluster_size:
                _logger.debug(f'Committing {cluster_size} blocks at {block_hash}')
                await self.commit_blocks()
                if self.batch_processing == False:
                    await self.commit_txs()
                else: 
                    await self.batch_commit_txs()
                asyncio.create_task(self.handle_blocks_committed())
                # Update balances whenever a cluster is committed, not only at tip
                if self.env_enable_balance != False:
                    # Enqueue for background balance worker; do not block block processing
                    self.commit_balances(self.addresses_to_update)
                    

    def commit_balances(self, addresses):
        try:
            unique_addresses = list(set(addresses or []))
            if not unique_addresses:
                return

            # Prefer threaded/batched background processing when available
            if hasattr(self.balance, "enqueue_balance_updates"):
                _logger.info(f"Enqueueing {len(unique_addresses)} addresses for balance update")
                self.balance.enqueue_balance_updates(unique_addresses)
            else:
                # Best-effort fallback: run async updater in background
                asyncio.create_task(self.balance.update_balance_from_rpc(unique_addresses))

            # After enqueuing balances for the cluster, clear for next round
            self.addresses_to_update = []
        except Exception as e:
            _logger.error(f'Error enqueueing balances for addresses {len(unique_addresses) if "unique_addresses" in locals() else "?"}: {e}')
        

    async def handle_blocks_committed(self):
        """
        this function is executed, when a new cluster of blocks were added to the database
        """
        global task_runner
        while task_runner and not task_runner.done():
            return
        task_runner = asyncio.create_task(self.vcp.yield_to_database())

    async def blockiter(self, start_point):
        """
        generator for iterating the blocks added to the blockDAG
        """
        low_hash = start_point
        while True:
            _logger.info('Requesting with low hash block %s.', low_hash)
            daginfo = await self.client.request("getBlockDagInfoRequest", {})
            if daginfo != None:
                resp = await self.client.request("getBlocksRequest",
                                                params={
                                                    "lowHash": low_hash,
                                                    "includeTransactions": True,
                                                    "includeBlocks": True
                                                },
                                                timeout=30)
                # go through each block and yield
                if resp != None:
                    block_response = resp.get("getBlocksResponse", None)
                    if block_response != None:
                        block_hashes = block_response.get("blockHashes", [])
                        _logger.info(f'Received {len(block_hashes)} blocks from getBlocksResponse')
                        blocks = block_response.get("blocks", [])
                        for i, blockHash in enumerate(block_hashes):
                            if daginfo["getBlockDagInfoResponse"]["tipHashes"][0] == blockHash:
                                _logger.info('Found tip hash. Generator is synced now.')
                                self.synced = True
                                break # Dont iterate over the tipHash, because getBlock request returns old blocks. 
                            # yield blockhash and it's data
                            yield blockHash, blocks[i]
                        if self.synced: 
                            low_hash = daginfo["getBlockDagInfoResponse"]["tipHashes"][0]
                            self.synced = False
                            _logger.info(f'Waiting for the next blocks request.')
                            await asyncio.sleep(CLUSTER_WAIT_SECONDS)
                            _logger.info('New low hash block %s.', low_hash)
                        else:
                            if len(block_hashes) > 1:
                                low_hash = block_hashes[len(block_hashes) - 1]
                            _logger.info('New low hash block %s.', low_hash)
                else:
                    await asyncio.sleep(CLUSTER_WAIT_SECONDS * 2)
                    raise RuntimeError("Forced crash: No valid response from getBlocksRequest")
            await asyncio.sleep(1)

    async def __add_tx_to_queue(self, block_hash, block):
        """
        Adds block's transactions to queue. This is only prepartion without commit!
        """
        # Accumulate addresses across the whole cluster; do not reset here.
        if block.get("transactions") is not None:
            for transaction in block["transactions"]:
                if transaction.get("verboseData") is not None:
                    tx_id = transaction["verboseData"]["transactionId"]

                    # Check, that the transaction isn't prepared yet. Otherwise ignore
                    if not self.is_tx_id_in_queue(tx_id):
                        # Add transaction
                        if transaction["subnetworkId"] == "0300000000000000000000000000000000000000":
                            self.txs[tx_id] = Transaction(subnetwork_id=transaction["subnetworkId"],
                                                    transaction_id=tx_id,
                                                    hash=transaction["verboseData"]["hash"],
                                                    mass=transaction["verboseData"].get("mass"),
                                                    block_hash=[transaction["verboseData"]["blockHash"]],
                                                    block_time=int(transaction["verboseData"]["blockTime"]),
                                                    payload=transaction.get("payload"))
                        else:
                            self.txs[tx_id] = Transaction(subnetwork_id=transaction["subnetworkId"],
                                                    transaction_id=tx_id,
                                                    hash=transaction["verboseData"]["hash"],
                                                    mass=transaction["verboseData"].get("mass"),
                                                    block_hash=[transaction["verboseData"]["blockHash"]],
                                                    block_time=int(transaction["verboseData"]["blockTime"]))
                        for index, out in enumerate(transaction.get("outputs", [])):
                            address = out["verboseData"].get("scriptPublicKeyAddress")
                            amount = out["amount"]
                            if self.env_enable_balance != False: 
                                if address not in self.addresses_to_update:
                                    self.addresses_to_update.append(address)
                            self.txs_output.append(TransactionOutput(transaction_id=tx_id,
                                                                    index=index,
                                                                    amount=amount,
                                                                    script_public_key=out["scriptPublicKey"]["scriptPublicKey"],
                                                                    script_public_key_address=address,
                                                                    script_public_key_type=out["verboseData"].get("scriptPublicKeyType")))

                        for index, tx_in in enumerate(transaction.get("inputs", [])):
                            if self.env_enable_balance != False: 
                                prev_out_tx_id = tx_in["previousOutpoint"]["transactionId"]
                                prev_out_index = int(tx_in["previousOutpoint"].get("index", 0))
                                with session_maker() as session:
                                    prev_output = session.query(TransactionOutput).filter_by(
                                        transaction_id=prev_out_tx_id,
                                        index=prev_out_index
                                    ).first()
                                    if prev_output:
                                        address = prev_output.script_public_key_address
                                        if address not in self.addresses_to_update:
                                            self.addresses_to_update.append(address)
                            self.txs_input.append(TransactionInput(transaction_id=tx_id,
                                                                    index=index,
                                                                    previous_outpoint_hash=tx_in["previousOutpoint"]["transactionId"],
                                                                    previous_outpoint_index=int(tx_in["previousOutpoint"].get("index", 0)),
                                                                    signature_script=tx_in["signatureScript"],
                                                                    sig_op_count=tx_in.get("sigOpCount", 0)))
                    else:
                        # If the block if already in the Queue, merge the block_hashes.
                        self.txs[tx_id].block_hash = list(set(self.txs[tx_id].block_hash + [block_hash]))

    async def batch_commit_txs(self):
        """
        Add all queued transactions and their in- and outputs to the database in batches
        to avoid exceeding PostgreSQL limits.
        """
        BATCH_SIZE = 5  # Define a suitable batch size

        # First, handle updates for existing transactions.
        tx_ids_to_add = list(self.txs.keys())

        # Calculate the number of batches needed for updating existing transactions
        num_batches = len(tx_ids_to_add) // BATCH_SIZE + (1 if len(tx_ids_to_add) % BATCH_SIZE > 0 else 0)

        # Handle updates for existing transactions in batches
        for i in range(num_batches):
            with session_maker() as session:
                # Determine the subset of transaction IDs for this batch
                batch_tx_ids = tx_ids_to_add[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]

                # Query only the transactions in the current batch
                tx_items = session.query(Transaction).filter(Transaction.transaction_id.in_(batch_tx_ids)).all()

                for tx_item in tx_items:
                    # Update block_hash by combining existing ones with new ones from self.txs
                    new_block_hashes = list(set(tx_item.block_hash) | set(self.txs[tx_item.transaction_id].block_hash))
                    tx_item.block_hash = new_block_hashes
                    # Remove the transaction from self.txs since it's now been processed
                    self.txs.pop(tx_item.transaction_id)

                # Commit the updates for this batch
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
                    _logger.error(f'Error updating transactions in batch {i+1}/{num_batches}: {e}')

        # Pre-map outputs and inputs to their transaction IDs for new transactions
        outputs_by_tx = {tx_id: [] for tx_id in self.txs.keys()}
        for output in self.txs_output:
            if output.transaction_id in outputs_by_tx:
                outputs_by_tx[output.transaction_id].append(output)

        inputs_by_tx = {tx_id: [] for tx_id in self.txs.keys()}
        for input in self.txs_input:
            if input.transaction_id in inputs_by_tx:
                inputs_by_tx[input.transaction_id].append(input)

        # Now, handle insertion of new transactions in batches.
        all_new_txs = list(self.txs.values())

        # Insert new transactions and their inputs/outputs in batches
        for i in range(0, len(all_new_txs), BATCH_SIZE):
            with session_maker() as session:
                batch_txs = all_new_txs[i:i + BATCH_SIZE]
                batch_tx_ids = [tx.transaction_id for tx in batch_txs]

                batch_outputs = [output for tx_id in batch_tx_ids for output in outputs_by_tx[tx_id]]
                batch_inputs = [input for tx_id in batch_tx_ids for input in inputs_by_tx[tx_id]]

                # Add all new transactions, outputs, and inputs to the session
                session.add_all(batch_txs)
                session.add_all(batch_outputs)
                session.add_all(batch_inputs)

                try:
                    session.commit()
                    # _logger.info(f'Added {len(batch_txs)} TXs to database in a batch.')
                except Exception as e:
                    session.rollback()
                    _logger.error(f'Error adding TXs to database in a batch {i+1}/{num_batches}: {e}')

        # Reset queues after all batches have been processed.
        self.txs = {}
        self.txs_input = []
        self.txs_output = []

    # Original commit_txs
    async def commit_txs(self):
        """
        Add all queued transactions and their in- and outputs to the database
        """
        tx_ids_to_add = list(self.txs.keys())
        
        # Use a single session and avoid duplicate transactions
        with session_maker() as session:
            # Check if any transactions already exist in the database
            tx_items = session.query(Transaction).filter(Transaction.transaction_id.in_(tx_ids_to_add)).all()
            
            # Update existing transactions (if any) and remove them from the queue
            for tx_item in tx_items:
                tx_item.block_hash = list(set(tx_item.block_hash) | set(self.txs[tx_item.transaction_id].block_hash))
                self.txs.pop(tx_item.transaction_id)

            # Now add new transactions and their inputs/outputs
            for txv in self.txs.values():
                session.add(txv)  # This will insert new or update existing transaction

            # Add related outputs and inputs
            for tx_output in self.txs_output:
                if tx_output.transaction_id in self.txs:
                    session.add(tx_output)

            for tx_input in self.txs_input:
                if tx_input.transaction_id in self.txs:
                    session.add(tx_input)

            try:
                session.commit()  # Commit only once after all changes
                # Reset queues
                self.txs = {}
                self.txs_input = []
                self.txs_output = []

            except IntegrityError:
                session.rollback()
                _logger.error('Error adding TXs to database')
                raise

    async def __add_block_to_queue(self, block_hash, block):
        """
        Adds a block to the queue, which is used for adding a cluster
        """

        if 'parents' in block["header"] and block["header"]["parents"]:
            parent_hashes = block["header"]["parents"][0].get("parentHashes", [])
        else:
            parent_hashes = []
        block_entity = Block(hash=block_hash,
                             accepted_id_merkle_root=block["header"]["acceptedIdMerkleRoot"],
                             difficulty=block["verboseData"]["difficulty"],
                             is_chain_block=block["verboseData"].get("isChainBlock", False),
                             merge_set_blues_hashes=block["verboseData"].get("mergeSetBluesHashes", []),
                             merge_set_reds_hashes=block["verboseData"].get("mergeSetRedsHashes", []),
                             selected_parent_hash=block["verboseData"]["selectedParentHash"],
                             bits=block["header"]["bits"],
                             blue_score=int(block["header"].get("blueScore", 0)),
                             blue_work=block["header"]["blueWork"],
                             daa_score=int(block["header"].get("daaScore", 0)),
                             hash_merkle_root=block["header"]["hashMerkleRoot"],
                             nonce=block["header"].get("nonce", 0),
                             parents=parent_hashes,
                             pruning_point=block["header"]["pruningPoint"],
                             timestamp=datetime.fromtimestamp(int(block["header"]["timestamp"]) / 1000).isoformat(),
                             utxo_commitment=block["header"]["utxoCommitment"],
                             version=block["header"].get("version", 0))

        # remove same block hash
        self.blocks_to_add = [b for b in self.blocks_to_add if b.hash != block_hash]
        self.blocks_to_add.append(block_entity)

    async def commit_blocks(self):
        """
        Insert queued blocks to database only if they don't already exist
        """
        with session_maker() as session:
            try:
                blocks_to_insert = []
                block_hashes = [b.hash for b in self.blocks_to_add]
                _logger.debug(f'Checking blocks with hashes: {block_hashes}')
                
                # Check which blocks already exist
                existing_hashes = set(
                    session.query(Block.hash)
                    .filter(Block.hash.in_(block_hashes))
                    .all()
                )
                existing_hashes = {h[0] for h in existing_hashes}
                
                # Only add blocks that don't exist
                for block in self.blocks_to_add:
                    if block.hash not in existing_hashes:
                        blocks_to_insert.append(block)
                
                if blocks_to_insert:
                    for block in blocks_to_insert:
                        session.add(block)
                    session.commit()
                    _logger.debug(f'Added {len(blocks_to_insert)} new blocks to database. '
                                f'Timestamp: {blocks_to_insert[-1].timestamp}')
                else:
                    _logger.debug('No new blocks to add to database.')
                
                self.blocks_to_add = []
            except IntegrityError as e:
                session.rollback()
                _logger.error(f'Error adding group of blocks: {e}')
                raise
            except Exception as e:
                session.rollback()
                _logger.error(f'Unexpected error committing blocks: {e}')
                raise
        

    def is_tx_id_in_queue(self, tx_id):
        """
        Checks if given TX ID is already in the queue
        """
        return tx_id in self.txs