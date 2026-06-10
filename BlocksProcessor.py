
import asyncio
import logging
import sys
import os
from datetime import datetime
import time

from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy import text, bindparam

from dbsession import session_maker, engine
from models.Block import Block
from models.Transaction import Transaction, TransactionOutput, TransactionInput
from utils.Event import Event

_logger = logging.getLogger(__name__)

# For 5 BPS
# CLUSTER_SIZE = 4
# CLUSTER_WAIT_SECONDS = 1

# For 1 BPS
CLUSTER_SIZE = 5
CATCH_UP_CLUSTER_SIZE = 500
CLUSTER_WAIT_SECONDS = 1

B_TREE_SIZE = 2500

# Number of blocks to insert per DB batch. Use smaller batches to avoid large executemany failures.
BLOCK_COMMIT_BATCH = int(os.getenv("BLOCK_COMMIT_BATCH", "100"))

# DB timeouts for best-effort mapping inserts (milliseconds)
LOCK_TIMEOUT_MS = int(os.getenv("TX_ADDR_MAPPING_LOCK_TIMEOUT_MS", "200"))
STATEMENT_TIMEOUT_MS = int(os.getenv("TX_ADDR_MAPPING_STATEMENT_TIMEOUT_MS", "5000"))

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

        # NEW: Track last processed block time to prevent going backwards
        self.last_block_time = 0
        self.max_time_drift_seconds = 3600 * 0.5

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
            cluster_size = CATCH_UP_CLUSTER_SIZE if not self.synced else CLUSTER_SIZE
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
        low_hash = start_point
        while True:
            _logger.info('Requesting with low hash block %s.', low_hash)
            daginfo = await self.client.request("getBlockDagInfoRequest", {})
            if daginfo is None:
                await asyncio.sleep(2)
                continue

            tip_hash = daginfo["getBlockDagInfoResponse"]["tipHashes"][0]
            start_req = time.time()
            resp = await self.client.request("getBlocksRequest", {
                "lowHash": low_hash,
                "includeTransactions": True,
                "includeBlocks": True
            }, timeout=30)
            req_duration = time.time() - start_req
            _logger.debug(f'getBlocksRequest took {req_duration:.3f}s for lowHash {low_hash[:8]}...')

            if resp is None:
                _logger.error("No valid response from getBlocksRequest. Retrying...")
                await asyncio.sleep(2)
                continue

            block_response = resp.get("getBlocksResponse", {})
            block_hashes = block_response.get("blockHashes", [])
            blocks = block_response.get("blocks", [])

            _logger.info(f'Received {len(block_hashes)} blocks. Tip: {tip_hash[:8]}...')

            advanced = False
            for i, blockHash in enumerate(block_hashes):
                block_data = blocks[i]
                block_time = int(block_data["header"]["timestamp"]) // 1000

                # NEW: Strong time-based guard
                if self.last_block_time > 0 and block_time < self.last_block_time - self.max_time_drift_seconds:
                    _logger.warning(f"Block {blockHash[:8]}... has old timestamp ({block_time}). Skipping.")
                    continue

                if blockHash == tip_hash:
                    _logger.info('Found tip hash. Generator is synced now.')
                    self.synced = True
                    break

                # Yield the block
                yield blockHash, block_data

                # Update last seen time
                if block_time >= self.last_block_time:
                    self.last_block_time = block_time

                advanced = True

            # Update low_hash safely
            if self.synced:
                low_hash = tip_hash
                _logger.info(f'Synced to tip. Waiting {CLUSTER_WAIT_SECONDS}s...')
                await asyncio.sleep(CLUSTER_WAIT_SECONDS)
            else:
                if advanced and block_hashes:
                    # Only advance to the last *newer* block
                    last_hash = block_hashes[-1]
                    last_time = int(blocks[-1]["header"]["timestamp"]) // 1000
                    if last_time >= self.last_block_time - 60:  # small tolerance
                        low_hash = last_hash
                        self.last_block_time = max(self.last_block_time, last_time)
                    else:
                        _logger.warning(f"Last returned block is old ({last_time}). Keeping current low_hash.")
                else:
                    _logger.info("No progress. Waiting...")
                    await asyncio.sleep(3)

            _logger.info(f'New low hash: {low_hash[:8]}... (last block time: {self.last_block_time})')

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
                        # Log queued transaction details
                        outputs_count = len(transaction.get("outputs", []))
                        inputs_count = len(transaction.get("inputs", []))
                        _logger.debug(f'Queued TX {tx_id}: outputs={outputs_count}, inputs={inputs_count}')
                        for index, out in enumerate(transaction.get("outputs", [])):
                            address = out["verboseData"].get("scriptPublicKeyAddress")
                            amount = out["amount"]
                            if self.env_enable_balance != False and self.synced == True: 
                                if address not in self.addresses_to_update:
                                    self.addresses_to_update.append(address)
                            self.txs_output.append(TransactionOutput(transaction_id=tx_id,
                                                                    index=index,
                                                                    amount=amount,
                                                                    script_public_key=out["scriptPublicKey"]["scriptPublicKey"],
                                                                    script_public_key_address=address,
                                                                    script_public_key_type=out["verboseData"].get("scriptPublicKeyType")))

                        for index, tx_in in enumerate(transaction.get("inputs", [])):
                            if self.env_enable_balance != False and self.synced == True: 
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
        BATCH_SIZE = int(os.getenv("TX_BATCH_SIZE", "50"))  # Define a suitable batch size (configurable via TX_BATCH_SIZE)

        # First, handle updates for existing transactions.
        tx_ids_to_add = list(self.txs.keys())

        # Calculate the number of batches needed for updating existing transactions
        num_batches = len(tx_ids_to_add) // BATCH_SIZE + (1 if len(tx_ids_to_add) % BATCH_SIZE > 0 else 0)

        # Handle updates for existing transactions in batches
        for i in range(num_batches):
            # Determine the subset of transaction IDs for this batch
            batch_tx_ids = tx_ids_to_add[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]

            if not batch_tx_ids:
                continue

            # Try once, and retry one time if we hit a transient "aborted transaction"/connection state.
            for attempt in range(2):
                session = session_maker()
                try:
                    # Query only the transactions in the current batch
                    tx_items = session.query(Transaction).filter(Transaction.transaction_id.in_(batch_tx_ids)).all()

                    for tx_item in tx_items:
                        # Update block_hash by combining existing ones with new ones from self.txs
                        if tx_item.transaction_id in self.txs:
                            new_block_hashes = list(set(tx_item.block_hash) | set(self.txs[tx_item.transaction_id].block_hash))
                            tx_item.block_hash = new_block_hashes
                            # Remove the transaction from self.txs since it's now been processed
                            self.txs.pop(tx_item.transaction_id)

                    # Commit the updates for this batch
                    session.commit()
                    session.close()
                    _logger.info(f'Committed {len(batch_tx_ids)} transactions in batch {i+1}-{min(i+BATCH_SIZE, len(tx_ids_to_add))}')
                    break
                except Exception as e:
                    # Ensure the session/connection is cleaned up and not returned to pool in an aborted state
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    try:
                        session.close()
                    except Exception:
                        pass

                    # If this looks like an aborted transaction state, dispose the engine
                    try:
                        msg = str(e) or repr(e)
                        if 'InFailedSqlTransaction' in msg or 'current transaction is aborted' in msg:
                            _logger.warning('Detected aborted DB transaction state; disposing engine pool to reset connections')
                            try:
                                engine.dispose()
                            except Exception as de:
                                _logger.error(f'Error disposing engine: {de}')
                    except Exception:
                        pass

                    if attempt == 0:
                        _logger.warning(f"Transient DB error during update of tx batch {i+1}/{num_batches}: {e}. Rolling back and retrying once.")
                        await asyncio.sleep(0.5)
                        continue
                    else:
                        _logger.error(f'Error updating transactions in batch {i+1}/{num_batches} after retry: {e}')
                        break

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
            _logger.info(f'Batch committing transactions {i+1}-{min(i+BATCH_SIZE, len(all_new_txs))} of {len(all_new_txs)}')
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
                    # Flush pending INSERTs so they are visible to the DB
                    session.flush()

                    # Commit the main TXs/outputs/inputs first. Mapping inserts are best-effort
                    # and must not be allowed to abort this primary transaction. Run mapping
                    # inserts in a separate session after commit so failures do not affect
                    # the committed data.
                    session.commit()
                    _logger.info(f'Committed {len(batch_tx_ids)} new transactions in batch {i+1}-{min(i+BATCH_SIZE, len(all_new_txs))}')

                    # Attempt mapping inserts in a separate session (non-fatal)
                    if batch_tx_ids:
                        try:
                            with session_maker() as map_sess:

                                def _execute_mapping_with_retries_map(sess, stmt, params):
                                    txs_param = params.get('tx_ids') if isinstance(params, dict) else []
                                    _logger.debug(f'Attempting mapping insert (separate session) for {len(txs_param)} txs')
                                    try:
                                        with sess.begin():
                                            try:
                                                sess.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
                                                sess.execute(text(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_MS}ms'"))
                                            except Exception:
                                                _logger.debug('Could not set local DB timeouts for mapping insert (batch)')
                                            sess.execute(stmt, params)
                                        _logger.debug('Mapping insert succeeded (separate session)')
                                        return True
                                    except OperationalError as e:
                                        _logger.warning(f"Mapping insert skipped due to DB lock/timeout: {e}")
                                        # Retry once with an extended timeout
                                        extended = max(STATEMENT_TIMEOUT_MS * 6, 30000)
                                        try:
                                            with sess.begin():
                                                try:
                                                    sess.execute(text(f"SET LOCAL statement_timeout = '{extended}ms'"))
                                                except Exception:
                                                    pass
                                                sess.execute(stmt, params)
                                            _logger.debug('Mapping insert retry succeeded (separate session)')
                                            return True
                                        except OperationalError as e2:
                                            _logger.warning(f"Mapping insert retry failed (separate session): {e2}")
                                            # Fallback: try inserting per-tx id to reduce locking/scan cost
                                            for tx in txs_param:
                                                try:
                                                    with sess.begin():
                                                        try:
                                                            sess.execute(text(f"SET LOCAL statement_timeout = '{extended}ms'"))
                                                        except Exception:
                                                            pass
                                                        sess.execute(stmt, {"tx_ids": [tx]})
                                                    _logger.debug(f'Per-tx mapping insert succeeded for {tx} (separate session)')
                                                except Exception as e3:
                                                    _logger.debug(f"Per-tx mapping insert failed for {tx}: {e3}")
                                            return False
                                    except Exception as e:
                                        _logger.warning(f"Mapping insert failed (non-fatal, separate session): {e}")
                                        return False

                                stmt_out = text("""
                                    INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)
                                    SELECT transactions_outputs.transaction_id, transactions_outputs.script_public_key_address, transactions.block_time
                                    FROM transactions_outputs
                                    JOIN transactions ON transactions.transaction_id = transactions_outputs.transaction_id
                                    WHERE transactions_outputs.transaction_id IN :tx_ids
                                      AND transactions_outputs.script_public_key_address IS NOT NULL
                                    ON CONFLICT DO NOTHING
                                """
                                ).bindparams(bindparam("tx_ids", expanding=True))

                                success_out = _execute_mapping_with_retries_map(map_sess, stmt_out, {"tx_ids": batch_tx_ids})
                                _logger.info(f'Outputs mapping insert success={success_out} for {len(batch_tx_ids)} txs')

                                stmt_in = text("""
                                    INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)
                                    SELECT transactions_inputs.transaction_id, transactions_outputs.script_public_key_address, transactions.block_time
                                    FROM transactions_inputs
                                    LEFT JOIN transactions_outputs ON transactions_outputs.transaction_id = transactions_inputs.previous_outpoint_hash
                                        AND transactions_outputs.index = transactions_inputs.previous_outpoint_index
                                    LEFT JOIN transactions ON transactions.transaction_id = transactions_inputs.transaction_id
                                    WHERE transactions_inputs.transaction_id IN :tx_ids
                                      AND transactions_outputs.script_public_key_address IS NOT NULL
                                    ON CONFLICT DO NOTHING
                                """
                                ).bindparams(bindparam("tx_ids", expanding=True))

                                success_in = _execute_mapping_with_retries_map(map_sess, stmt_in, {"tx_ids": batch_tx_ids})
                                _logger.info(f'Inputs mapping insert success={success_in} for {len(batch_tx_ids)} txs')
                        except OperationalError as e:
                            _logger.warning(f"Batch mapping insert skipped due to DB lock/timeout (separate session): {e}")
                        except Exception as e:
                            _logger.warning(f"Batch mapping insert failed (non-fatal, separate session): {e}")

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
        _logger.info(f'Committing {len(tx_ids_to_add)} TXs: outputs queued={len(self.txs_output)}, inputs queued={len(self.txs_input)}')

        # Use a single session and avoid duplicate transactions
        with session_maker() as session:
            # Check if any transactions already exist in the database
            tx_items = []
            # Protect against very large IN-lists by chunking
            CHUNK = 500
            if tx_ids_to_add:
                for i in range(0, len(tx_ids_to_add), CHUNK):
                    chunk = tx_ids_to_add[i:i + CHUNK]
                    # Run the chunked SELECT with a single retry to avoid failing on a connection
                    # that was returned to the pool in an aborted state.
                    for attempt in range(2):
                        try:
                            rows = session.query(Transaction).filter(Transaction.transaction_id.in_(chunk)).all()
                            tx_items.extend(rows)
                            break
                        except Exception as e:
                            try:
                                session.rollback()
                            except Exception:
                                pass
                            # If this looks like an aborted transaction state, dispose the engine
                            try:
                                msg = str(e) or repr(e)
                                if 'InFailedSqlTransaction' in msg or 'current transaction is aborted' in msg:
                                    _logger.warning('Detected aborted DB transaction state during select; disposing engine pool to reset connections')
                                    try:
                                        engine.dispose()
                                    except Exception as de:
                                        _logger.error(f'Error disposing engine: {de}')
                            except Exception:
                                pass

                            _logger.warning(f"Transient DB error selecting transactions chunk {i}/{len(tx_ids_to_add)}: {e}. Retrying once.")
                            if attempt == 0:
                                await asyncio.sleep(0.25)
                                continue
                            else:
                                _logger.error(f"Failed selecting transactions chunk after retry: {e}")
                                break

            # Update existing transactions (if any) and remove them from the queue
            for tx_item in tx_items:
                tx_item.block_hash = list(set(tx_item.block_hash) | set(self.txs[tx_item.transaction_id].block_hash))
                self.txs.pop(tx_item.transaction_id)

            # Now add new transactions and their inputs/outputs
            for txv in self.txs.values():
                session.add(txv)  # This will insert new or update transaction

            # Add related outputs and inputs
            for tx_output in self.txs_output:
                if tx_output.transaction_id in self.txs:
                    session.add(tx_output)

            for tx_input in self.txs_input:
                if tx_input.transaction_id in self.txs:
                    session.add(tx_input)

            try:
                # Flush pending INSERTs so they are visible to the DB
                session.flush()

                # Commit the main TXs/outputs/inputs first. Mapping inserts are best-effort
                # and must not be allowed to abort this primary transaction. Run mapping
                # inserts in a separate session after commit so failures do not affect
                # the committed data.
                session.commit()

                # Attempt mapping inserts in a separate session (non-fatal)
                if tx_ids_to_add:
                    try:
                        with session_maker() as map_sess:

                            def _execute_mapping_with_retries_map(sess, stmt, params):
                                txs_param = params.get('tx_ids') if isinstance(params, dict) else []
                                _logger.debug(f'Attempting mapping insert (separate session) for {len(txs_param)} txs')
                                try:
                                    with sess.begin():
                                        try:
                                            sess.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
                                            sess.execute(text(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_MS}ms'"))
                                        except Exception:
                                            _logger.debug('Could not set local DB timeouts for mapping insert')
                                        sess.execute(stmt, params)
                                    _logger.debug('Mapping insert succeeded (separate session)')
                                    return True
                                except OperationalError as e:
                                    _logger.warning(f"Mapping insert skipped due to DB lock/timeout: {e}")
                                    # Retry once with an extended timeout
                                    extended = max(STATEMENT_TIMEOUT_MS * 6, 30000)
                                    try:
                                        with sess.begin():
                                            try:
                                                sess.execute(text(f"SET LOCAL statement_timeout = '{extended}ms'"))
                                            except Exception:
                                                pass
                                            sess.execute(stmt, params)
                                        _logger.debug('Mapping insert retry succeeded (separate session)')
                                        return True
                                    except OperationalError as e2:
                                        _logger.warning(f"Mapping insert retry failed (separate session): {e2}")
                                        # Fallback: try inserting per-tx id to reduce locking/scan cost
                                        for tx in txs_param:
                                            try:
                                                with sess.begin():
                                                    try:
                                                        sess.execute(text(f"SET LOCAL statement_timeout = '{extended}ms'"))
                                                    except Exception:
                                                        pass
                                                    sess.execute(stmt, {"tx_ids": [tx]})
                                                _logger.debug(f'Per-tx mapping insert succeeded for {tx} (separate session)')
                                            except Exception as e3:
                                                _logger.debug(f"Per-tx mapping insert failed for {tx}: {e3}")
                                        return False
                                except Exception as e:
                                    _logger.warning(f"Mapping insert failed (non-fatal, separate session): {e}")
                                    return False

                            # outputs -> mapping
                            stmt_out = text("""
                                INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)
                                SELECT transactions_outputs.transaction_id, transactions_outputs.script_public_key_address, transactions.block_time
                                FROM transactions_outputs
                                JOIN transactions ON transactions.transaction_id = transactions_outputs.transaction_id
                                WHERE transactions_outputs.transaction_id IN :tx_ids
                                  AND transactions_outputs.script_public_key_address IS NOT NULL
                                ON CONFLICT DO NOTHING
                            """
                            ).bindparams(bindparam("tx_ids", expanding=True))

                            success_out = _execute_mapping_with_retries_map(map_sess, stmt_out, {"tx_ids": tx_ids_to_add})
                            _logger.info(f'Outputs mapping insert success={success_out} for {len(tx_ids_to_add)} txs')

                            # inputs -> mapping (map spending tx to address of previous outpoint)
                            stmt_in = text("""
                                INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)
                                SELECT transactions_inputs.transaction_id, transactions_outputs.script_public_key_address, transactions.block_time
                                FROM transactions_inputs
                                LEFT JOIN transactions_outputs ON transactions_outputs.transaction_id = transactions_inputs.previous_outpoint_hash
                                    AND transactions_outputs.index = transactions_inputs.previous_outpoint_index
                                LEFT JOIN transactions ON transactions.transaction_id = transactions_inputs.transaction_id
                                WHERE transactions_inputs.transaction_id IN :tx_ids
                                    AND transactions_outputs.script_public_key_address IS NOT NULL
                                ON CONFLICT DO NOTHING
                            """
                            ).bindparams(bindparam("tx_ids", expanding=True))

                            success_in = _execute_mapping_with_retries_map(map_sess, stmt_in, {"tx_ids": tx_ids_to_add})
                            _logger.info(f'Inputs mapping insert success={success_in} for {len(tx_ids_to_add)} txs')
                    except OperationalError as e:
                        _logger.warning(f"Mapping insert skipped due to DB lock/timeout (separate session): {e}")
                    except Exception as e:
                        _logger.warning(f"Mapping insert failed (non-fatal, separate session): {e}")

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
        Insert queued blocks to database only if they don't already exist.
        To avoid large multi-row insert failures (which can abort the DB
        transaction), insert in configurable chunks and fall back to
        per-block inserts when necessary. Failed inserts are kept queued
        for retry.
        """
        try:
            blocks_to_insert = []
            block_hashes = [b.hash for b in self.blocks_to_add]
            _logger.debug(f'Checking blocks with hashes: {block_hashes}')

            # Check which blocks already exist. Chunk large IN-lists to avoid DB parameter limits.
            existing_hashes = set()
            CHUNK = 500
            if block_hashes:
                for i in range(0, len(block_hashes), CHUNK):
                    chunk = block_hashes[i:i + CHUNK]
                    # Retry once on transient DB errors
                    for attempt in range(2):
                        try:
                            with session_maker() as s:
                                rows = s.query(Block.hash).filter(Block.hash.in_(chunk)).all()
                                existing_hashes.update({h[0] for h in rows})
                            break
                        except Exception as e:
                            # If this looks like an aborted transaction state, dispose the engine
                            try:
                                msg = str(e) or repr(e)
                                if 'InFailedSqlTransaction' in msg or 'current transaction is aborted' in msg:
                                    _logger.warning('Detected aborted DB transaction state during block-hash select; disposing engine pool to reset connections')
                                    try:
                                        engine.dispose()
                                    except Exception as de:
                                        _logger.error(f'Error disposing engine: {de}')
                            except Exception:
                                pass

                            _logger.warning(f"Transient DB error selecting block hashes chunk {i}/{len(block_hashes)}: {e}. Retrying once.")
                            if attempt == 0:
                                await asyncio.sleep(0.25)
                                continue
                            else:
                                _logger.error(f"Failed selecting block hashes after retry: {e}")
                                break

            # Only add blocks that don't exist
            for block in self.blocks_to_add:
                if block.hash not in existing_hashes:
                    blocks_to_insert.append(block)

            if not blocks_to_insert:
                _logger.debug('No new blocks to add to database.')
                # nothing to do; keep queue clear
                self.blocks_to_add = []
                return

            failed_blocks = []
            last_inserted_ts = None
            inserted_count = 0

            # Insert in chunks to avoid giant executemany statements
            for i in range(0, len(blocks_to_insert), BLOCK_COMMIT_BATCH):
                chunk = blocks_to_insert[i:i + BLOCK_COMMIT_BATCH]
                chunk_range = f"{i+1}-{i+len(chunk)}"
                success = False

                # Try chunk commit with one retry
                for attempt in range(2):
                    try:
                        with session_maker() as s:
                            s.add_all(chunk)
                            s.flush()
                            s.commit()
                        success = True
                        inserted_count += len(chunk)
                        last_inserted_ts = chunk[-1].timestamp
                        _logger.debug(f'Committed block chunk {chunk_range} ({len(chunk)} blocks)')
                        break
                    except IntegrityError as e:
                        # Possible race/duplicate insert; fallback to per-block
                        _logger.warning(f'IntegrityError committing block chunk {chunk_range}: {e}. Falling back to per-block insert.')
                        break
                    except Exception as e:
                        _logger.warning(f'Transient DB error committing block chunk {chunk_range}: {e}. Retrying once.')
                        if attempt == 0:
                            await asyncio.sleep(0.5)
                            continue
                        else:
                            _logger.error(f'Failed committing chunk {chunk_range} after retry: {e}')
                            break

                # If chunk-level commit failed, try per-block inserts to isolate bad rows
                if not success:
                    for block in chunk:
                        try:
                            with session_maker() as s2:
                                s2.add(block)
                                s2.commit()
                            inserted_count += 1
                            last_inserted_ts = block.timestamp
                        except IntegrityError:
                            # Likely inserted concurrently by another worker; ignore
                            _logger.debug(f'Block {block.hash} already exists (IntegrityError); skipping.')
                        except Exception as e2:
                            _logger.error(f'Failed inserting block {block.hash}: {e2}')
                            failed_blocks.append(block)

            if inserted_count > 0:
                _logger.info(f'Added {inserted_count} new blocks to database. Timestamp: {last_inserted_ts}')
            if failed_blocks:
                _logger.error(f'Failed to insert {len(failed_blocks)} blocks; keeping them queued for retry')

            # Keep failed blocks in the queue for next attempt
            self.blocks_to_add = failed_blocks

        except Exception as e:
            _logger.error(f'Unexpected error committing blocks: {e}')
            raise
        

    def is_tx_id_in_queue(self, tx_id):
        """
        Checks if given TX ID is already in the queue
        """
        return tx_id in self.txs