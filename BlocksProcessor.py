
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

# For 1 BPS - increased for better throughput
CLUSTER_SIZE = 5
CATCH_UP_CLUSTER_SIZE = 500
CLUSTER_WAIT_SECONDS = 1
# Wait this long before asking for the same window again when nothing to resume from
# could be found or fetched (see blockiter / _resume_from): longer than the node's 1 s
# getBlocks response cache, so the retry gets a fresh window.
NO_CHAIN_BLOCK_WAIT_SECONDS = int(os.getenv("NO_CHAIN_BLOCK_WAIT_SECONDS", "10"))
# After this many consecutive such rounds the warning becomes an error and the wait grows
# to RESUME_SLOW_WAIT_SECONDS. The iterator keeps asking and never raises: main.py would
# restart the whole walk from the start hash and drop the uncommitted queue.
RESUME_FAST_ATTEMPTS = int(os.getenv("RESUME_FAST_ATTEMPTS", "6"))
RESUME_SLOW_WAIT_SECONDS = int(os.getenv("RESUME_SLOW_WAIT_SECONDS", "60"))
# Timeout of one getBlockRequest used to fetch a block the window did not serve.
GET_BLOCK_TIMEOUT_SECONDS = int(os.getenv("GET_BLOCK_TIMEOUT_SECONDS", "30"))
# How many selected parents a non-chain low hash is followed down to its chain ancestor.
CHAIN_ANCESTOR_MAX_STEPS = 1000

B_TREE_SIZE = 2500

# Number of blocks to insert per DB batch. Use smaller batches to avoid large executemany failures.
BLOCK_COMMIT_BATCH = int(os.getenv("BLOCK_COMMIT_BATCH", "100"))

# DB timeouts for best-effort mapping inserts (milliseconds)
LOCK_TIMEOUT_MS = int(os.getenv("TX_ADDR_MAPPING_LOCK_TIMEOUT_MS", "100"))
STATEMENT_TIMEOUT_MS = int(os.getenv("TX_ADDR_MAPPING_STATEMENT_TIMEOUT_MS", "60000"))

BATCH_SIZE = int(os.getenv("TX_BATCH_SIZE", "100"))

task_runner = None


class _ResumeFailed(Exception):
    """Neither route of BlocksProcessor._resume_from produced a resume point; the message says why."""


def _is_chain_block(block):
    # MessageToDict leaves false booleans out, so a missing key means False.
    return bool(block.get("verboseData", {}).get("isChainBlock", False))


def _selected_parent(block):
    return block.get("verboseData", {}).get("selectedParentHash")


def _blue_score(block):
    # uint64 fields arrive as strings from MessageToDict.
    return int(block["header"].get("blueScore", 0))


def _blue_work_key(item):
    # The node's own window order (syncmanager/antipast.go sortInTopologicalOrder): blue
    # work ascending, ties by hash. Blue work grows from every block to its children, so
    # no block sorts before one of its ancestors.
    block_hash, block = item
    return int(block["header"].get("blueWork") or "0", 16), block_hash


def _merge_set(block):
    # The selected parent is the first blue (ghostdagmanager/ghostdag.go), so this is the
    # whole merge set and past(block) = past(selected parent) + merge set.
    verbose = block.get("verboseData", {})
    return verbose.get("mergeSetBluesHashes", []) + verbose.get("mergeSetRedsHashes", [])


def _missing_past(block_hash, anchor_hash, by_hash, held):
    """
    The merge-set members the window lacks on the selected parent chain from the chain
    block block_hash down to the anchor, as a list, or None when that chain itself is not
    in the window (a block on it is missing from the window or is not a chain block).
    Members in `held` (blocks the database holds already) are never missing, and the walk
    may stop at a held chain block, whose whole past is held too.

    HTND adds a chain block's merge set to the window only when its walk dequeues that
    block (syncmanager/antipast.go); a chain block can also appear in the window earlier,
    as a member of a non-chain block's merge set, while the merge sets of the chain blocks
    below it have not been served yet. Resuming from such a block would skip those merge
    sets for good, because the next window filters out its whole past. The node's own IBD
    only ever resumes from a dequeued chain block (actualHighHash); this walk recovers
    that distinction from the data the response carries and names the blocks that have to
    be fetched by hash before the candidate is a safe resume point.
    """
    missing = []
    seen = set()
    current = block_hash
    steps = 0
    while current != anchor_hash and current not in held:
        block = by_hash.get(current)
        if block is None or not _is_chain_block(block):
            return None
        for member in _merge_set(block):
            if member != anchor_hash and member not in by_hash and member not in held \
                    and member not in seen:
                seen.add(member)
                missing.append(member)
        current = _selected_parent(block)
        steps += 1
        if not current or steps > len(by_hash):
            return None
    return missing


def _resume_candidate(anchor_hash, by_hash, held):
    """
    (hash, missing merge-set members) of the block to resume getBlocks from, or
    (None, None) when the window holds no chain block above the anchor whose selected
    parent chain down to the anchor is in the window.

    HTND answers getBlocks(lowHash) with the merge sets of the blocks above the low
    hash's nearest chain ancestor (the anchor), stops once more than MergeSetSizeLimit + 1
    hashes are collected and sorts the list by blue work (rpchandlers/get_blocks.go,
    syncmanager/antipast.go). Its last element is therefore the heaviest block of the
    window, which need not be on the selected parent chain, and a non-chain low hash is
    slid back to its chain ancestor on the next call, so resuming from it serves the
    same window again. Two sibling blocks that each end the other's window make the
    iterator alternate between them forever (explorer-eu, 2026-10-14 02:22, blocks
    a30316af and 5cfd8d2b). A chain block is never slid back, so only a chain block is a
    safe resume point, and only one whose past is held: by earlier windows, by this
    window, or by the blocks fetched for it. The node's own IBD keeps exactly that
    checkpoint (actualHighHash); the RPC response just does not carry it.

    Candidates are the chain blocks of the window other than the anchor, highest blue
    score first. The first one whose chain down to the anchor is in the window and whose
    merge sets on that chain the window holds entirely wins, with nothing to fetch;
    failing that, the one with the fewest missing members, which are then fetched.
    """
    candidates = []
    for block_hash, block in by_hash.items():
        if block_hash != anchor_hash and block_hash not in held and _is_chain_block(block):
            candidates.append((_blue_score(block), block_hash))
    candidates.sort(reverse=True)

    best = None
    for _, block_hash in candidates:
        missing = _missing_past(block_hash, anchor_hash, by_hash, held)
        if missing is None:
            continue
        if not missing:
            return block_hash, missing
        if best is None or len(missing) < len(best[1]):
            best = (block_hash, missing)
    if best is not None:
        return best
    return None, None


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
                await self.vcp.set_start_block(block_hash)
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
                    self.addresses_to_update = []  # Clear addresses to prevent memory buildup
                    

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
        global task_runner
        if task_runner and not task_runner.done():
            return
        task_runner = asyncio.create_task(self.vcp.yield_to_database())

        def on_task_done(task):
            if task.cancelled():
                _logger.debug("VCP task was cancelled")
                return

            try:
                exc = task.exception()
            except asyncio.CancelledError:
                _logger.debug("VCP task was cancelled while collecting its result")
                return

            if exc is not None:
                _logger.error(f"VCP task failed: {exc}")

        task_runner.add_done_callback(on_task_done)

    async def _fetch_block(self, block_hash, include_transactions):
        """
        One block by getBlockRequest, in the same dict shape as a getBlocks block (header,
        transactions, verboseData: both handlers build it with DomainBlockToRPCBlock and
        PopulateRPCBlockWithVerboseData). Raises _ResumeFailed on a timeout, an RPC error,
        or the empty answer a node gives while it is not nearly synced
        (rpchandlers/get_block.go).
        """
        resp = await self.client.request("getBlockRequest", {
            "hash": block_hash,
            "includeTransactions": include_transactions
        }, timeout=GET_BLOCK_TIMEOUT_SECONDS)
        if resp is None:
            raise _ResumeFailed(f'getBlock {block_hash} got no response')
        block_response = resp.get("getBlockResponse", {})
        error = block_response.get("error")
        if error:
            message = error.get("message", error) if isinstance(error, dict) else error
            raise _ResumeFailed(f'getBlock {block_hash} failed: {message}')
        block = block_response.get("block")
        if not block or "header" not in block or "verboseData" not in block:
            raise _ResumeFailed(f'getBlock {block_hash} returned no block (node not nearly synced?)')
        return block

    async def _fetch_blocks(self, block_hashes):
        """Full blocks for the hashes, as (hash, block) pairs in blue work order."""
        fetched = []
        for block_hash in block_hashes:
            fetched.append((block_hash, await self._fetch_block(block_hash, True)))
        fetched.sort(key=_blue_work_key)
        return fetched

    async def _anchor(self, low_hash, by_hash):
        """
        (anchor, held): the low hash's nearest chain ancestor, from which the node built the
        window (syncmanager/antipast.go findLowHashInHighHashSelectedParentChain), and the
        blocks between the two, which the database holds already. A chain low hash is its
        own anchor. A non-chain one (the first round after a restart from a non-chain block,
        or after a reorg) is followed down its selected parents with getBlock: the window
        excludes the low hash's whole past, and that past is the anchor's past plus the
        merge sets of the blocks walked here.
        """
        held = set()
        current_hash = low_hash
        current = by_hash.get(low_hash)
        steps = 0
        while True:
            if current is None:
                current = await self._fetch_block(current_hash, False)
            if _is_chain_block(current):
                return current_hash, held
            held.add(current_hash)
            held.update(_merge_set(current))
            current_hash = _selected_parent(current)
            current = by_hash.get(current_hash)
            steps += 1
            if not current_hash or steps > CHAIN_ANCESTOR_MAX_STEPS:
                raise _ResumeFailed(f'no chain block within {steps} selected parents of {low_hash}')

    async def _chain_child(self, anchor_hash):
        """
        The chain block whose selected parent is the anchor, found through getBlock's
        childrenHashes (every DAG child), isChainBlock and selectedParentHash, all read
        fresh. Raises _ResumeFailed when no child qualifies: the anchor is the virtual
        selected parent, or the chain moved between the calls.
        """
        anchor = await self._fetch_block(anchor_hash, False)
        best = None
        for child_hash in anchor.get("verboseData", {}).get("childrenHashes", []):
            child = await self._fetch_block(child_hash, False)
            if not _is_chain_block(child) or _selected_parent(child) != anchor_hash:
                continue
            key = (_blue_score(child), child_hash)
            if best is None or key > best[0]:
                best = (key, child_hash)
        if best is None:
            raise _ResumeFailed(f'{anchor_hash} has no chain child (it is the virtual selected parent, '
                                f'or the chain moved)')
        return best[1]

    async def _resume_from(self, low_hash, block_hashes, blocks):
        """
        Where to resume getBlocks after a window, as (next_hash, next_block, fetched): the
        chain block to use as the next low hash, its block, and the (hash, block) pairs the
        window did not serve but the next low hash's past needs, fetched by getBlock, in
        blue work order. Raises _ResumeFailed when neither route works.

        Route 1: a chain block of the window whose chain down to the anchor is in the
        window; merge-set members that chain lacks are fetched (_resume_candidate).
        Route 2: the window holds no such block, because the walk's budget ran out on the
        merge sets of non-chain children of the anchor before its chain child was dequeued
        (explorer-eu, 2026-10-14 12:14, low hash 9ffb5fbb: 183 blocks, three non-chain
        children with 179 reds each, the chain child 65a7bef7 absent). Then the chain child
        is found through getBlock and fetched together with the members of its merge set
        that are neither in the window nor held: past(child) is past(anchor) plus the
        child's merge set, and past(anchor) is held, so that is exactly what completeness
        needs.
        """
        by_hash = dict(zip(block_hashes, blocks))
        anchor_hash, held = await self._anchor(low_hash, by_hash)

        next_hash, missing = _resume_candidate(anchor_hash, by_hash, held)
        if next_hash is not None:
            if missing:
                _logger.warning(f'Chain block {next_hash} of the getBlocks window from {low_hash} lacks '
                                f'{len(missing)} merge-set members in the window; fetching them by getBlock.')
            return next_hash, by_hash[next_hash], await self._fetch_blocks(missing)

        child_hash = await self._chain_child(anchor_hash)
        child = await self._fetch_block(child_hash, True)
        members = [member for member in _merge_set(child)
                   if member != anchor_hash and member not in by_hash and member not in held]
        _logger.warning(f'getBlocks window from {low_hash} ({len(block_hashes)} blocks) holds no chain block '
                        f'above it whose past is in the window; resuming from the chain child {child_hash} '
                        f'of {anchor_hash} with {len(members)} merge-set members fetched by getBlock.')
        fetched = await self._fetch_blocks(members)
        fetched.append((child_hash, child))
        return child_hash, child, fetched

    async def blockiter(self, start_point):
        low_hash = start_point
        resume_failures = 0
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
            }, timeout=90)
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
                    # Advance to a chain block whose past has been served, never to the last
                    # block: that is the heaviest block, chain or not, and a non-chain low
                    # hash makes the node serve the same window again (_resume_candidate).
                    # Blocks the window failed to serve are fetched by hash and yielded
                    # here, through the same path as window blocks.
                    try:
                        next_hash, next_block, fetched = await self._resume_from(low_hash, block_hashes, blocks)
                    except _ResumeFailed as failure:
                        resume_failures += 1
                        if resume_failures <= RESUME_FAST_ATTEMPTS:
                            wait, log = NO_CHAIN_BLOCK_WAIT_SECONDS, _logger.warning
                        else:
                            wait, log = RESUME_SLOW_WAIT_SECONDS, _logger.error
                        log(f'No block to resume from after the getBlocks window from {low_hash} '
                            f'({len(block_hashes)} blocks): {failure} (round {resume_failures}). '
                            f'Waiting {wait}s and asking again.')
                        await asyncio.sleep(wait)
                        continue
                    resume_failures = 0
                    for fetched_hash, fetched_block in fetched:
                        yield fetched_hash, fetched_block
                        fetched_time = int(fetched_block["header"]["timestamp"]) // 1000
                        if fetched_time >= self.last_block_time:
                            self.last_block_time = fetched_time
                    if fetched:
                        _logger.info(f'Yielded {len(fetched)} blocks fetched by getBlock after the window '
                                     f'from {low_hash[:8]}...')
                    low_hash = next_hash
                    next_time = int(next_block["header"]["timestamp"]) // 1000
                    self.last_block_time = max(self.last_block_time, next_time)
                else:
                    _logger.info("No progress. Waiting...")
                    await asyncio.sleep(0.5)

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
        """Batch commit with proper handling for transactions that already exist in DB.

        If a tx already exists, we merge the new block_hash(es) into its array
        instead of attempting an INSERT (which would violate the PK).
        """
        if not self.txs:
            self.txs = {}
            self.txs_output = []
            self.txs_input = []
            return

        tx_ids = list(self.txs.keys())
        _logger.info(f'Committing {len(tx_ids)} transactions (outputs={len(self.txs_output)}, inputs={len(self.txs_input)})')

        all_tx_ids_to_map = []

        # ============================================================
        # Pre-check which transactions already exist in the DB
        # and merge block_hashes for them (prevents UniqueViolation).
        # ============================================================
        existing_tx_ids = set()
        CHUNK = 500
        if tx_ids:
            for i in range(0, len(tx_ids), CHUNK):
                chunk = tx_ids[i:i + CHUNK]
                for attempt in range(2):
                    try:
                        with session_maker() as session:
                            rows = session.query(Transaction.transaction_id).filter(
                                Transaction.transaction_id.in_(chunk)
                            ).all()
                            for r in rows:
                                existing_tx_ids.add(r[0])
                        break
                    except Exception as e:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        # Handle aborted transaction state (copied from commit_txs)
                        try:
                            msg = str(e) or repr(e)
                            if 'InFailedSqlTransaction' in msg or 'current transaction is aborted' in msg:
                                _logger.warning('Detected aborted DB transaction state during tx existence check; disposing engine pool')
                                try:
                                    engine.dispose()
                                except Exception as de:
                                    _logger.error(f'Error disposing engine: {de}')
                        except Exception:
                            pass

                        _logger.warning(f"Transient DB error checking existing txs chunk {i}/{len(tx_ids)}: {e}. Retrying once.")
                        if attempt == 0:
                            await asyncio.sleep(0.25)
                            continue
                        else:
                            _logger.error(f"Failed checking existing txs after retry: {e}")
                            break

        # Update existing transactions' block_hash arrays (in small chunks)
        if existing_tx_ids:
            existing_list = list(existing_tx_ids)
            UPDATE_CHUNK = 100
            updated_count = 0
            for i in range(0, len(existing_list), UPDATE_CHUNK):
                chunk = existing_list[i:i + UPDATE_CHUNK]
                for attempt in range(2):
                    try:
                        with session_maker() as session:
                            tx_items = session.query(Transaction).filter(
                                Transaction.transaction_id.in_(chunk)
                            ).all()
                            for tx_item in tx_items:
                                if tx_item.transaction_id in self.txs:
                                    new_hashes = self.txs[tx_item.transaction_id].block_hash or []
                                    current_hashes = tx_item.block_hash or []
                                    tx_item.block_hash = list(set(current_hashes) | set(new_hashes))
                                    self.txs.pop(tx_item.transaction_id, None)
                                    updated_count += 1
                            session.commit()
                        break
                    except Exception as e:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        _logger.warning(f"Error updating existing tx block_hashes chunk: {e}")
                        if attempt == 0:
                            await asyncio.sleep(0.25)
                            continue
                        else:
                            _logger.error(f"Failed updating existing txs after retry: {e}")
                            break

            if updated_count:
                _logger.info(f"Updated block_hash array for {updated_count} existing transactions (no insert needed)")

        # Only NEW txs remain in self.txs now
        remaining_tx_ids = list(self.txs.keys())
        if not remaining_tx_ids:
            _logger.info("No new transactions to insert (all were existing and had their block_hashes merged).")
            self.txs = {}
            self.txs_output = []
            self.txs_input = []
            return

        # ============================================================
        # Original batch insert logic — now only for NEW transactions
        # ============================================================
        for i in range(0, len(remaining_tx_ids), BATCH_SIZE):
            batch_tx_ids = remaining_tx_ids[i:i + BATCH_SIZE]
            batch_txs = [self.txs[tx_id] for tx_id in batch_tx_ids]

            # Only outputs/inputs belonging to txs we are actually inserting
            batch_outputs = [out for out in self.txs_output if out.transaction_id in batch_tx_ids]
            batch_inputs = [inp for inp in self.txs_input if inp.transaction_id in batch_tx_ids]

            with session_maker() as session:
                try:
                    session.add_all(batch_txs)
                    session.add_all(batch_outputs)
                    session.add_all(batch_inputs)

                    session.flush()
                    session.commit()

                    _logger.info(f'Committed batch {i//BATCH_SIZE + 1} ({len(batch_tx_ids)} new txs)')
                    all_tx_ids_to_map.extend(batch_tx_ids)

                except Exception as e:
                    session.rollback()
                    _logger.error(f'Error committing tx batch {i//BATCH_SIZE + 1}: {e}')
                    # Rare race condition between our check and insert is possible.
                    # We log and continue — the tx will be retried in a future cluster.

        # Mapping (only for newly inserted txs)
        if all_tx_ids_to_map:
            await self._process_mapping_inserts_optimized(all_tx_ids_to_map)

        # Clear queues
        self.txs = {}
        self.txs_output = []
        self.txs_input = []

    async def _process_mapping_inserts_optimized(self, all_tx_ids):
        """Split mapping into simpler, faster operations."""
        if not all_tx_ids:
            return

        BATCH_SIZE = 60   # Even smaller for the heavy inputs part

        try:
            for i in range(0, len(all_tx_ids), BATCH_SIZE):
                batch = all_tx_ids[i:i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1

                with session_maker() as sess:
                    sess.execute(text("SET LOCAL statement_timeout = '300s'"))
                    sess.execute(text("SET LOCAL lock_timeout = '20s'"))

                    # 1. Simple outputs mapping (fast)
                    sess.execute(text("""
                        INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)
                        SELECT o.transaction_id, o.script_public_key_address, t.block_time
                        FROM transactions_outputs o
                        JOIN transactions t ON t.transaction_id = o.transaction_id
                        WHERE o.transaction_id = ANY(:tx_ids)
                        AND o.script_public_key_address IS NOT NULL
                        ON CONFLICT DO NOTHING
                    """), {"tx_ids": batch})

                    # 2. Inputs mapping - LATERAL + LIMIT forces a per-input index
                    # lookup on (transaction_id, index) instead of a full scan
                    # of transactions_outputs
                    sess.execute(text("""
                        INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)
                        SELECT i.transaction_id, o.script_public_key_address, t.block_time
                        FROM transactions_inputs i
                        JOIN transactions t ON t.transaction_id = i.transaction_id
                        CROSS JOIN LATERAL (
                            SELECT po.script_public_key_address
                            FROM transactions_outputs po
                            WHERE po.transaction_id = i.previous_outpoint_hash
                            AND po.index = i.previous_outpoint_index
                            LIMIT 1
                        ) o
                        WHERE i.transaction_id = ANY(:tx_ids)
                        AND o.script_public_key_address IS NOT NULL
                        ON CONFLICT DO NOTHING
                    """), {"tx_ids": batch})

                    sess.commit()
                    _logger.info(f"Mapping batch {batch_num} completed ({len(batch)} txs)")

        except Exception as e:
            _logger.warning(f"Mapping batch failed (non-fatal): {e}")

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

                # Attempt mapping inserts using the optimized method
                if tx_ids_to_add:
                    # Get the remaining transaction IDs that need mapping
                    remaining_tx_ids = [tx_id for tx_id in tx_ids_to_add if tx_id in self.txs]
                    if remaining_tx_ids:
                        await self._process_mapping_inserts_optimized(remaining_tx_ids)

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