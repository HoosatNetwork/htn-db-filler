
import asyncio
import concurrent.futures
import logging
import os
import threading
import time
from typing import List
from dbsession import session_maker
from models.Balance import Balance
from sqlalchemy.exc import SQLAlchemyError

_logger = logging.getLogger(__name__)

class BalanceProcessor(object):

    def __init__(self, client):
        self.client = client

        # All grpc.aio operations must run on a single running asyncio loop.
        # We capture the loop (when constructed from within async code) and schedule
        # balance RPC coroutines onto it from the background thread.
        self._loop: asyncio.AbstractEventLoop | None = None
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None

        self._pause_seconds = float(os.getenv("BALANCE_PAUSE_SECONDS", "0.05"))
        self._batch_size = int(os.getenv("BALANCE_BATCH_SIZE", "25"))
        self._commit_every = int(os.getenv("BALANCE_COMMIT_EVERY", "1"))
        self._deadlock_retries = int(os.getenv("BALANCE_DEADLOCK_RETRIES", "5"))
        self._deadlock_backoff_seconds = float(os.getenv("BALANCE_DEADLOCK_BACKOFF_SECONDS", "0.2"))
        self._threaded = os.getenv("BALANCE_THREADED", "True").lower() in ["true", "1", "t", "y", "yes"]

        self._pending_lock = threading.Lock()
        self._pending_addrs: set[str] = set()
        self._has_work = threading.Event()
        self._stop = threading.Event()
        self._worker_thread: threading.Thread | None = None
        self._last_enqueue_log_ts = 0.0

        if self._threaded:
            if self._loop is None:
                _logger.warning(
                    "BALANCE_THREADED is enabled but no running asyncio loop was found; "
                    "balance worker will not start (falling back to async updates)."
                )
                self._threaded = False
            else:
                self.start_worker()

        _logger.info(
            f"BalanceProcessor: threaded={self._threaded}, pause={self._pause_seconds}s, "
            f"batch_size={self._batch_size}, commit_every={self._commit_every}"
        )

    @staticmethod
    def _is_deadlock_error(exc: Exception) -> bool:
        # psycopg2 sets pgcode on the underlying exception.
        pgcode = getattr(getattr(exc, "orig", None), "pgcode", None)
        return pgcode in {"40P01", "40001"}  # deadlock_detected, serialization_failure

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Attach an asyncio loop after construction and start worker if enabled."""
        self._loop = loop
        if self._threaded and (not self._worker_thread or not self._worker_thread.is_alive()):
            self.start_worker()

    def start_worker(self) -> None:
        if self._worker_thread and self._worker_thread.is_alive():
            return
        if self._loop is None:
            _logger.warning("BalanceProcessor worker not started: no asyncio loop attached")
            return
        self._stop.clear()
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True, name="BalanceProcessor")
        self._worker_thread.start()
        _logger.info("BalanceProcessor worker thread started")

    def stop_worker(self, join_timeout: float | None = 2.0) -> None:
        self._stop.set()
        self._has_work.set()
        if self._worker_thread and join_timeout is not None:
            self._worker_thread.join(timeout=join_timeout)

    def enqueue_balance_updates(self, addresses: List[str] | None) -> None:
        """Queue addresses for balance refresh without blocking the event loop."""
        if not addresses:
            return
        if not isinstance(addresses, list):
            _logger.error(f"enqueue_balance_updates: expected list, got {type(addresses)}")
            return
        added = 0
        ignored_duplicates = 0
        invalid = 0
        with self._pending_lock:
            before = len(self._pending_addrs)
            for addr in addresses:
                if not isinstance(addr, str) or not addr:
                    invalid += 1
                    continue
                if addr in self._pending_addrs:
                    ignored_duplicates += 1
                    continue
                self._pending_addrs.add(addr)
                added += 1
            after = len(self._pending_addrs)
        self._has_work.set()

        # Ensure worker is running (in case loop was attached after init)
        if self._threaded and (not self._worker_thread or not self._worker_thread.is_alive()):
            self.start_worker()

        # Throttle noisy logs
        now = time.time()
        if now - self._last_enqueue_log_ts >= 5:
            with self._pending_lock:
                pending = len(self._pending_addrs)
            _logger.info(
                f"BalanceProcessor: queued balances (pending={pending}, added={added}, dup={ignored_duplicates}, invalid={invalid})"
            )
            self._last_enqueue_log_ts = now

    def _drain_pending_batch(self, max_items: int) -> list[str]:
        batch: list[str] = []
        with self._pending_lock:
            while self._pending_addrs and len(batch) < max_items:
                batch.append(self._pending_addrs.pop())
            if not self._pending_addrs:
                self._has_work.clear()
        return batch

    def _worker_loop(self) -> None:
        """Background thread loop: fetch balances with pause and write to DB.

        This intentionally runs in a separate OS thread because SQLAlchemy calls are
        synchronous and would otherwise block the asyncio event loop.
        """
        loop = self._loop
        if loop is None:
            _logger.error("Balance worker started without an asyncio loop; exiting.")
            return

        try:
            while not self._stop.is_set():
                self._has_work.wait(timeout=1.0)
                if self._stop.is_set():
                    break

                batch = self._drain_pending_batch(self._batch_size)
                if not batch:
                    continue

                # Deterministic ordering helps avoid deadlocks across concurrent workers/processes.
                batch = sorted(batch)
                _logger.info(f"BalanceProcessor: processing batch size={len(batch)}")

                failed_addresses: list[str] = []
                with session_maker() as session:
                    since_commit = 0
                    for address in batch:
                        if self._stop.is_set():
                            break
                        try:
                            fut = asyncio.run_coroutine_threadsafe(self._get_balance_from_rpc(address), loop)
                            new_balance = fut.result(timeout=75)

                            # Avoid query-triggered autoflush mid-loop.
                            with session.no_autoflush:
                                existing_balance = (
                                    session.query(Balance)
                                    .filter_by(script_public_key_address=address)
                                    .first()
                                )
                                if new_balance is None:
                                    if existing_balance:
                                        session.delete(existing_balance)
                                else:
                                    if existing_balance:
                                        existing_balance.balance = new_balance
                                    else:
                                        session.add(Balance(script_public_key_address=address, balance=new_balance))

                            since_commit += 1
                            if self._commit_every > 0 and since_commit >= self._commit_every:
                                committed = False
                                for attempt in range(1, self._deadlock_retries + 1):
                                    try:
                                        session.commit()
                                        committed = True
                                        since_commit = 0
                                        break
                                    except SQLAlchemyError as e:
                                        session.rollback()
                                        if self._is_deadlock_error(e):
                                            _logger.warning(
                                                f"BalanceProcessor: deadlock/serialization on commit (attempt {attempt}/{self._deadlock_retries})"
                                            )
                                            time.sleep(self._deadlock_backoff_seconds * attempt)
                                            continue
                                        raise
                                if not committed:
                                    failed_addresses.append(address)

                        except concurrent.futures.TimeoutError:
                            _logger.error(f"Balance RPC timed out for address {address}")
                            failed_addresses.append(address)
                        except Exception as e:
                            _logger.error(f"Balance worker error for address {address}: {e}")
                            failed_addresses.append(address)

                        if self._pause_seconds > 0:
                            time.sleep(self._pause_seconds)

                    try:
                        # Final commit for any remaining changes in this session.
                        if self._commit_every <= 0 or since_commit > 0:
                            committed = False
                            for attempt in range(1, self._deadlock_retries + 1):
                                try:
                                    session.commit()
                                    committed = True
                                    break
                                except SQLAlchemyError as e:
                                    session.rollback()
                                    if self._is_deadlock_error(e):
                                        _logger.warning(
                                            f"BalanceProcessor: deadlock/serialization on final commit (attempt {attempt}/{self._deadlock_retries})"
                                        )
                                        time.sleep(self._deadlock_backoff_seconds * attempt)
                                        continue
                                    raise
                            if not committed:
                                failed_addresses = list(set(failed_addresses + batch))
                    except Exception as e:
                        session.rollback()
                        _logger.error(f"Balance worker DB commit error: {e}")
                        failed_addresses = list(set(failed_addresses + batch))

                if failed_addresses:
                    self.enqueue_balance_updates(failed_addresses)

                with self._pending_lock:
                    pending = len(self._pending_addrs)
                _logger.info(f"BalanceProcessor: batch done (pending={pending}, failed={len(failed_addresses)})")
        except Exception:
            _logger.exception("BalanceProcessor worker crashed")


    async def _get_balance_from_rpc(self, address):
        """
        Fetch balance for the given address from the RPC node.
        """
        try:
            response = await self.client.request("getBalanceByAddressRequest", params= {"address": address}, timeout=60)

            get_balance_response = response.get("getBalanceByAddressResponse", {})
            balance = get_balance_response.get("balance", None)
            error = get_balance_response.get("error", None)

            if error:
                _logger.error(f"Error fetching balance for address {address}: {error}")
            
            if balance is not None:
                return int(balance)
            
            return None
        
        except Exception as e:
            _logger.error(f"Error fetching balance for address {address}: {e}")
            return None

    async def update_all_balances(self):
        with session_maker() as session:
            try:
                from sqlalchemy import text
                query = session.execute(
                    text("""
                    SELECT DISTINCT script_public_key_address 
                    FROM transactions_outputs 
                    WHERE script_public_key_address IS NOT NULL 
                    ORDER BY script_public_key_address
                    """)
                )

                addresses = [row[0] for row in query.fetchall()]

                if not addresses:
                    _logger.info("No addresses found to update balances.")
                    return

                _logger.info(f"Found {len(addresses)} addresses to update balances.")

                # For bulk updates (e.g. on boot), allow threading to avoid blocking.
                if self._threaded:
                    self.enqueue_balance_updates(addresses)
                else:
                    await self.update_balance_from_rpc(addresses, 10)
                    await asyncio.sleep(0.1)

            except Exception as e:
                _logger.error(f"Error updating balances: {e}")
                return


    async def update_balance_from_rpc(self, addresses: List[str], batch_size: int = 10) -> None:
        _logger.info(
            f"update_balance_from_rpc: received "
            f"{len(addresses) if isinstance(addresses, list) else 'non-list'} "
            f"addresses, batch_size={batch_size}"
        )

        if not isinstance(addresses, list):
            _logger.error(f"Expected a list of addresses, got {type(addresses)}: {addresses}")
            return
        if len(addresses) == 0:
            _logger.info("update_balance_from_rpc: no addresses to update, skipping.")
            return
        if not all(isinstance(addr, str) for addr in addresses):
            _logger.error(f"Invalid address types in batch: {[type(addr) for addr in addresses]}")
            return

        failed_addresses = []
        created_count = 0
        updated_count = 0
        deleted_count = 0
        processed_count = 0
        skipped_count = 0
        with session_maker() as session:
            for i in range(0, len(addresses), batch_size):
                batch_addresses = addresses[i:i + batch_size]
                _logger.info(f"Processing batch {i // batch_size + 1} with {len(batch_addresses)} addresses")
                _logger.debug(f"Batch addresses: {batch_addresses}")

                try:
                    for address in batch_addresses:
                        try:
                            new_balance = await self._get_balance_from_rpc(address)
                            _logger.debug(f"Fetched balance {new_balance} for address {address}")
                            existing_balance = session.query(Balance).filter_by(script_public_key_address=address).first()

                            if new_balance is None:
                                if existing_balance:
                                    session.delete(existing_balance)
                                    _logger.debug(f"Deleted balance record for address {address} (balance is None)")
                                    deleted_count += 1
                                else:
                                    skipped_count += 1
                            else:
                                if existing_balance:
                                    existing_balance.balance = new_balance
                                    _logger.debug(f"Updated balance for address {address} to {new_balance}")
                                    updated_count += 1
                                else:
                                    new_record = Balance(
                                        script_public_key_address=address,
                                        balance=new_balance
                                    )
                                    session.add(new_record)
                                    _logger.debug(f"Created new balance record for address {address} with balance {new_balance}")
                                    created_count += 1
                            processed_count += 1
                        except Exception as e:
                            _logger.error(f"Error processing address {address} in batch {i // batch_size + 1}: {e}")
                            failed_addresses.append(address)
                            continue

                    session.commit()
                    _logger.info(f"Committed batch {i // batch_size + 1} with {len(batch_addresses)} addresses")
                except SQLAlchemyError as db_err:
                    _logger.error(f"Database error in batch {i // batch_size + 1}: {db_err}")
                    session.rollback()
                    failed_addresses.extend(batch_addresses)
                    continue
                except Exception as e:
                    _logger.error(f"Unexpected error in batch {i // batch_size + 1}: {e}")
                    session.rollback()
                    failed_addresses.extend(batch_addresses)
                    continue

        if failed_addresses:
            _logger.warning(f"Failed to process {len(failed_addresses)} addresses: {failed_addresses[:10]}{'...' if len(failed_addresses) > 10 else ''}")
        else:
            _logger.info(f"Successfully processed all {len(addresses)} addresses")

        _logger.info(
            f"update_balance_from_rpc summary: processed={processed_count}, "
            f"created={created_count}, updated={updated_count}, deleted={deleted_count}, "
            f"skipped={skipped_count}, failed={len(failed_addresses)}"
        )
