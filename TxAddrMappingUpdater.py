# encoding: utf-8

import logging
import os
import time
from datetime import datetime, timedelta

from dbsession import session_maker
from helper import KeyValueStore
from models.TxAddrMapping import TxAddrMapping
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

LIMIT = int(os.getenv("TX_ADDR_MAPPING_LIMIT", "500"))
PRECONDITION_RETRIES = int(os.getenv("TX_ADDR_MAPPING_RETRIES", "3"))
# Timeouts to avoid waiting long on DB locks (milliseconds)
LOCK_TIMEOUT_MS = int(os.getenv("TX_ADDR_MAPPING_LOCK_TIMEOUT_MS", "200"))
STATEMENT_TIMEOUT_MS = int(os.getenv("TX_ADDR_MAPPING_STATEMENT_TIMEOUT_MS", "5000"))

_logger = logging.getLogger(__name__)


class TxAddrMappingUpdater(object):
    def __init__(self):
        self.last_block_time = None
        self.id_counter_inputs = None
        self.id_counter_outputs = None

    def precondition(self):
        with session_maker() as s:
            self.id_counter_inputs = int(KeyValueStore.get("last_id_counter_inputs") or 0)
            self.id_counter_outputs = int(KeyValueStore.get("last_id_counter_outputs") or 0)

    @staticmethod
    def minimum_timestamp():
        return round((datetime.now() - timedelta(minutes=1)).timestamp() * 1000)

    def loop(self):
        self.precondition()

        error_cnt = 0

        _logger.debug('Start TxAddrMappingUpdater')  # type: TxAddrMapping

        while True:
            # get max id ( either LIMIT or maximum in DB )
            if self.id_counter_inputs is not None and self.id_counter_outputs is not None:
                with session_maker() as s:
                    max_in = min(self.id_counter_inputs + LIMIT,
                                s.execute(
                                    text(f"""SELECT id FROM transactions_inputs ORDER by id DESC LIMIT 1"""))
                                .scalar() or 0)

                    max_out = min(self.id_counter_outputs + LIMIT,
                                s.execute(
                                    text(f"""SELECT id FROM transactions_outputs ORDER by id DESC LIMIT 1"""))
                                .scalar() or 0)

                try:
                    count_outputs, new_last_block_time_outputs = self.update_outputs(self.id_counter_outputs,
                                                                                    max_out)
                    count_inputs, new_last_block_time_inputs = self.update_inputs(self.id_counter_inputs,
                                                                                max_in)
                    # save last runs ids in case of restart
                    KeyValueStore.set("last_id_counter_inputs", max_in)
                    KeyValueStore.set("last_id_counter_outputs", max_out)

                except Exception:
                    error_cnt += 1
                    if error_cnt <= 3:
                        time.sleep(10)
                        continue
                    raise
                if count_inputs > 0:
                    _logger.info(f"Updated {count_inputs} input mappings.")
                if count_outputs > 0:
                    _logger.info(f"Updated {count_outputs} outputs mappings.")

                last_id_counter_inputs = self.id_counter_inputs
                last_id_counter_outputs = self.id_counter_outputs

                # next start id is the maximum of last request
                self.id_counter_inputs = max_in
                self.id_counter_outputs = max_out

                # _logger.debug(f"Next TX-Input ID: {self.id_counter_inputs}.")
                # _logger.debug(f"Next TX-Output ID: {self.id_counter_outputs}.")

                if last_id_counter_inputs + LIMIT > self.id_counter_inputs and \
                        last_id_counter_outputs + LIMIT > self.id_counter_outputs:
                    time.sleep(10)

    def get_last_block_time(self, start_block_time):
        with session_maker() as s:
            result = s.execute(text(f"""SELECT
                transactions.block_time
                
                FROM transactions
                WHERE transactions.block_time >= :blocktime
                 ORDER by transactions.block_time ASC
                 LIMIT {LIMIT}"""), {"blocktime": start_block_time}).all()

        try:
            return result[-1][0]
        except TypeError:
            return start_block_time

    def update_inputs(self, min_id: int, max_id: int):
        attempts = 0
        result = None
        insert_sql = text(f"""
            INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)

            SELECT DISTINCT * FROM (
                SELECT transactions_inputs.transaction_id,
                       transactions_outputs.script_public_key_address,
                       transactions.block_time FROM transactions_inputs 
                LEFT JOIN transactions_outputs ON 

                    transactions_outputs.transaction_id = transactions_inputs.previous_outpoint_hash AND
                    transactions_outputs.index = transactions_inputs.previous_outpoint_index

                LEFT JOIN transactions ON transactions.transaction_id = transactions_inputs.transaction_id

                WHERE transactions_inputs.id > :minId AND transactions_inputs.id <= :maxId
                   AND transactions_outputs.script_public_key_address IS NOT NULL
                ORDER by transactions_inputs.id
                ) as distinct_query

             ON CONFLICT DO NOTHING
                     RETURNING block_time;
        """)

        while True:
            attempts += 1
            try:
                with session_maker() as s:
                    # avoid blocking on locks for long periods
                    try:
                        s.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
                        s.execute(text(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_MS}ms'"))
                    except Exception:
                        # not fatal; continue without local timeouts
                        _logger.debug('Could not set local DB timeouts for update_inputs')

                    result = s.execute(insert_sql, {"minId": min_id, "maxId": max_id})
                    s.commit()
                break
            except OperationalError as e:
                _logger.warning(f"update_inputs: DB operation timed out/locked (attempt %s): %s", attempts, e)
                if attempts >= PRECONDITION_RETRIES:
                    raise
                time.sleep(0.5 * attempts)
                continue

        try:
            rows = result.all()
            return len(rows), rows[-1][0]
        except (IndexError, TypeError):
            return 0, None

    def update_outputs(self, min_id: int, max_id: int):
        attempts = 0
        result = None
        insert_sql = text(f"""

            INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)

            (SELECT sq.*, transactions.block_time FROM (SELECT transaction_id, script_public_key_address                 
            FROM transactions_outputs
            WHERE transactions_outputs.id > :minId and transactions_outputs.id <= :maxId
            ORDER by transactions_outputs.id DESC) as sq
            JOIN transactions ON transactions.transaction_id = sq.transaction_id)

             ON CONFLICT DO NOTHING
             RETURNING block_time;
        """)

        while True:
            attempts += 1
            try:
                with session_maker() as s:
                    try:
                        s.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
                        s.execute(text(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_MS}ms'"))
                    except Exception:
                        _logger.debug('Could not set local DB timeouts for update_outputs')

                    result = s.execute(insert_sql, {"minId": min_id, "maxId": max_id})
                    s.commit()
                break
            except OperationalError as e:
                _logger.warning(f"update_outputs: DB operation timed out/locked (attempt %s): %s", attempts, e)
                if attempts >= PRECONDITION_RETRIES:
                    raise
                time.sleep(0.5 * attempts)
                continue

        try:
            rows = result.all()
            return len(rows), rows[-1][0]
        except (IndexError, TypeError):
            return 0, None
