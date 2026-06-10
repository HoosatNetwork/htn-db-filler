import asyncio
import logging
import os
import threading
import sys
import time
import cProfile

from BlocksProcessor import BlocksProcessor
from TxAddrMappingUpdater import TxAddrMappingUpdater
from VirtualChainProcessor import VirtualChainProcessor
from BalanceProcessor import BalanceProcessor
from dbsession import create_all
from helper import KeyValueStore
from htnd.HtndMultiClient import HtndMultiClient

logging.basicConfig(format="%(asctime)s::%(levelname)s::%(name)s::%(message)s",
                    level=logging.DEBUG if os.getenv("DEBUG", False) else logging.INFO,
                    handlers=[
                        logging.StreamHandler()
                    ]
                    )

# disable sqlalchemy notifications
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

# get file logger
_logger = logging.getLogger(__name__)

# create tables in database
_logger.info('Creating DBs if not exist.')
create_all(drop=False)

htnd_hosts = []

for i in range(100):
    try:
        htnd_hosts.append(os.environ[f"HTND_HOSTS_{i + 1}"].strip())
    except KeyError:
        break

if not htnd_hosts:
    raise Exception('Please set at least HTND_HOSTS_1 environment variable.')


# create Htnd client
client = HtndMultiClient(htnd_hosts)


async def main():
    # initialize htnds
    await client.initialize_all()

    while client.htnds[0].is_synced == False:
        _logger.debug('Client not synced yet. Waiting...')
        await asyncio.sleep(60)

    # find last acceptedTx's block hash, when restarting this tool
    start_hash = KeyValueStore.get("vspc_last_start_hash")
    start_block = None

    # if there is nothing in the db, just get the first block after genesis.
    daginfo = await client.request("getBlockDagInfoRequest", {})
    if daginfo is None:
        _logger.debug("Failed first BlockDagInfoRequest")
    if not start_hash:
        virtualParentHash = daginfo["getBlockDagInfoResponse"]["virtualParentHashes"][0]
        start_hash = virtualParentHash

    # if there is argument start_hash start with that instead of last acceptedTx or latest block.
    env_start_hash = os.getenv('START_HASH', None) # Default to None if not set
    if env_start_hash != None:
        start_hash = env_start_hash

    find_start_block_str = os.getenv('FIND_START_BLOCK', 'False')  # Default to 'False' if not set
    find_start_block = find_start_block_str.lower() in ['true', '1', 't', 'y', 'yes']

    _logger.info(f"Find start block: {find_start_block}")
    _logger.info(f"Start hash: {start_hash}")
    if start_hash:
        resp = await client.request("getBlockRequest",
                                             params={
                                                 "hash": start_hash,
                                                 "includeTransactions": True,
                                             },
                                             timeout=60)
        if resp is not None and "getBlockResponse" in resp:
            start_block = resp["getBlockResponse"].get("block", [])

    if find_start_block:
        _logger.info("Finding start block from database (highest processed block)...")
        
        from dbsession import session_maker
        from models.Block import Block  

        with session_maker() as session:
            # Get the block with the highest blue score / timestamp / height
            highest_block = session.query(Block).order_by(
                Block.blue_score.desc(),   
                Block.timestamp.desc()     
            ).first()

            if highest_block:
                start_hash = highest_block.hash
                _logger.info(f"✅ Found highest block in DB: {start_hash} (blueScore={highest_block.blue_score})")
            else:
                _logger.warning("No blocks found in database. Falling back to genesis/nearest virtual parent.")
                # fallback to original logic
                daginfo = await client.request("getBlockDagInfoRequest", {})
                if daginfo and "getBlockDagInfoResponse" in daginfo:
                    virtualParentHash = daginfo["getBlockDagInfoResponse"]["virtualParentHashes"][0]
                    start_hash = virtualParentHash

    batch_processing_str = os.getenv('BATCH_PROCESSING', 'False')  # Default to 'False' if not set
    batch_processing = batch_processing_str.lower() in ['true', '1', 't', 'y', 'yes']

    env_enable_balance_str = os.getenv('BALANCE_ENABLED', 'False')
    env_enable_balance = env_enable_balance_str.lower() in ['true', '1', 't', 'y', 'yes']
    env_update_balance_on_boot_str = os.getenv('UPDATE_BALANCE_ON_BOOT', 'False')
    env_update_balance_on_boot = env_update_balance_on_boot_str.lower() in ['true', '1', 't', 'y', 'yes']
    bap = BalanceProcessor(client)
    if env_update_balance_on_boot:
        await bap.update_all_balances()

    env_update_balance_only_str = os.getenv('UPDATE_BALANCE_ONLY', 'False')
    env_update_balance_only = env_update_balance_only_str.lower() in ['true', '1', 't', 'y', 'yes']
    if env_update_balance_only:
        refresh_every_seconds = int(os.getenv('BALANCE_ONLY_REFRESH_SECONDS', '1800'))
        _logger.info(
            'UPDATE_BALANCE_ONLY enabled: running periodic full balance refresh every %ss.',
            refresh_every_seconds,
        )
        while True:
            started = time.time()
            await bap.update_all_balances()
            duration = time.time() - started
            _logger.info(
                'UPDATE_BALANCE_ONLY: refresh cycle queued in %.2fs; sleeping %ss.',
                duration,
                refresh_every_seconds,
            )
            await asyncio.sleep(refresh_every_seconds)

    # create instances of blocksprocessor and virtualchainprocessor
    vcp = VirtualChainProcessor(client, start_block, start_hash)
    bp = BlocksProcessor(client, vcp, bap, batch_processing, env_enable_balance)

    # start blocks processor working concurrent
    restart_attempts = 0
    while True:
        try:
            await bp.loop(start_hash)
            # If the loop returns without exception, restart gracefully
            _logger.error('BlocksProcessor.loop exited unexpectedly without exception. Restarting in 5s.')
            await asyncio.sleep(5)
            bp = BlocksProcessor(client, vcp, bap, batch_processing, env_enable_balance)
            restart_attempts = 0
            continue
        except Exception:
            restart_attempts += 1
            backoff = min(60, 2 ** min(restart_attempts, 6))
            _logger.exception('BlocksProcessor.loop crashed; restarting after %ss (attempt %s).', backoff, restart_attempts)
            await asyncio.sleep(backoff)
            # recreate the processor to ensure a clean state
            bp = BlocksProcessor(client, vcp, bap, batch_processing, env_enable_balance)
            continue


if __name__ == '__main__':
    update_balance_only_str = os.getenv('UPDATE_BALANCE_ONLY', 'False')
    update_balance_only = update_balance_only_str.lower() in ['true', '1', 't', 'y', 'yes']

    # Optionally start legacy TxAddrMappingUpdater thread. Disabled by default.
    start_updater_str = os.getenv('START_TX_ADDR_MAPPING_UPDATER', 'False')
    start_updater = start_updater_str.lower() in ['true', '1', 't', 'y', 'yes']

    tx_addr_mapping_updater = None

    # custom exception hook for thread
    def custom_hook(args):
        global tx_addr_mapping_updater
        # report the failure
        _logger.error(f'Thread failed: {args.exc_value}')
        thread = args.thread

        # check if TxAddrMappingUpdater
        if thread.name == 'TxAddrMappingUpdater':
            if tx_addr_mapping_updater is not None:
                p = threading.Thread(target=tx_addr_mapping_updater.loop, daemon=True, name="TxAddrMappingUpdater")
                p.start()
                raise Exception("TxAddrMappingUpdater thread crashed.")
            else:
                _logger.error('TxAddrMappingUpdater thread crashed but updater is disabled.')
                raise Exception('TxAddrMappingUpdater thread crashed but updater is disabled.')


    # set the exception hook
    threading.excepthook = custom_hook

    if not update_balance_only and start_updater:
        # run TxAddrMappingUpdater (legacy), only when explicitly enabled
        tx_addr_mapping_updater = TxAddrMappingUpdater()
        _logger.info('Starting updater thread now (legacy behavior).')
        threading.Thread(target=tx_addr_mapping_updater.loop, daemon=True, name="TxAddrMappingUpdater").start()
    else:
        if not start_updater:
            _logger.info('START_TX_ADDR_MAPPING_UPDATER not enabled: skipping legacy TxAddrMappingUpdater thread.')
        else:
            _logger.info('UPDATE_BALANCE_ONLY enabled: skipping TxAddrMappingUpdater thread.')

    _logger.info('Starting main thread now.')

    profile_str = os.getenv('PROFILE', 'False')
    profile = profile_str.lower() in ['true', '1', 't', 'y', 'yes']
    if profile:
        profiler = cProfile.Profile()
        profiler.enable()
        try:
            asyncio.run(main())
        finally:
            profiler.disable()
            profiler.dump_stats("profile_output.prof")
    else:
        asyncio.run(main())
