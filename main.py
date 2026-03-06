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
        _logger.info("Finding start block...")
        low_hash = start_hash
        found = False
        headers_processed = 0
        used_low_hashes = []
        while found == False:
            resp = await client.request("getBlocksRequest",
                                             params={
                                                 "lowHash": low_hash,
                                                 "includeBlocks": True,
                                                 "includeTransactions": False
                                             },
                                             timeout=60)
            # go through each block and yield
            if resp is not None:
                block_hashes = resp["getBlocksResponse"].get("blockHashes", [])
                blocks = resp["getBlocksResponse"].get("blocks", [])
                _logger.info(f"Current low hash: {low_hash}, found {len(blocks)} blocks.")
                for i in range(len(blocks)):
                    block = blocks[i]
                    block_hash = block_hashes[i]
                    isHeaderOnly = block["verboseData"].get('isHeaderOnly')
                    _logger.info(f"Block hash: {block_hash}, isHeaderOnly: {isHeaderOnly}")
                    if isHeaderOnly != True:
                        found = True
                        start_block = block
                        start_hash = block_hash
                        _logger.info(f"Found start block: {start_hash}")
                headers_processed += len(blocks)
                _logger.info(f'Processed {headers_processed} headers so far.')
                used_low_hashes.append(low_hash)
                for i, block in reversed(list(enumerate(blocks))):
                    _logger.info(f"Checking next possible low hash {block['verboseData']['hash']}")
                    if block['verboseData']['hash'] != low_hash:
                        hash_used = False
                        for used_low_hash in used_low_hashes:
                            if used_low_hash == block['verboseData']['hash']:
                                hash_used = True
                        if hash_used == False:
                            low_hash = block['verboseData']['hash']
                            break
        used_low_hashes = []

    batch_processing_str = os.getenv('BATCH_PROCESSING', 'False')  # Default to 'False' if not set
    batch_processing = batch_processing_str.lower() in ['true', '1', 't', 'y', 'yes']

    env_enable_balance_str = os.getenv('BALANCE_ENABLED', 'False')
    env_enable_balance = env_enable_balance_str.lower() in ['true', '1', 't', 'y', 'yes']
    env_update_balance_on_boot_str = os.getenv('UPDATE_BALANCE_ON_BOOT', 'False')
    env_update_balance_on_boot = env_update_balance_on_boot_str.lower() in ['true', '1', 't', 'y', 'yes']
    bap = BalanceProcessor(client)
    if env_update_balance_on_boot is not False: 
        await bap.update_all_balances()
    env_update_balance_only_str = os.getenv('UPDATE_BALANCE_ONLY', 'False')
    env_update_balance_only = env_update_balance_only_str.lower() in ['true', '1', 't', 'y', 'yes']
    if env_update_balance_only is not False:
        while True:
            await bap.update_all_balances()
            # Use non-blocking sleep inside async context
            await asyncio.sleep(1800)

    # create instances of blocksprocessor and virtualchainprocessor
    vcp = VirtualChainProcessor(client, start_block, start_hash)
    bp = BlocksProcessor(client, vcp, bap, batch_processing, env_enable_balance)

    # start blocks processor working concurrent
    while True:
        try:
            await bp.loop(start_hash)
        except Exception:
            _logger.exception('Exception occured and script crashed..')
            raise


if __name__ == '__main__':
    cProfile.run("main()", "profile_output.prof")
    tx_addr_mapping_updater = TxAddrMappingUpdater()


    # custom exception hook for thread
    def custom_hook(args):
        global tx_addr_mapping_updater
        # report the failure
        _logger.error(f'Thread failed: {args.exc_value}')
        thread = args[3]

        # check if TxAddrMappingUpdater
        if thread.name == 'TxAddrMappingUpdater':
            p = threading.Thread(target=tx_addr_mapping_updater.loop, daemon=True, name="TxAddrMappingUpdater")
            p.start()
            raise Exception("TxAddrMappingUpdater thread crashed.")


    # set the exception hook
    threading.excepthook = custom_hook

    # run TxAddrMappingUpdater
    # will be rerun
    _logger.info('Starting updater thread now.')
    threading.Thread(target=tx_addr_mapping_updater.loop, daemon=True, name="TxAddrMappingUpdater").start()
    _logger.info('Starting main thread now.')
    asyncio.run(main())
