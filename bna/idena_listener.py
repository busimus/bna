import os
import time
import json
import aiohttp
import asyncio
import datetime
from logging import Logger
from sortedcontainers import SortedDict
from decimal import Decimal
from urllib.parse import urljoin

from bna.config import IdenaConfig
from bna.database import Database
from bna.tags import *
from bna.transfer import CHAIN_IDENA, Transfer
from bna.utils import calculate_usd_value, rpc_api_type_map


class IdenaListener:
    def __init__(self, conf: IdenaConfig, log: Logger, db: Database):
        self.conf = conf
        self.db = db
        print(os.environ)
        self.rpc_url = os.environ.get('IDENA_RPC_URL')
        self.api_url = os.environ.get('IDENA_API_URL')
        self.rpc_key = os.environ.get('IDENA_RPC_KEY')
        if log is None:
            self.log = Logger("IL")
        else:
            self.log = log.getChild("IL")
        self.trs = SortedDict()
        self.block_timestamps = {}
        self.rpc_session = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=2))
        self.api_session = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=10))
        self.slow_trs = []  # for killtx, which can take a minute to fetch from the indexer

    async def run_old(self, event_chan):
        self.log.info("Idena listener started")
        mempool_task = asyncio.create_task(self.mempool_watcher(), name='idena_mempool_watcher')
        ident_task = asyncio.create_task(self.identities_cacher(), name='idena_identity_fetcher')
        last_block = await self.db.get_last_block(CHAIN_IDENA)
        self.log.debug(f"Resuming from block {last_block}")
        # Get height of the last block if DB is empty
        while last_block is None:
            try:
                block = await self.rpc_req('bcn_lastBlock')
                last_block = block['height']
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
            except Exception as e:
                self.log.error(f'bcn_lastBlock exception: "{e}"', exc_info=True)
                await asyncio.sleep(3)

        retry_block = 0
        while True:
            # Try to get the next block
            try:
                block = await self.rpc_req('bcn_blockAt', [last_block + 1])
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
            except Exception as e:
                self.log.error(f'bcn_blockAt exception ({retry_block=}): "{e}"', exc_info=True)
                await asyncio.sleep(3)
                continue
            # Try to process new block
            try:
                if block is None:
                    continue
                if block is not None and last_block is not None:
                    self.log.debug(f"Got block: {block['height']}")
                    trs = await self.process_block(block)
                    if len(trs) != 0:
                        event_chan.put_nowait(trs)
                    last_block += 1

                    # If node got behind on blocks, sleep for less time to catch up quickly
                    diff = time.time() - block['timestamp']
                    if abs(diff) > 30:
                        self.log.warn(f"Block time irregularity: {diff=:.1f}")
                        await asyncio.sleep(0.05)
                        continue
                if len(self.slow_trs) > 0:
                    self.log.info(f"Got slow_trs, {len(self.slow_trs)=}")
                    event_chan.put_nowait(self.slow_trs.copy())
                    self.slow_trs.clear()
                retry_block = 0
                await asyncio.sleep(self.conf.rpc_interval)
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
            except Exception as e:
                self.log.error(f'idena.run exception ({retry_block=}): "{e}"', exc_info=True)
                retry_block += 1
                if retry_block > 10 and last_block is not None:
                    retry_block = 0
                    last_block += 1
                    continue
                await asyncio.sleep(3)
        ident_task.cancel()
        mempool_task.cancel()
        await self.rpc_session.close()
        await self.api_session.close()

    async def run(self, event_chan):
        self.log.info("Idena listener started")
        mempool_task = asyncio.create_task(self.mempool_watcher(), name='idena_mempool_watcher')
        ident_task = asyncio.create_task(self.identities_cacher(), name='idena_identity_fetcher')

        last_block: int = await self.db.get_last_block(CHAIN_IDENA)
        block: dict = None
        state: str = 'GET_NEXT_BLOCK' if last_block is not None else 'GET_HEIGHT'
        retry_block = 0
        self.log.debug(f"Resuming from block {last_block}")

        while True:
            # self.log.debug(f"{last_block=} {state=}")
            try:
                if state == 'GET_HEIGHT':
                    block = await self.rpc_req('bcn_lastBlock')
                    last_block = block['height']
                    state = 'GET_NEXT_BLOCK'
                elif state == 'GET_NEXT_BLOCK':
                    block = await self.rpc_req('bcn_blockAt', [last_block + 1])
                    if block:
                        self.log.debug(f"Got block: {block['height']}")
                        state = 'PROCESS_BLOCK'
                    else:
                        state = 'AFTER_BLOCK'
                        await asyncio.sleep(self.conf.rpc_interval)
                elif state == 'PROCESS_BLOCK':
                    tfs = await self.process_block(block)
                    if len(tfs) != 0:
                        event_chan.put_nowait({'type': 'transfers', 'chain': CHAIN_IDENA, 'tfs': tfs})
                    last_block += 1
                    event_chan.put_nowait({'type': 'block', 'chain': CHAIN_IDENA, 'value': int(block['height'])})
                    state = 'AFTER_BLOCK'
                    await asyncio.sleep(0.05)  # to not overload the node during catchup
                elif state == 'AFTER_BLOCK':
                    if len(self.slow_trs) > 0:
                        self.log.info(f"Got slow_trs, {len(self.slow_trs)=}")
                        for tr in self.slow_trs:
                            event_chan.put_nowait({'type': 'transfers', 'chain': CHAIN_IDENA, 'tfs': [tr]})
                        self.slow_trs.clear()
                    state = 'GET_NEXT_BLOCK'
                else:
                    self.log.error(f"No such state: {state}")
                    state = 'GET_NEXT_BLOCK'
                    await asyncio.sleep(3)
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
            except Exception as e:
                self.log.error(f'idena.run exception, {state=} ({retry_block=}): "{e}"', exc_info=True)
                if state == 'PROCESS_BLOCK':
                    retry_block += 1
                    if retry_block > 10:
                        state = 'GET_NEXT_BLOCK'
                        retry_block = 0
                        last_block += 1
                elif state == 'AFTER_BLOCK':
                    state = 'GET_NEXT_BLOCK'
                await asyncio.sleep(3)
        ident_task.cancel()
        mempool_task.cancel()
        await self.rpc_session.close()
        await self.api_session.close()

    async def process_block(self, block) -> list[Transfer]:
        self.block_timestamps[block['height']] = block['timestamp']
        block_txs = block['transactions'] if block['transactions'] else []
        tfs = []
        for i, tx_hash in enumerate(block_txs):
            tx = await self.rpc_req('bcn_transaction', [tx_hash])
            if tx is None and self.api_url:
                self.log.warning(f"TX missing from node: {tx_hash}")
                tx = await self.api_req(f'transaction/{tx_hash}')
                await asyncio.sleep(0.2)
                tx['timestamp'] = int(datetime.datetime.strptime(tx['timestamp'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp())
                tx['type'] = rpc_api_type_map[tx['type']]
            elif tx is None:
                self.log.warning(f"TX missing from node and no API URL is set: {tx_hash}")
                continue
            tf = await self.process_tx(tx, block['height'], i)
            if tf and tf.should_store():
                tfs.append(tf)
        return tfs

    async def process_tx(self, tx, blockNumber, logIndex) -> Transfer | None:
        tf = None
        tx['to'] = tx['to'].lower() if tx.get('to') else None
        tx['from'] = tx['from'].lower() if tx.get('from') else None
        if tx['type'] == IDENA_TAG_SEND:
            if tx['to'] == self.db.addrs_of_type('bridge')[0]:
                try:
                    bpayload = bytes.fromhex(tx['payload'][2:])
                    _, dest = bpayload.split(b'BSCADDRESS')
                    tags = [IDENA_TAG_BRIDGE_BURN]
                    meta = {'bridge_to': dest.decode('utf-8').lower()}
                except Exception as e:
                    self.log.warning(f"Bridge decode exception, probably not a bridge: {e}", exc_info=True)
                    tags = [IDENA_TAG_BRIDGE_BURN_WRONG]  # lmao
                    meta = {}
                tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=tags)
                tf.meta = meta
            else:
                tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
        elif tx['type'] == IDENA_TAG_DELEGATE:
            pool = tx['to']
            tx['to'] = None
            tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
            tf.meta['pool'] = pool
        elif tx['type'] == IDENA_TAG_UNDELEGATE:
            ident = self.db.get_identity(tx['from'])
            tx['to'] = None
            if ident is not None:
                tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
                tf.meta['pool'] = ident.get('delegatee')
        elif tx['type'] in [IDENA_TAG_KILL, IDENA_TAG_KILL_DELEGATOR]:
            if tx['type'] == IDENA_TAG_KILL:
                killed = tx['from']
            else:
                killed = tx['to']
                tx['to'] = None
            ident = self.db.get_identity(killed)
            # if identity is in cache, pull info out of it
            if ident is not None and time.time() - ident.get('_fetchTime', 0) < 3600:
                stake = ident.get('stake', '0')
                tx['amount'] = stake
                tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=['kill'])
                tf.meta['killedIdentity'] = killed
                tf.meta['age'] = ident.get('age', 0)
                tf.meta['pool'] = ident.get('delegatee')
                tf.meta['usd_value'] = float(stake) * self.db.prices['cg:idena']
                self.log.debug(f"Created kill TF from cache: {tf}")
            else:
                # otherwise fetch from indexer asynchronously
                tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=['kill'])
                tf.meta['killedIdentity'] = killed
                asyncio.create_task(self.fetch_killtx(tx, tf), name='fetch_killtx')

            # Without this the identity would be sending coins on termination
            for ch_addr, change in tf.changes.items():
                tf.changes[ch_addr] = -1 * change
        elif tx['type'] in IDENA_COUNTABLE_TAGS:
            tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
        elif tx['type'] == IDENA_TAG_STAKE:
            tx['to'] = None  # locking coins isn't a self transfer
            tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
        elif tx['type'] == IDENA_TAG_DEPLOY:
            if 'txReceipt' in tx: # if fetched from API
                receipt = tx['txReceipt']
                receipt['contract'] = receipt['contractAddress']
            else:
                receipt = await self.rpc_req('bcn_txReceipt', [tx['hash']])
            if receipt['success']:
                tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
                tf.meta['call'] = {'contract': receipt['contract'].lower()}
        elif tx['type'] == IDENA_TAG_CALL:
            if 'txReceipt' in tx: # if fetched from API
                receipt = tx['txReceipt']
                receipt['contract'] = receipt['contractAddress']
            else:
                receipt = await self.rpc_req('bcn_txReceipt', [tx['hash']])
            if receipt['success']:
                # TODO: try to parse the payload
                tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
                tf.meta['call'] = {'contract': receipt['contract'].lower()}
        else:
            pass
        if tf and tf.meta.get('usd_value') is None:
            tf.meta['usd_value'] = calculate_usd_value(tf, self.db.prices, self.db.known)
        return tf

    async def fetch_killtx(self, tx: dict, tf: Transfer):
        "Fetch the killtx amount and age from the indexer, which may take over a minute because it's slow to update"
        if not self.api_url:
            self.log.warning("No API URL set, can't fetch killtx data")
            return
        # Sometimes indexer sends garbage data, so multiple attempts are necessary
        attempt = 0
        api_tx = None
        while attempt < 20:
            try:
                cur_epoch = (await self.api_req('epoch/last', attempts=5))['epoch']
                birth_epoch = (await self.api_req(f'epoch/{cur_epoch - 1}/identity/{tf.meta["killedIdentity"]}', attempts=5))['birthEpoch']
                age = cur_epoch - birth_epoch
                api_tx = await self.api_req(f'transaction/{tf.hash}', attempts=50)
                stake = Decimal(api_tx['data']['transfer'])
                tx['value'] = stake
                tf.changes = Transfer.create_changes(tx)
                tf.meta['age'] = age
                tf.meta['usd_value'] = float(stake) * self.db.prices['cg:idena']
                self.log.debug(f"Created kill TF: {tf}")
                self.slow_trs.append(tf)
                break
            except:
                self.log.error(f"Failed to fetch killtx for hash={tf.hash}", exc_info=True)
                self.log.debug(f"Data for the failed fetch: {api_tx}")
                attempt += 1
                await asyncio.sleep(2)

    async def rpc_req(self, method=None, params = [], data=None, id=0):
        if data is None:
            data = json.dumps({"method": method,"params": params,"id": id, "key": self.rpc_key})
        resp = await self.rpc_session.post(self.rpc_url, data=data)
        # self.log.debug(f"{await resp.text()=}")
        j = await resp.json()
        return j['result']

    async def api_req(self, path, attempts=1):
        self.log.debug(f"api_req {path=}")
        attempt = 0
        while attempt < attempts:
            try:
                resp = await self.rpc_session.get(urljoin(self.api_url, path))
                j = await resp.json()
                return j['result']
            except Exception as e:
                attempt += 1
                self.log.warn(f'API fetch #{attempt}/{attempts} error: "{e}"')
                if attempt == attempts:
                    raise e
                await asyncio.sleep(10)

    async def mempool_watcher(self):
        mempool_req = json.dumps({"method": "bcn_mempool","params": [], "id": 123, "key": self.rpc_key})
        seen = set()
        while True:
            await asyncio.sleep(1)
            try:
                mempool = await self.rpc_req(data=mempool_req)
            except Exception as e:
                self.log.warning(f'Mempool error: "{e}"', exc_info=True)
                continue
            if mempool is None or len(mempool) == 0:
                seen.clear() # no reason to keep old hashes
                continue

            for tx_hash in mempool:
                if tx_hash in seen:
                    continue
                seen.add(tx_hash)
                self.log.debug(F"Mempool tx: {tx_hash}")
                try:
                    tx = await self.rpc_req("bcn_transaction", [tx_hash])
                except Exception as e:
                    self.log.warning(f'Mempool TX error: "{e}"', exc_info=True)
                    continue
                if tx['type'] == 'kill' or True:
                    try:
                        ident = await self.rpc_req("dna_identity", [tx['from']])
                        if ident['state'].lower() != 'undefined':
                            ident['_fetchTime'] = int(time.time())
                            await self.db.insert_identity(ident)
                    except Exception as e:
                        self.log.warning(f'Mempool KillTX error: "{e}"', exc_info=True)
                        continue

    async def identities_cacher(self):
        "Gets all identities from the node and inserts them into DB. Takes 5-10 seconds per fetch, so it's done rarely."
        idents_req = json.dumps({"method": "dna_identities","params": [], "id": 123, "key": self.rpc_key})
        session = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=60))
        await asyncio.sleep(5)
        while True:
            try:
                self.log.info("Fetching identities...")
                resp = await session.post(self.rpc_url, data=idents_req)
                identities = (await resp.json())['result']
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
            except Exception as e:
                self.log.warning(f'Identity fetch error: "{e}"', exc_info=True)
                await asyncio.sleep(60)
                continue

            now = int(time.time())
            for ident in identities:
                ident['_fetchTime'] = now
                for field in ['pubkey', 'flipKeyWordPairs', 'code', 'generation', 'profileHash', 'delegationEpoch', 'delegationNonce', 'pendingUndelegation', 'invitees', 'inviter', 'shardId']:
                    try:
                        del ident[field]
                    except:
                        pass

            self.log.debug(f"Inserting {len(identities)} identities...")
            await self.db.insert_identities(identities)
            self.log.info("Identities cached")
            try:
                await asyncio.sleep(self.conf.identities_cache_interval)
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
        await session.close()
