import os
import time
import json
import aiohttp
import asyncio
import websockets
from copy import deepcopy
from logging import Logger
from decimal import Decimal
from collections import defaultdict
from datetime import datetime, timezone
from sortedcontainers import SortedDict
from asyncio.queues import Queue as AsyncQueue

from bna.database import Database
from bna.config import BscConfig
from bna.transfer import CHAIN_BSC, Transfer, BscLog
from bna.tags import *
from bna.event import BlockEvent, ChainTransferEvent
from bna.utils import any_in, calculate_usd_value, widen


WIDNA_CONTRACT = "0x0de08c1abe5fb86dd7fd2ac90400ace305138d5b"
BTCB_CONTRACT = "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c"
BUSD_CONTRACT = "0xe9e7cea3dedca5984780bafc599bd69add087d56"
BSUSD_CONTRACT = "0x55d398326f99059ff775485246999027b3197955"
WBNB_CONTRACT = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
DOGE_CONTRACT = "0xba2ae424d960c26247dd6c32edc70b295c744c43"
MOON_CONTRACT = "0x42981d0bfbaf196529376ee702f2a9eb9092fcb5"
CURRENT_CONTRACT = WIDNA_CONTRACT
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
LP_MINT_TOPIC = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"
LP_BURN_TOPIC = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"
LP_TOPICS = [[LP_MINT_TOPIC, LP_BURN_TOPIC]]
NULL_ADDRESS = "0x0000000000000000000000000000000000000000"

class BscListener:
    def __init__(self, conf: BscConfig, db: Database, log: Logger):
        self.log = log.getChild("BL")
        self.db = db
        self.conf = conf
        self.last_block = 0
        self.logs: SortedDict[int, dict[int, BscLog]] = SortedDict()
        self.rpc_session = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=10))
        self.block_timestamps: dict[int, int] = {}
        self.tx_signers: dict[str, str] = {}  # @TODO: leaks
        self.subs = {}
        self.last_sub_id = 0

        self.rpc_url = os.environ['BSC_RPC_URL']
        self.ws_url = os.environ['BSC_WS_URL']
        if os.environ.get("BSC_HISTORY_RPC_URL"):
            self.history_rpc_url = os.environ['BSC_HISTORY_RPC_URL']
        else:
            self.history_rpc_url = self.rpc_url

    async def run(self, event_chan):
        ws_read_timeout = self.conf.ws_event_timeout
        logs_task = asyncio.create_task(self.logs_reader(event_chan), name="logs_reader")
        self.log.info("BSC listener started")
        # await self.fetch_missing(from_=25416520, until=25416525)
        self.last_block = (await self.db.get_last_block(CHAIN_BSC)) or 0
        self.log.debug(f"Resuming from block {self.last_block}")
        while True:
            self.last_sub_id = 0
            self.subs.clear()
            try:
                websocket = await websockets.connect(self.ws_url, max_size=None, max_queue=None)
                # @TODO Future: This is ~1M messages per month for not much benefit
                await self.ws_sub(websocket, ["newHeads"], 'head')
                await self.ws_sub(websocket, ["logs", {'address': WIDNA_CONTRACT, 'topics': [TRANSFER_TOPIC]}], 'log')
                for lp in self.db.known_by_type['pool'].keys():
                    await self.ws_sub(websocket, ["logs", {'address': lp, 'topics': LP_TOPICS}], 'log')
                    await self.ws_sub(websocket, ["logs", {'topics': [TRANSFER_TOPIC, None, widen(lp)]}], 'log')
                    await self.ws_sub(websocket, ["logs", {'topics': [TRANSFER_TOPIC, widen(lp), None]}], 'log')
                while True:
                    # @Update: for Python 3.11
                    # try:
                    #     async with asyncio.timeout(ws_read_timeout):
                    r = await websocket.recv()
                    # except TimeoutError:
                    #     self.log.warn(f"Timeout on socket read after {ws_read_timeout}, reconnecting")
                    #     try:
                    #         await websocket.close()
                    #         self.log.debug(f"Socket closed")
                    #     except Exception as e:
                    #         self.log.error(f'Error while closing socket: "{e}"')
                    #     break
                    j = json.loads(r)
                    if 'id' in j:
                        # print(j)
                        self.subs[j['result']] = self.subs[j['id']]
                    if 'method' in j:
                        if j['method'] == 'eth_subscription':
                            sub_name = self.subs[j['params']['subscription']]
                            if sub_name == 'head':
                                await self.new_head(j['params']['result'], event_chan)
                            elif sub_name == 'log':
                                await self.new_log(j['params']['result'])
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
            except Exception as e:
                self.log.error(f"WS exception: \"{e}\"", exc_info=True)
                await asyncio.sleep(1)
        logs_task.cancel()
        await self.rpc_session.close()
        await websocket.close()

    async def logs_reader(self, event_chan: AsyncQueue):
        "Consumes logs after some delay (for finality) and generates transfer events"
        num = -1
        while True:
            retry_block = 0
            try:
                self.log.info('BSC event generator started')
                while True:
                    await asyncio.sleep(1)
                    if len(self.logs) == 0:
                        continue
                    block: list[BscLog] = list(self.logs.peekitem(0)[1].values())
                    if len(block) == 0:
                        self.log.warning(f"Block after {num} has zero length")
                        continue
                    num = block[0].blockNumber
                    block_time = await self.get_block_time(num)
                    if not block_time or time.time() < block_time + self.conf.transfer_delay:
                        self.log.debug(f"Block {num} is early: {block_time}")
                        continue
                    self.log.info(f"Processing block {num}, ts={block_time}, {len(block)=}, {block}")
                    tfs = self.process_block(block)
                    self.logs.popitem(0)
                    if tfs:
                        event_chan.put_nowait(ChainTransferEvent(chain=CHAIN_BSC, tfs=tfs))
            except Exception as e:
                self.log.error(f"Reader exc, retry={retry_block}: {e}", exc_info=True)
                retry_block += 1
                await asyncio.sleep(1)
                if retry_block == 3:
                    self.log.error(f"Skipping block {num}")
                    self.logs.popitem(0)
                    continue

    async def ws_sub(self, ws, params: list, name: str):
        self.last_sub_id += 1
        sub = {'id': self.last_sub_id, 'method': 'eth_subscribe', 'params': params}
        self.subs[self.last_sub_id] = name
        await ws.send(json.dumps(sub))

    async def new_log(self, log: dict):
        log: BscLog = BscLog(**log)
        log.convert()
        self.log.debug(f"New tf_log: {log.blockNumber}:{log.logIndex:3d}, {log.removed=}, {log.transactionHash}")
        # Types of removes (probably not all of them):
        #   1. Whole block is removed because an earlier one replaced it -
        #      most TXes get moved to the next block.
        #   2. TX rearrangement, index changes within the same block.
        #   3. "Late TX", gets re-inserted in the next block.
        if log.removed:
            try:
                del self.logs[log.blockNumber][log.logIndex]
                # when all logs get removed from a block
                if len(self.logs[log.blockNumber]) == 0:
                    del self.logs[log.blockNumber]
            except:
                pass
            return
        block = self.logs.get(log.blockNumber, {})
        block[log.logIndex] = log
        self.logs[log.blockNumber] = block
        if log.transactionHash not in self.tx_signers:
            self.tx_signers[log.transactionHash] = await self.fetch_signer(log.transactionHash)

    async def new_head(self, head, event_chan: asyncio.Queue):
        num = int(head['number'], 16)
        timestamp = int(head['timestamp'], 16)
        now = int(time.time())
        if abs(now - timestamp) > 2:
            self.log.debug(f"New head: \t{num} {now=}-{timestamp=} = {now-timestamp}")
        self.block_timestamps[num] = timestamp
        for tf in self.logs.get(num, {}).values():
            tf.timeStamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        # shrink timestamps dict
        if len(self.block_timestamps) > 500:
            self.block_timestamps = dict(sorted(list(self.block_timestamps.items()))[300:])

        diff = num - self.last_block
        if diff > 1 and self.last_block != 0:
            self.log.warning(f"Block {num} has {diff=}, fetching potentially missing logs")
            await self.fetch_missing(from_=self.last_block, until=num)
            self.log.info("Finished fetching missing")
        self.last_block = num
        event_chan.put_nowait(BlockEvent(chain=CHAIN_BSC, height=int(num)))

    async def get_block_time(self, blockNumber) -> int:
        cached_time = self.block_timestamps.get(blockNumber)
        if cached_time:
            return cached_time
        else:
            self.log.warning(f"Block time cache miss: {blockNumber}")  # actually happens
            timestamp = time.time()
            for i in range(3):
                try:
                    block = await self.rpc_req('eth_getBlockByNumber', [hex(blockNumber), False])
                    timestamp = int(block['timestamp'], 16)
                    self.block_timestamps[blockNumber] = timestamp
                    break
                except Exception as e:
                    self.log.error(f"Error while fetching block time: {e}", exc_info=True)
                    await asyncio.sleep(2)
            else:
                self.block_timestamps[blockNumber] = timestamp
                self.log.warning(f"Assigning current time {int(timestamp)} to block {blockNumber}")
            return timestamp

    def process_block(self, block: list[BscLog]) -> list[Transfer]:
        "Extract transfers from all logs in a block once it's deemed final"
        block.sort(key=lambda log: log.logIndex)
        by_hash = defaultdict(list)
        for log in block:
            by_hash[log.transactionHash].append(log)
            log.signer = self.tx_signers.get(log.transactionHash, NULL_ADDRESS)

        tfs = []
        for tx_logs in by_hash.values():
            tf = self.process_tx_logs(tx_logs)
            if tf:
                tfs.append(tf)
            else:
                self.log.warning(f"Not tf? {tf=}")
        return tfs

    def process_tx_logs(self, logs: list[BscLog]) -> Transfer:
        tags = set()
        meta = {'lp': defaultdict(lambda: {'idna': Decimal(0), 'token': Decimal(0)}),
                'token': defaultdict(Decimal)}
        tfs = []
        for log in logs:
            topic = log.topics[0]
            if topic == TRANSFER_TOPIC:
                if log.address == WIDNA_CONTRACT:
                    tf = Transfer.from_bsc_log(log, timeStamp=self.block_timestamps.get(log.blockNumber))
                    if not tf.should_store():
                        continue
                    tfs.append(tf)
                    for addr, amount in tf.changes.items():
                        if self.db.addr_type(addr) == 'pool':
                            tags.add(DEX_TAG)
                            if amount < 0:
                                tags.add(DEX_TAG_BUY)
                            elif amount > 0:
                                tags.add(DEX_TAG_SELL)
                        elif addr == NULL_ADDRESS:
                            if amount < 0:
                                tags.add(BSC_TAG_BRIDGE_MINT)
                            elif amount > 0:
                                tags.add(BSC_TAG_BRIDGE_BURN)
                        # if len(tags) == 0:
                        #     tags.add(IDENA_TAG_SEND)
                else:
                    token = self.db.known.get(log.address, None)
                    if token is None or token['type'] != 'token':
                        self.log.warning(f'Transfer log of unknown token "{log.address}", skipping')
                        continue
                    tf_from = log.topic_as_addr(1)
                    tf_to = log.topic_as_addr(2)
                    amount = Decimal(int(log.data, 16)) / Decimal(10**token['decimals'])
                    if self.db.addr_type(tf_from) == 'pool':
                        meta['token'][log.address] -= amount
                    elif self.db.addr_type(tf_to) == 'pool':
                        meta['token'][log.address] += amount
                    else:
                        self.log.warning(f"Transfer log is not related to any pools! {log=}")

            elif topic in LP_TOPICS[0] and self.db.addr_type(log.address) == 'pool':
                amount_idna = Decimal(int(log.data[2:][:64], 16)) / Decimal(10**18)
                amount_token = Decimal(int(log.data[2:][64:], 16)) / Decimal(10**18)
                if topic == LP_MINT_TOPIC:
                    meta['lp'][log.address]['idna'] += amount_idna
                    meta['lp'][log.address]['token'] += amount_token
                    tags.add(DEX_TAG_PROVIDE_LP)
                elif topic == LP_BURN_TOPIC:
                    meta['lp'][log.address]['idna'] -= amount_idna
                    meta['lp'][log.address]['token'] -= amount_token
                    tags.add(DEX_TAG_WITHDRAW_LP)
            else:
                self.log.warning(f"Unknown log: {log}")

        if DEX_TAG_PROVIDE_LP in tags and DEX_TAG_WITHDRAW_LP in tags:
            # shouldn't happen, but you never know with those stupid sexy MEV freaks
            self.log.warning(f"Both mint and burn in TX: {logs[0].transactionHash}")
        if DEX_TAG_BUY in tags and DEX_TAG_SELL in tags:
            tags.remove(DEX_TAG_BUY)
            tags.remove(DEX_TAG_SELL)
            tags.add(DEX_TAG_ARB)

        if len(tags) == 0:
            tags.add(IDENA_TAG_SEND)

        if not tfs:
            self.log.warning(f"tfs is empty despite logs received! {logs=}")
            return None
        squashed = BscListener.squash(tfs)

        if any_in(tags, [DEX_TAG_PROVIDE_LP, DEX_TAG_WITHDRAW_LP]):
            tags -= set((DEX_TAG_BUY, DEX_TAG_SELL, DEX_TAG_ARB))
            # User can deposit/withdraw any ratio of pooled tokens and the rest will be
            # converted by buying or selling into that pool. `excess` shows the swapped amount.
            excess = Decimal(0)
            for pool_addr, pool in meta['lp'].items():
                excess += pool['idna'] - squashed.changes.get(pool_addr, Decimal(0))

            if excess != Decimal(0):
                meta['lp_excess'] = excess
                if excess > Decimal(0):
                    tags.add(DEX_TAG_BUY)
                elif excess < Decimal(0):
                    tags.add(DEX_TAG_SELL)

        # Remove empty fields from meta and convert defaultdict to dict
        new_meta = {}
        for name, obj in meta.items():
            if not obj:
                continue
            if name == 'lp':
                for pool_addr, pool_ch in obj.items():
                    if pool_ch['idna'] == 0 and pool_ch['token'] == 0:
                        continue
                    else:
                        new_meta['lp'] = new_meta.get('lp', {})
                        new_meta['lp'][pool_addr] = dict(pool_ch)
            elif name == 'token':
                obj = dict(filter(lambda kv: kv[1] != 0, obj.items()))
                if len(obj) != 0:
                    new_meta['token'] = obj
            else:
                new_meta[name] = obj

        tags = sorted(list(tags))  # sort just because
        squashed.tags = tags
        squashed.meta = deepcopy(new_meta)
        squashed.meta['usd_value'] = calculate_usd_value(squashed, self.db.prices, self.db.known)
        try:
            if squashed.meta['usd_value'] and DEX_TAG in squashed.tags:
                if any_in(squashed.tags, DEX_LP_TAGS):
                    pool_addr, ch = list(squashed.meta['lp'].items())[0]  # no multi-pool nonsense
                    pool = self.db.known[pool_addr]
                    token_price = self.db.prices[self.db.known[self.db.known[pool_addr]['token1']]['price_id']]
                    squashed.meta['usd_price'] = abs((float(ch['token']) * token_price) / float(ch['idna']))
                elif any_in(squashed.tags, DEX_TRADE_TAGS) and squashed.value():
                    squashed.meta['usd_price'] = squashed.meta['usd_value'] / float(squashed.value())
        except Exception as e:
            self.log.error(f"Error calculating USD price for '{squashed.hash}': {e}", exc_info=True)
        self.log.debug(f"{squashed=}")
        return squashed

    async def fetch_missing(self, from_: int, until: int, batch=10000):
        # I wrote this batching thing because I thought there was a limit of 10000 blocks per
        # request on QuickNode, but actually it's just 10000 blocks from the tip.
        url = self.history_rpc_url
        for batch_from in range(from_, until, batch):
            batch_until = (batch_from + batch) if (until - (batch_from + batch)) > batch else until
            self.log.debug(f"Processing batch {batch_from}-{batch_until}")
            params = [{"topics": [TRANSFER_TOPIC], "address": CURRENT_CONTRACT,
                    #    "fromBlock": from_ + 1, "toBlock": until - 1}]
                    "fromBlock": hex(batch_from + 1), "toBlock": hex(batch_until - 1)}]
            # Fetch iDNA transfer logs
            logs: list[dict] = await self.rpc_req('eth_getLogs', params, url=url) or []
            for lp in self.db.known_by_type['pool'].keys():
                # Fetch LP token mint/burn logs
                params[0]['topics'] = LP_TOPICS
                params[0]['address'] = lp
                logs.extend(await self.rpc_req('eth_getLogs', params=params, url=url) or [])
                await asyncio.sleep(0.3) # rate limiting

                # Fetch transfer of any token to/from pools
                del params[0]['address']
                params[0]['topics'] = [TRANSFER_TOPIC, widen(lp), None]
                logs.extend(await self.rpc_req('eth_getLogs', params=params, url=url) or [])
                params[0]['topics'] = [TRANSFER_TOPIC, None, widen(lp)]
                logs.extend(await self.rpc_req('eth_getLogs', params=params, url=url) or [])
            self.log.debug(f"Fetched {len(logs)=}")

            new_block_times = {}
            new_signers = {}
            for log in logs:
                blockNumber = int(log['blockNumber'], 16)
                # Since it's possible that "removed=True logs" weren't received,
                # old logs for newly fetched blocks must be cleared.
                if blockNumber in self.logs:
                    del self.logs[blockNumber]
                if blockNumber not in self.block_timestamps and blockNumber not in new_block_times:
                    block = await self.rpc_req('eth_getBlockByNumber', [hex(blockNumber), False])
                    timestamp = int(block['timestamp'], 16)
                    self.log.debug(f"Missing head: \t{blockNumber} {timestamp}")
                    new_block_times[blockNumber] = timestamp
                hash = log['transactionHash']
                if hash not in self.tx_signers and hash not in new_signers:
                    signer = await self.fetch_signer(hash)
                    self.log.debug(f"Missing signer: \t{hash} {signer}")
                    new_signers[hash] = signer

            # This is done separately to avoid awaiting in the middle of state modification
            self.block_timestamps.update(new_block_times)
            self.tx_signers.update(new_signers)

            logs.sort(key=lambda l: (int(l['blockNumber'], 16), int(l['logIndex'], 16)))
            self.log.debug(f"Inserting {len(logs)=}")
            for log in logs:
                # await here is really bad because it could cause the reader to read an incomplete block,
                # unless timestamps and signers are fetched ahead of time, and they are
                await self.new_log(log)

    async def fetch_signer(self, tx_hash) -> str:
        "Fetched the full transaction from the RPC and returns its signer."
        # defaulting to the zero address better than failing?
        signer = NULL_ADDRESS
        # Sometimes TX info isn't available immediately
        for i in range(3):
            try:
                tx = await self.rpc_req('eth_getTransactionByHash', [tx_hash], attempts=3)
                signer = tx['from'].lower()
                break
            except Exception as e:
                self.log.error(f"Failed to fetch signer for {tx_hash=}: {e}", exc_info=True)
                self.log.debug(f"{tx=}")
                await asyncio.sleep(1)
        return signer

    async def rpc_req(self, method, params, id=0, attempts=60, url=None) -> dict:
        if url is None:
            url = self.rpc_url
        self.log.debug(f"rpc_req {method=}, {params=} url={url[:30]}")
        req = {"method": method, "params": params, "id": id}
        attempt = 0
        resp = None
        while attempt < attempts:
            try:
                resp = await self.rpc_session.post(url, json=req)
                j = await resp.json(content_type=None)
                if 'error' in j:
                    self.log.error(f"RPC Error, returning None: {j['error']}")
                return j['result']
            except Exception as e:
                attempt += 1
                self.log.warn(f'API fetch #{attempt}/{attempts} error: "{e}"')
                self.log.debug(f'{resp=}')
                if attempt == attempts:
                    raise e
                await asyncio.sleep(2)

    # TODO: Test this
    def squash(transfers: list[Transfer]) -> Transfer:
        """
        Takes a list of Transfers in a single TX and returns a "squashed" Transfer.
        Removes intermediary addresses and combines many Transfers from one address into a
        single Transfer to a list of addresses.
        """
        changes = defaultdict(Decimal)
        transfers.sort(key=lambda tf: tf.logIndex)
        main_tf = transfers[0]
        for tf in transfers:
            assert tf.hash == main_tf.hash
            for addr, change in tf.changes.items():
                if change != 0:
                    changes[addr] += change
        main_tf.changes = {a: changes[a] for a in changes if not changes[a].is_zero()}
        return main_tf


    async def _fetch_txs(self, tx_hashes: list[str]) -> list[Transfer]:
        "Fetches a list of BSC TXes and returns a list of Transfers, only used for development."
        self.log.info(f"Fetching txs: {tx_hashes}")
        tfs = []
        for tx_hash in filter(lambda h: h, tx_hashes):
            logs: SortedDict[int, dict[int, BscLog]] = SortedDict()
            tx = await self.rpc_req('eth_getTransactionReceipt', [tx_hash], attempts=3)
            signer = tx['from'].lower()
            self.tx_signers[tx_hash] = signer
            for log in tx['logs']:
                log: BscLog = BscLog(**log)
                log.convert()
                log.signer = signer
                self.log.debug(f"Fetched tf_log: {log.blockNumber}:{log.logIndex:3d}, {log.transactionHash}")
                block = logs.get(log.blockNumber, {})
                block[log.logIndex] = log
                logs[log.blockNumber] = block
                if log.blockNumber not in self.block_timestamps:
                    block = await self.rpc_req('eth_getBlockByNumber', [hex(log.blockNumber), False])
                    timestamp = int(block['timestamp'], 16)
                    self.log.debug(f"Fetched head: \t{log.blockNumber} {timestamp}")
                    self.block_timestamps[log.blockNumber] = timestamp
            for block in logs.values():
                block = sorted(block.values(), key=lambda l: l.logIndex)
                block_tfs = self.process_block(block)
                tfs.extend(block_tfs)
        return tfs
