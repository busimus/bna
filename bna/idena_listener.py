import os
import time
import json
import aiohttp
import asyncio
from logging import Logger
from decimal import Decimal
from urllib.parse import urljoin
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from sortedcontainers import SortedDict

from bna.config import IdenaConfig
from bna.database import Database
from bna.tags import *
from bna.event import BlockEvent, ChainTransferEvent, ClubEvent
from bna.transfer import CHAIN_IDENA, Transfer
from bna.utils import calculate_usd_value
from bna.models_pb2 import ProtoTransaction, ProtoCallContractAttachment

RPC_API_TYPE_MAP = {
    'SendTx': "send",
    'ActivationTx': "activation",
    'InviteTx': "invite",
    'KillTx': "kill",
    'KillInviteeTx': "killInvitee",
    'SubmitFlipTx': "submitFlip",
    'SubmitAnswersHashTx': "submitAnswersHash",
    'SubmitShortAnswersTx': "submitShortAnswers",
    'SubmitLongAnswersTx': "submitLongAnswers",
    'EvidenceTx': "evidence",
    'OnlineStatusTx': "online",
    'ChangeGodAddressTx': "changeGodAddress",
    'BurnTx': "burn",
    'ChangeProfileTx': "changeProfile",
    'DeleteFlipTx': "deleteFlip",
    'DeployContractTx': "deployContract",
    'CallContractTx': "callContract",
    'TerminateContractTx': "terminateContract",
    'DelegateTx': "delegate",
    'UndelegateTx': "undelegate",
    'KillDelegatorTx': "killDelegator",
    'StoreToIpfsTx': "storeToIpfs",
    'ReplenishStakeTx': "replenishStake",
}

USELESS_IDENT_FIELDS = ['code', 'flips', 'inviter', 'pubkey', 'invites', 'shardId', 'invitees',
                        'generation', 'profileHash', 'requiredFlips', 'madeFlips', 'availableFlips',
                        'penalty', 'penaltySeconds', 'flipKeyWordPairs', 'lastValidationFlags',
                        'totalQualifiedFlips', 'totalShortFlipPoints', 'flipsWithPair']

MINING_STATES = {'Newbie', 'Verified', 'Human'}

BNA_CONTRACT_ADDRESS = '0xa877f4632dff78f8b87f835379f844e260d0245d'

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
        self.slow_tfs = []  # for killtx, which can take a minute to fetch from the indexer
        self.update_identities = set() # to get correct stake after replenishment
        self.event_chan = None
        self.apy_data = {'updated': datetime.min.replace(tzinfo=timezone.utc)}

    async def run(self, event_chan):
        self.log.info("Idena listener started")
        self.event_chan = event_chan
        mempool_task = asyncio.create_task(self.mempool_watcher(), name='idena_mempool_watcher')
        ident_task = asyncio.create_task(self.identities_cacher(), name='idena_identity_fetcher')

        last_block: int = await self.db.get_last_block(CHAIN_IDENA)
        # last_block = 5549620
        block: dict = None
        state: str = 'GET_NEXT_BLOCK' if last_block is not None else 'GET_HEIGHT'
        retry_block = 0
        update_identities = set()
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
                        event_chan.put_nowait(ChainTransferEvent(chain=CHAIN_IDENA, tfs=tfs))
                    last_block += 1
                    event_chan.put_nowait(BlockEvent(chain=CHAIN_IDENA, height=int(block['height'])))
                    state = 'AFTER_BLOCK'
                    await asyncio.sleep(0.05)  # to not overload the node during catchup
                elif state == 'AFTER_BLOCK':
                    if len(self.slow_tfs) > 0:
                        self.log.info(f"Got slow_tfs, {len(self.slow_tfs)=}")
                        for tf in self.slow_tfs:
                            event_chan.put_nowait(ChainTransferEvent(chain=CHAIN_IDENA, tfs=[tf]))
                        self.slow_tfs.clear()
                    if len(self.update_identities) > 0:
                        self.log.info(f"Got update_identities, {len(self.update_identities)=}")
                        for addr in self.update_identities:
                            await self.update_identity(addr)
                        # self.db.update_rankings()
                        self.update_identities.clear()
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
                    if retry_block > 5:
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
                tx['type'] = RPC_API_TYPE_MAP[tx['type']]
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
            bridge = self.db.addrs_of_type('bridge')[0]
            if tx['to'] == bridge:
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
            elif tx['from'] == bridge:
                tags = [IDENA_TAG_BRIDGE_MINT]
                tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=tags)
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
                if not tf.meta['pool']:
                    self.log.warning(f"Undelegate tx with no delegatee in cache: {tx}")
                    tf = None
            self.log.debug(f"undelegate for {tx['hash']} {tf=}")
        elif tx['type'] in [IDENA_TAG_KILL, IDENA_TAG_KILL_DELEGATOR]:
            pool = None
            if tx['type'] == IDENA_TAG_KILL:
                killed = tx['from']
            else:
                killed = tx['to']
                pool = tx['from']
                tx['to'] = None
            self.update_identities.add(killed)
            ident = self.db.get_identity(killed)
            # if identity is in cache, pull info out of it
            if ident is not None and time.time() - ident.get('_fetchTime', 0) < 3600:
                stake = ident.get('stake', '0')
                tx['amount'] = stake
                tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
                tf.meta['killedIdentity'] = killed
                tf.meta['age'] = ident.get('age', 0)
                tf.meta['pool'] = pool if pool else ident.get('delegatee')
                tf.meta['usd_value'] = float(stake) * self.db.prices['cg:idena']
                self.log.debug(f"Created kill TF from cache: {tf}")
            else:
                # otherwise fetch from indexer asynchronously
                tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
                tf.meta['killedIdentity'] = killed
                tf.meta['pool'] = pool
                asyncio.create_task(self.fetch_killtx(tx, tf), name='fetch_killtx')

            # Without this the identity would be sending coins on termination
            for ch_addr, change in tf.changes.items():
                tf.changes[ch_addr] = -1 * change
        elif tx['type'] in IDENA_COUNTABLE_TAGS:
            tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
        elif tx['type'] == IDENA_TAG_STAKE:
            meta = {}
            if tx['amount'] != '0':
                self.update_identities.add(tx['to'])
            receiver = tx['from']
            if tx['from'] == tx['to']:
                receiver = tx['to']
                tx['to'] = None  # locking coins isn't a self transfer
            tf = Transfer.from_idena_rpc(tx, blockNumber, logIndex, tags=[tx['type']])
            tf.meta.update(meta)
            tf.meta['cur_stake'] = str(Decimal(self.db.get_identity(receiver).get('stake', '0')) + tf.value())
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
                # tf.meta['payload'] = tx.get('payload')
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
                # cur_epoch = (await self.api_req('epoch/last', attempts=5))['epoch']
                cur_epoch = tx['epoch']
                birth_epoch = (await self.api_req(f'epoch/{cur_epoch - 1}/identity/{tf.meta["killedIdentity"]}', attempts=5))['birthEpoch']
                age = cur_epoch - birth_epoch
                api_tx = await self.api_req(f'transaction/{tf.hash}', attempts=50)
                if 'transfer' in api_tx.get('data', {}):
                    stake = Decimal(api_tx['data']['transfer'])
                else:
                    self.log.warning(f"No stake data for {tx['hash']=}")
                    stake = Decimal(0)
                tx['value'] = stake
                tf.changes = Transfer.create_changes(tx)
                tf.meta['age'] = age
                tf.meta['usd_value'] = float(stake) * self.db.prices['cg:idena']
                self.log.debug(f"Created kill TF: {tf}")
                self.slow_tfs.append(tf)
                break
            except:
                self.log.error(f"Failed to fetch killtx for hash={tf.hash}", exc_info=True)
                self.log.debug(f"Data for the failed fetch: {api_tx}")
                attempt += 1
                await asyncio.sleep(2)

    async def rpc_req(self, method=None, params = [], data=None, id=0, error_ok=False):
        if data is None:
            data = json.dumps({"method": method,"params": params,"id": id, "key": self.rpc_key})
        resp = await self.rpc_session.post(self.rpc_url, data=data)
        # self.log.debug(f"{await resp.text()=}")
        j = await resp.json()
        if error_ok:
            return j
        if 'result' not in j:
            self.log.error(f"Bad RPC response: {j}")
            if 'error' in j:
                raise Exception(f"RPC error: {j['error']}")
            else:
                raise Exception("RPC error")
        return j['result']

    async def api_req(self, path, attempts=1):
        self.log.debug(f"api_req {path=}")
        attempt = 0
        while attempt < attempts:
            try:
                resp = await self.rpc_session.get(urljoin(self.api_url, path))
                j = await resp.json()
                if 'result' not in j:
                    self.log.error(f"Bad API response: {j}")
                    raise Exception(f"API error")
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
                await asyncio.sleep(3)
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
                if tx['type'] in ['kill', 'killDelegator']:
                    try:
                        addr = tx['from'] if tx['type'] == 'kill' else tx['to']
                        ident = await self.fetch_identity(addr)
                        if ident['state'].lower() != 'undefined':
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
                for field in USELESS_IDENT_FIELDS:
                    try:
                        del ident[field]
                    except:
                        pass

            try:
                await self.process_identity_changes(changes=identities, full=True)
            except Exception as e:
                self.log.error(f'Identity change process error: "{e}"', exc_info=True)
                pass
            self.log.info("Identities cached")
            try:
                await asyncio.sleep(self.conf.identities_cache_interval)
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
        await session.close()

    async def fetch_identity(self, addr: str) -> dict:
        self.log.debug(f"Fetching identity {addr=}")
        ident = await self.rpc_req("dna_identity", [addr])
        ident['address'] = ident['address'].lower()
        ident['_fetchTime'] = int(time.time())
        for field in USELESS_IDENT_FIELDS:
            try:
                del ident[field]
            except:
                pass
        return ident

    async def update_identity(self, addr: str):
        ident = await self.fetch_identity(addr)
        await self.process_identity_changes([ident])

    async def process_identity_changes(self, changes: list[dict], full=False):
        evs = []
        for new_ident in changes:
            try:
                cur_ident = self.db.cache['identities'].get(new_ident['address'])
                if cur_ident is None:
                    continue
                new_club = self.check_new_club(Decimal(cur_ident['stake']), Decimal(new_ident['stake']))
                if new_club:
                    self.log.info(f"Club change: {new_ident['address']}, stake={new_ident['stake']}, {new_club=}")
                    evs.append(ClubEvent(addr=new_ident['address'], stake=Decimal(new_ident['stake']), club=new_club))
            except Exception as e:
                self.log.error(f"Error processing identity change for {new_ident=}: {e}", exc_info=True)

        self.log.debug(f"Inserting {len(changes)} identities")
        await self.db.insert_identities(changes, full=full)
        self.log.debug(f"Inserted")

        for ev in evs:
            ev.rank = self.db.count_identities_with_stake(ev.stake)
            self.event_chan.put_nowait(ev)
            self.log.debug(f"{ev=}")

    def check_new_club(self, cur_stake: Decimal, new_stake: Decimal) -> Decimal | None:
        "Returns new club if new stake is in a new club, else None."
        CLUBS = 50000
        if cur_stake > 500000:
            CLUBS = 100000
        if cur_stake > 2000000:
            CLUBS = 500000
        if cur_stake > 10000000:
            CLUBS = 1000000
        cur_club = cur_stake // CLUBS
        new_club = new_stake // CLUBS
        club_diff = new_club - cur_club
        if club_diff > 0:
            return new_club * CLUBS
        else:
            return None

    async def get_rewards_for_identity(self, addr):
        pow = 0.9
        ad = self.apy_data
        # if datetime.now(tz=timezone.utc) - ad['updated'] > timedelta(hours=2):
        await self.fetch_apy_data()

        ident = self.db.cache['identities'][addr]
        stake = float(ident['stake'])
        stake_weight = stake ** pow
        average_miner_weight = ad['average_miner_weight']
        online_miners_count = ad['online_miners']
        epoch_days = ad['epoch_days']
        self.log.debug(f"{stake=}, {stake_weight=}, {average_miner_weight=}, {online_miners_count=}, {epoch_days=}")

        proposer_only_reward = (6 * stake_weight * 20) / (stake_weight * 20 + average_miner_weight * 100)
        committee_only_reward = (6 * stake_weight) / (stake_weight + average_miner_weight * 119)
        proposer_and_committee_reward = (6 * stake_weight * 21) / (stake_weight * 21 + average_miner_weight * 99)
        proposer_probability = 1 / online_miners_count
        committee_probability = min(100, online_miners_count) / online_miners_count
        proposer_only_probability = proposer_probability * (1 - committee_probability)
        committee_only_probability = committee_probability * (1 - proposer_probability)
        proposer_and_committee_probability = proposer_only_probability * committee_only_probability
        mining_rewards = ((85000 * epoch_days) / 21.0) * (proposer_only_probability * proposer_only_reward + committee_only_probability * committee_only_reward + proposer_and_committee_probability * proposer_and_committee_reward)
        validation_rewards = (stake_weight / (stake_weight + ad['total_weight'])) * float(ad['epoch_rewards']['staking'])
        self.log.debug(f"{mining_rewards=}, {validation_rewards=}")
        rewards = mining_rewards + validation_rewards
        apy = rewards * 100 / stake * 366 / epoch_days
        return rewards, apy

    async def fetch_apy_data(self):
        ad = self.apy_data
        # These won't exactly match the true values from the node/indexer, but
        # it's a lot better to not have to reach out to API for them.
        ad['online_miners'] = self.get_online_miners()
        weight, average = self.get_staking_weights()
        # ad['online_miners'] = await self.api_req('onlineminers/count')
        # ad['staking'] = await self.api_req('staking')
        ad['total_weight'] = weight
        ad['average_miner_weight'] = average
        cur_epoch = await self.rpc_req('dna_epoch')
        if 'cur_epoch' not in ad or cur_epoch['epoch'] != ad['cur_epoch']['epoch']:
            ad['cur_epoch'] = cur_epoch
            if cur_epoch['nextValidation'].endswith('Z'):
                ad['cur_epoch']['validationTime'] = datetime.strptime(cur_epoch['nextValidation'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
            else:
                ad['cur_epoch']['validationTime'] = datetime.fromisoformat(cur_epoch['nextValidation'])
            ad['prev_epoch'] = await self.api_req(f'epoch/{ad["cur_epoch"]["epoch"] - 1}')
            ad['prev_epoch']['validationTime'] = datetime.strptime(ad['prev_epoch']['validationTime'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
            ad['epoch_rewards'] = await self.api_req(f'epoch/{ad["cur_epoch"]["epoch"] - 1}/rewardssummary')
            ad['epoch_days'] = (ad['cur_epoch']['validationTime'] - ad['prev_epoch']['validationTime']).days
        ad['updated'] = datetime.now(tz=timezone.utc)
        self.log.debug(f"Updated APY data: {ad=}")

    def get_online_miners(self) -> int:
        solo = 0
        pools = defaultdict(lambda: {'delegators': 0, 'online': False})
        for ident in self.db.cache['identities'].values():
            if ident['online']:
                if not ident['isPool']:
                    solo += 1
                else:
                    pools[ident['address']]['online'] = True
                    if ident['state'] in MINING_STATES:
                        solo += 1
            elif ident['delegatee'] and ident['state'] in MINING_STATES:
                pools[ident['delegatee']]['delegators'] += 1

        online = solo + sum([p['delegators'] for p in pools.values() if p['online']])
        return online

    def get_staking_weights(self):
        total_weight = 0
        miner_weight = 0
        miner_weights = []
        for ident in self.db.cache['identities'].values():
            if ident['state'] in MINING_STATES:
                weight = float(ident['stake']) ** 0.9
                total_weight += weight
                if ident['online'] or self.db.cache['identities'].get(ident.get('delegatee'), {}).get('online', False):
                    miner_weight += float(ident['stake']) ** 0.9
                    miner_weights.append(float(ident['stake']) ** 0.9)

        av1 = miner_weight / len(miner_weights)
        miner_weights.sort()
        av2 = calculate_average(miner_weights, 101)
        average = (av1 + av2) / 2
        self.log.debug(f"{average=} {miner_weight=} {total_weight=}")
        return total_weight, average

    async def _fetch_txs(self, tx_hashes: list[str]) -> list[Transfer]:
        tfs = []
        blocks = {}
        for hash in tx_hashes:
            tx = await self.rpc_req("bcn_transaction", [hash])
            if tx['blockHash'] not in blocks:
                blocks[tx['blockHash']] = await self.rpc_req("bcn_block", [tx['blockHash']])
            block = blocks[tx['blockHash']]
            blockNumber = block['height']
            index = block['transactions'].index(hash)
            tfs.append(await self.process_tx(tx, blockNumber=blockNumber, logIndex=index))
        return tfs

    async def build_bna_airdrop_tx(self, address: str) -> str | None:
        resp = await self.rpc_req('contract_readMap', [BNA_CONTRACT_ADDRESS, "c:", address, "bigint"], error_ok=True)
        if 'result' in resp:
            if Decimal(resp['result']) > Decimal(0):
                return 'already claimed!'
        elif resp.get('error', {}).get('message') == 'data is nil':
            pass
        else:
            raise Exception(f"Unexpected response: {resp}")

        eligible = ['Newbie', 'Verified', 'Human', 'Suspended', 'Zombie']
        if address not in self.db.cache['identities'] or self.db.get_identity(address)['state'] not in eligible:
            return 'not a valid identity!'

        tx = ProtoTransaction()
        tx.data.nonce = (await self.rpc_req('dna_getBalance', [address]))['mempoolNonce'] + 1
        tx.data.epoch = (await self.rpc_req('dna_epoch'))['epoch']
        tx.data.type = 16
        tx.data.to = addr_to_bytes(BNA_CONTRACT_ADDRESS)
        tx.data.maxFee = to_andy_bytes(Decimal(1))
        payload = ProtoCallContractAttachment()
        payload.method = "claimAirdrop"
        tx.data.payload = payload.SerializeToString()
        return tx.SerializeToString().hex()

    async def build_bna_send_tx(self, from_address: str, to_address: str, amount: Decimal) -> str:
        amount = to_andy_bytes(amount)
        tx = ProtoTransaction()
        tx.data.nonce = (await self.rpc_req('dna_getBalance', [from_address]))['mempoolNonce'] + 1
        tx.data.epoch = (await self.rpc_req('dna_epoch'))['epoch']
        tx.data.type = 16
        tx.data.to = addr_to_bytes(BNA_CONTRACT_ADDRESS)
        tx.data.maxFee = to_andy_bytes(Decimal(1))
        payload = ProtoCallContractAttachment()
        payload.method = "transfer"
        payload.args.extend([addr_to_bytes(to_address), amount])
        tx.data.payload = payload.SerializeToString()
        return tx.SerializeToString().hex(), from_andy_bytes(amount)

    async def get_bna_balance(self, address: str) -> Decimal:
        resp = await self.rpc_req('contract_readMap', [BNA_CONTRACT_ADDRESS, "b:", address, "bigint"], error_ok=True)
        if 'result' in resp:
            return Decimal(resp['result']) / Decimal(10**18)
        elif resp.get('error', {}).get('message') == 'data is nil':
            return Decimal('0')
        else:
            raise Exception(f"Unexpected response: {resp}")

    async def get_bna_supply(self) -> Decimal:
        resp = await self.rpc_req('contract_readData', [BNA_CONTRACT_ADDRESS, "STATE", "string"])
        state = json.loads(resp)
        return Decimal(state['totalSupply']) / Decimal(10**18)

    async def get_next_validation_time(self) -> int:
        resp = await self.rpc_req("dna_epoch")
        time = datetime.strptime(resp['nextValidation'], "%Y-%m-%dT%H:%M:%S%z")
        return int(time.timestamp())

def calculate_average(values: list[float], n: float):
    if len(values) == 0:
        return 0

    step = len(values) / float(n)
    total, cnt = 0.0, 0
    for i in range(n):
        index = int(round(step * i))
        if index >= len(values):
            index = len(values) - 1
        total += values[index]
        cnt += 1

    return total / cnt

def addr_to_bytes(addr: str) -> bytes:
    return bytes.fromhex(addr[2:])

def to_andy_bytes(amount: Decimal) -> bytes:
    return int(amount * (10**18)).to_bytes(16, 'big').lstrip(b'\x00')

def from_andy_bytes(value: bytes) -> Decimal:
    return Decimal(int.from_bytes(value, 'big')) / (10**18)
