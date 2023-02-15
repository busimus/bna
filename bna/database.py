from decimal import Decimal
import os
import json
import time
from dacite import from_dict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from bna.config import Config
from bna.pg_store import PgStore
from bna.transfer import Transfer
from bna.event import event_from_dict
from bna.cex_listeners import Trade

INTERESTING_ADDRESS_TYPES = ['premine', 'foundation']

class Database:
    def __init__(self, log, conf_path):
        self.log = log.getChild("DB")
        self.cache = {'transfers': {}, 'trades': {}, 'identities': {}}
        self.oldest_cached_tf = datetime.max
        self.oldest_cached_tr = datetime.max
        self.oldest_cached_tf = self.oldest_cached_tf.replace(tzinfo=timezone.utc)
        self.oldest_cached_tr = self.oldest_cached_tr.replace(tzinfo=timezone.utc)
        self.cache_cleaned_at = datetime.now(tz=timezone.utc)
        self.test_cache = True
        self.conf_path = conf_path
        self.conf: Config = self.get_config()
        self.store = PgStore(os.environ['POSTGRES_CONNSTRING'], self.log)
        self._load_known_addresses()
        # This is updated by the price oracle almost immediately.
        # @TODO: This probably shouldn't be in this class.
        self.prices = {'cg:bitcoin': 22000, 'cg:idena': 0.035, 'cg:binancecoin': 300, 'cg:binance-usd': 1, 'cg:tether': 1,
                       'ds:0x331ad6a9372d09d6ecb19399cda6634755c4460b': 0.0013}

    def get_config(self) -> Config:
        if os.path.exists(self.conf_path):
            conf_dict = json.load(open(self.conf_path, 'r'))
        else:
            self.log.warning(f'Config doesn\'t exist: "{self.conf_path}"')
            conf_dict = {}
            open(self.conf_path, 'w').write('{}')
        conf = from_dict(data_class=Config, data=conf_dict)
        self.log.debug(f"Config loaded: {conf}")
        # json.dump(asdict(conf), open(self.conf_path, 'w'))  # write defaults for unset params
        return conf

    def save_config(self, conf=None):
        "Saves the main conf object to disk, or the given conf object"
        if conf is None:
            conf = self.conf
        self.log.debug(f"Saving config: {conf}")
        conf_dict = asdict(conf)
        json.dump(conf_dict, open(self.conf_path, 'w'), indent=4)

    async def connect(self, drop_existing=False):
        self.log.debug("Connecting to the backing store")
        await self.store.connect(drop_existing)
        self.log.debug("Prefetching identities")
        self.cache['identities'] = await self.store.get_identities()

    async def recent_transfers(self, period: int) -> list[Transfer]:
        after = datetime.now(tz=timezone.utc) - timedelta(seconds=period)
        if after < self.oldest_cached_tf:# or self.disable_transfer_cache:
            return await self.fetch_transfers(after)

        cached_tfs = []
        for tf in self.cache['transfers'].values():
            if tf.timeStamp > after:
                cached_tfs.append(tf)
        self.clean_cache()
        return cached_tfs

    async def recent_trades(self, start: datetime, period: int) -> list[Trade]:
        now = datetime.now(tz=timezone.utc)
        period = timedelta(seconds=period)
        if now - start > period:
            start = now - period
        if start < self.oldest_cached_tr: # or self.disable_transfer_cache:
            return await self.fetch_trades(start)

        cached_trs: list[Trade] = []
        for k, tr in self.cache['trades'].items():
            if tr.timeStamp > start:
                cached_trs.append(tr)
        self.clean_cache()
        return cached_trs

    async def fetch_transfers(self, after: datetime) -> list[Trade]:
        self.log.debug(f'Fetching transfers since {after=}')
        tfs = await self.store.get_transfers(after)
        if after.timestamp() < time.time() - (self.conf.db.cached_record_age_limit * 8):# or self.disable_transfer_cache:
            self.log.debug(f'Records too old to store')
            pass
        else:
            self.cache['transfers'].update(tfs)
            self.oldest_cached_tf = after
        return list(tfs.values())

    async def fetch_trades(self, start: datetime) -> list[Trade]:
        self.log.debug(f'Fetching trades since {start=}')
        trs = await self.store.get_trades(start)
        if start.timestamp() < time.time() - (self.conf.db.cached_record_age_limit * 8):
            pass
        else:
            self.cache['trades'].update(trs)
            self.oldest_cached_tr = start
        return list(trs.values())

    async def insert_transfers(self, tfs: list[Transfer]):
        for tf in tfs:
            if (tf.blockNumber, tf.logIndex) in self.cache['transfers']:
                self.log.warning(f'Block number and index collision on hash={tf.hash}')
            # if not self.disable_transfer_cache:
            self.cache['transfers'][(tf.blockNumber, tf.logIndex)] = tf
        await self.store.insert_transfers(tfs)

    async def insert_trades(self, trs: list[Trade]):
        for tr in trs:
            if (tr.id, tr.market) in self.cache['trades']:
                pass
            # if not self.disable_transfer_cache:
            self.cache['trades'][(tr.id, tr.market)] = tr
        await self.store.insert_trades(trs)

    async def insert_identity(self, ident: dict):
        self.cache['identities'][ident['address'].lower()] = ident
        await self.store.insert_identities([ident])

    async def insert_identities(self, idents: list[dict], full=False):
        if not full:
            self.cache['identities'].update(dict(map(lambda ident: (ident['address'].lower(), ident), idents)))
        else:
            del self.cache['identities']
            self.cache['identities'] = dict(map(lambda ident: (ident['address'].lower(), ident), idents))
        await self.store.insert_identities(idents, full=full)

    async def insert_event(self, msg, ev):
        await self.store.insert_event(chan_id=msg.channel.id, msg_id=msg.id, ev_dict=ev.to_dict())

    def get_identity(self, addr: str) -> dict:
        return self.cache['identities'].get(addr.lower())

    def get_identities(self) -> list[dict]:
        return self.cache['identities']

    async def get_transfer(self, tx_hash: str) -> Transfer:
        return (await self.store.get_transfers_by_hash([tx_hash]))[0]

    async def get_transfers(self, tx_hashes: list[str]) -> list[Transfer]:
        return await self.store.get_transfers_by_hash(tx_hashes)

    async def get_event(self, ev_id: int):
        self.log.debug(f"Getting event {ev_id=}")
        chan_id, msg_id, ev_dict = await self.store.get_event(ev_id)
        if not ev_dict:
            return None, None, None
        try:
            ev = await event_from_dict(ev_dict, db=self)
        except Exception as e:
            self.log.error(f'Error parsing event {ev_dict=}: {e}', exc_info=True)
            return None, None, None
        return chan_id, msg_id, ev

    async def get_last_block(self, chain: str) -> int:
        return await self.store.get_latest_block(chain)

    async def close(self):
        self.log.info("Closing DB connections...")
        self.save_config()
        await self.store.close()
        self.log.info("DB connections stopped")

    def address_is_interesting(self, address, chain=None):
        "Returns info about `address` if it's interesting, otherwise returns None"
        info = self.known.get(address, {'type': None})
        if info['type'] in INTERESTING_ADDRESS_TYPES and ('chain' not in info or info.get('chain') == chain):
            return info
        else:
            return None

    def addr_type(self, address):
        return self.known.get(address.lower(), {}).get('type', None)

    def addrs_of_type(self, type: str, full=False) -> list[str] | list[dict]:
        return [kv if full else kv[0] for kv in self.known.items() if kv[1]['type'] == type]

    def ignored_addrs(self) -> list[str]:
        return [kv[0] for kv in self.known.items() if kv[1].get('ignored_sender')]

    def count_alive_identities(self):
        states = {'Newbie', 'Verified', 'Human', 'Suspended', 'Zombie'}
        return sum(1 for _ in filter(lambda i: i.get('state') in states, self.cache['identities'].values()))

    def count_identities_with_stake(self, stake: Decimal):
        return sum(1 for _ in filter(lambda i: Decimal(i.get('stake', '0')) >= stake, self.cache['identities'].values()))

    def count_identities_with_age(self, age: int):
        return sum(1 for _ in filter(lambda i: i.get('age', 0) >= age, self.cache['identities'].values()))

    async def _remove_transfer(self, tf: Transfer):
        "For testing only"
        await self.store._remove_transfer(tf)
        if (tf.blockNumber, tf.logIndex) in self.cache['transfers']:
            del self.cache['transfers'][(tf.blockNumber, tf.logIndex)]

    async def _remove_trade(self, tr: Trade):
        "For testing only"
        await self.store._remove_trade(tr)
        if (tr.id, tr.market) in self.cache['trades']:
            del self.cache['trades'][(tr.id, tr.market)]

    def clean_cache(self):
        now = datetime.now(tz=timezone.utc)
        record_age_limit = timedelta(seconds=self.conf.db.cached_record_age_limit * 8)
        if now - timedelta(seconds=60) < self.cache_cleaned_at:
            return

        self.log.debug(f"Cleaning cache, sizes before: tf={len(self.cache['transfers'])}\ttr={len(self.cache['trades'])}")
        new_cache = {'transfers': {}, 'trades': {}, 'identities': self.cache['identities']}
        for k, tf in self.cache['transfers'].items():
            if now - tf.timeStamp < record_age_limit:
                new_cache['transfers'][k] = tf
        for k, tr in self.cache['trades'].items():
            if now - tr.timeStamp < record_age_limit:
                new_cache['trades'][k] = tr
        self.cache = new_cache
        self.cache_cleaned_at = now
        self.log.debug(f"Cleaned cache, sizes after:   tf={len(self.cache['transfers'])}\ttr={len(self.cache['trades'])}")

    def _load_known_addresses(self):
        try:
            self.known = json.load(open("known_addresses.json", "r"))
            k = {}
            for addr, info in self.known.items():
                k[addr.lower()] = info
            self.known = k
        except Exception as e:
            self.log.warning(f'Couldn\'t load "known_addresses.json": {e}')
            self.known = {}
        self.known_by_type = defaultdict(dict)
        for addr, info in self.known.items():
            self.known_by_type[info['type']][addr] = info
