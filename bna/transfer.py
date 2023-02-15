from copy import deepcopy
from decimal import Decimal
from datetime import datetime, timezone
from collections import defaultdict
from dataclasses import dataclass, asdict, field

from bna.tags import *
from bna.utils import any_in

WIDNA_DECIMALS = Decimal(10**18)
CHAIN_BSC = 'bsc'
CHAIN_IDENA = 'idena'


@dataclass
class BscLog:
    address: str
    topics: list[str]
    data: str
    blockNumber: int
    transactionHash: str
    transactionIndex: int
    blockHash: str
    logIndex: int
    removed: bool
    signer: str = "0x0000000000000000000000000000000000000000"

    # Dumb, but shorter than writing my own __init__ lmao
    def convert(self):
        self.blockNumber = int(self.blockNumber, base=16)
        self.transactionIndex = int(self.transactionIndex, base=16)
        self.logIndex = int(self.logIndex, base=16)

    def topic_as_addr(self, index):
        return "0x{:040x}".format(int(self.topics[index], base=16))

@dataclass
class Transfer:
    changes: dict[str, Decimal]
    hash: str
    blockNumber: int
    logIndex: int
    timeStamp: datetime | None
    chain: str
    signer: str = "0x0000000000000000000000000000000000000000"
    tags: list[str] = field(default_factory=list)
    meta: dict = field(default_factory=dict)

    def from_bsc(tx: dict):
        tx['value'] = Decimal(tx['value']) / WIDNA_DECIMALS
        tx['changes'] = Transfer.create_changes(tx)
        tx['chain'] = CHAIN_BSC
        tx = {k: tx[k] for k in tx if k in Transfer.__match_args__}
        tx['timeStamp'] = datetime.fromtimestamp(int(tx['timeStamp']), tz=timezone.utc)
        tx['blockNumber'] = int(tx['blockNumber'])
        return Transfer(**tx)

    def from_bsc_log(log: BscLog, timeStamp=None):
        log = asdict(log)
        log['value'] = Decimal(int(log['data'], base=16)) / WIDNA_DECIMALS
        log['from'] = "0x{:040x}".format(int(log['topics'][1], base=16))
        log['to'] = "0x{:040x}".format(int(log['topics'][2], base=16))
        log['changes'] = Transfer.create_changes(log)
        log['hash'] = log['transactionHash']
        log['chain'] = CHAIN_BSC
        log = {k: log[k] for k in log if k in Transfer.__match_args__}
        log['timeStamp'] = timeStamp
        if timeStamp:
            log['timeStamp'] = datetime.fromtimestamp(int(timeStamp), tz=timezone.utc)
        return Transfer(**log)

    def from_idena_rpc(tx: dict, blockNumber, logIndex, tags=[]):
        tx['value'] = Decimal(tx['amount'])
        tx['signer'] = tx['from']
        tx['changes'] = Transfer.create_changes(tx)
        tx['timeStamp'] = datetime.fromtimestamp(int(tx['timestamp']), tz=timezone.utc)
        tx['chain'] = CHAIN_IDENA
        tx = {k: tx[k] for k in tx if k in Transfer.__match_args__}
        tx['blockNumber'] = int(blockNumber)
        tx['logIndex'] = int(logIndex)
        tx['tags'] = tags
        return Transfer(**tx)

    def create_changes(tx):
        from_ = tx['from'].lower() if tx.get('from') else None
        to = tx['to'].lower() if tx.get('to') else None
        ch = {from_: Decimal(0), to: Decimal(0)}
        # this protects against self transfers
        ch[from_] -= tx['value']
        ch[to] += tx['value']
        if None in ch:
            del ch[None]
        return ch

    def has_no_effect(self):
        "Returns True if the end result of all changes is zero (balances stay the same)"
        balances = defaultdict(Decimal)
        for addr, amount in self.changes.items():
            balances[addr] += amount
        return all([change == 0 for change in balances.values()])

    def value(self, recv=False):
        if len(self.changes) == 0:
            return Decimal(0)
        else:
            if not recv:
                return abs(sum([ch for ch in self.changes.values() if ch < 0]))
            else:
                return abs(sum([ch for ch in self.changes.values() if ch > 0]))

    def from_(self, single=False) -> list[str] | str:
        "Returns a list of sending addresses or only the first sender if `single`"
        send = [ch[0] for ch in self.changes.items() if ch[1] < 0]
        if not single and len(send) > 0:
            return send
        else:
            return send[0] if len(send) > 0 else self.signer

    def to(self, single=False) -> list[str] | str | None:
        "Returns a list of receiving addresses or only the first receiver if `single`"
        recv = [ch[0] for ch in self.changes.items() if ch[1] > 0]
        if not single:
            return recv
        else:
            return recv[0] if len(recv) > 0 else None

    def should_store(self):
        if any_in(self.tags, IDENA_STATS_TAGS):
            return True
        if not self.has_no_effect():
            return True
        return False

    def from_dict(d: dict):
        d = deepcopy(d)
        d['timeStamp'] = datetime.fromtimestamp(int(d['timeStamp']), tz=timezone.utc)
        for k, v in d['changes'].items():
            d['changes'][k] = Decimal(v)
        if d.get('meta'):
            meta = d['meta']
            if meta.get('lp'):
                for pool_addr, pool in meta['lp'].items():
                    pool['idna'] = Decimal(pool['idna'])
                    pool['token'] = Decimal(pool['token'])
            if meta.get('token'):
                for k, v in meta['token'].items():
                    meta['token'][k] = Decimal(v)
            if meta.get('token'):
                for k, v in meta['token'].items():
                    meta['token'][k] = Decimal(v)
            if meta.get('lp_excess'):
                meta['lp_excess'] = Decimal(meta['lp_excess'])
        return Transfer(**d)

    def to_dict(self) -> dict:
        d = asdict(self)
        d['timeStamp'] = d['timeStamp'].timestamp()
        for token_addr, v in d['changes'].items():
            d['changes'][token_addr] = str(v)
        if d.get('meta'):
            meta = d['meta']
            if meta.get('lp'):
                for pool_addr, pool in meta['lp'].items():
                    pool['idna'] = str(pool['idna'])
                    pool['token'] = str(pool['token'])
            if meta.get('token'):
                for token_addr, v in meta['token'].items():
                    meta['token'][token_addr] = str(v)
            if meta.get('lp_excess'):
                meta['lp_excess'] = str(meta['lp_excess'])
        return d
