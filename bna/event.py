import time
from copy import deepcopy
from decimal import Decimal
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field
from disnake import Color

from bna.transfer import Transfer
from bna.utils import aggregate_dex_trades, trade_color
from bna.tags import IDENA_TAG_KILL

@dataclass(kw_only=True)
class Event:
    id: int = -1
    _color: Color = Color.from_rgb(129, 190, 238)

    def to_dict(self) -> dict:
        return {'type': 'event', 'id': self.id}

    @classmethod
    def from_dict(cls, d: dict):
        return Event(id=d['id'])

    def __init__(self, id=None):
        if id:
            self.id = id
        else:
            self.id = int(time.time() * 1000000000)  # that's enough zeroes, right?

@dataclass(kw_only=True)
class TransferEvent(Event):
    "User-facing event for regular transfers"
    time: datetime = datetime.min
    amount: Decimal = Decimal(-1)
    by: str = ''
    tfs: list[Transfer] = field(default_factory=list)
    # If True then `by` represents the receiver address
    _recv: bool = False

    def total_usd_value(self) -> float:
        return sum(tf.meta.get('usd_value', 0) for tf in self.tfs)

    def join(self, event):
        receivers = set([tf.to(single=True) for tf in self.tfs])
        senders = set([tf.from_(single=True) for tf in self.tfs])
        ev_senders = set([tf.from_(single=True) for tf in event.tfs])
        if senders != ev_senders:
            self.by = list(receivers)[0]  # but what if there's more than one? i guess we'll never know
            self._recv = True
        self.tfs.extend(event.tfs)
        self.time = event.time
        self.amount += event.amount

    def can_join(self, event):
        if type(event) != TransferEvent:
            return False
        if len(self.tfs) > 1 and self.by == event.by and not self._recv:
            return True
        else:
            receivers = set([tf.to(single=True) for tf in self.tfs])
            ev_receivers = set([tf.to(single=True) for tf in event.tfs])
            if receivers == ev_receivers and len(list(receivers)) > 0 and list(receivers)[0]:
                return True
        return False

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'transfer'
        d['by'] = self.by
        d['time'] = self.time.isoformat()
        d['tfs'] = [tf.hash for tf in self.tfs]
        d['amount'] = str(self.amount)
        return d

    @classmethod
    async def from_dict(cls, d: dict, db):
        te = TransferEvent()
        te.__dict__.update(super().from_dict(d).__dict__)  # I LOVE OOP
        te.amount = Decimal(d['amount'])
        te.time = datetime.fromisoformat(d['time'])
        te.by = d['by']
        te.tfs = await db.get_transfers(d['tfs'])
        if any([tf is None for tf in te.tfs]):
            raise Exception(f"Could not find all transfers for event: {d=}")
        return te

    def __len__(self):
        return len(self.tfs)

    def __post_init__(self):
        super().__init__()

@dataclass(kw_only=True)
class ChainTransferEvent(Event):
    "Internal event that's created by chain listeners"
    chain: str
    tfs: list[Transfer]

    def __post_init__(self):
        super().__init__()

@dataclass(kw_only=True)
class InterestingTransferEvent(TransferEvent):
    pass

@dataclass(kw_only=True)
class DexEvent(TransferEvent):
    # For multi DEX events
    buy_usd: float = 0
    sell_usd: float = 0
    lp_usd: float = 0
    avg_price: float = 0
    last_price: float = 0
    _color: any = None

    def join(self, event, known_addr: dict):
        self.tfs.extend(event.tfs)
        self.time = event.time
        self.amount = sum([tf.value() for tf in self.tfs])
        for tf in reversed(self.tfs):
            if tf.meta.get('usd_price'):
                self.last_price = tf.meta['usd_price']
                break
        aggr = aggregate_dex_trades(self.tfs, known_addr)
        self.buy_usd = aggr['buy_usd']
        self.sell_usd = aggr['sell_usd']
        self.lp_usd = aggr['lp_usd']
        self.avg_price = aggr['avg_price']

    def can_join(self, event):
        if type(event) != DexEvent:
            return False
        if len(self.tfs) > 1 and len(event.tfs) > 1:
            return True
        return False

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'dex'
        d['by'] = ''
        d['buy_usd'] = self.buy_usd
        d['sell_usd'] = self.sell_usd
        d['lp_usd'] = self.lp_usd
        d['avg_price'] = self.avg_price
        d['last_price'] = self.last_price
        if self._color:
            d['_color'] = self._color.to_rgb()
        return d

    @classmethod
    async def from_dict(cls, d: dict, db):
        de = DexEvent()
        de.__dict__.update((await super().from_dict(d, db)).__dict__)
        de.buy_usd = d['buy_usd']
        de.sell_usd = d['sell_usd']
        de.avg_price = d['avg_price']
        if 'lp_usd' in d:
            de.lp_usd = d['lp_usd']
        if 'last_price' in d:
            de.last_price = d['last_price']
        de._color = Color.from_rgb(*d['_color'])
        return de

    @classmethod
    def from_tfs(cls, tfs: list[Transfer], known_addr: dict):
        de = DexEvent()
        de.tfs = tfs
        de.time = max([tf.timeStamp for tf in tfs])
        de.amount = sum([tf.value() for tf in tfs])
        aggr = aggregate_dex_trades(de.tfs, known_addr)
        de.buy_usd = aggr['buy_usd']
        de.sell_usd = aggr['sell_usd']
        de.lp_usd = aggr['lp_usd']
        de.avg_price = aggr['avg_price']
        for tf in reversed(de.tfs):
            if tf.meta.get('usd_price'):
                de.last_price = tf.meta['usd_price']
                break
        de._color = trade_color(de.buy_usd, de.sell_usd)
        return de

@dataclass(kw_only=True)
class KillEvent(TransferEvent):
    killed: str = ''
    stake: Decimal = Decimal(0)
    age: int = ''
    pool: str | None = None

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'kill'
        d['killed'] = self.killed
        d['stake'] = str(self.stake)
        d['age'] = self.age
        d['pool'] = self.pool
        return d

    @classmethod
    def from_tf(cls, tf: Transfer):
        ke = KillEvent()
        ke.tfs = [tf]
        ke.time = tf.timeStamp
        ke.by = tf.signer
        ke.killed = tf.meta['killedIdentity']
        ke.stake = tf.value(recv=True)
        ke.amount = ke.stake
        ke.age = tf.meta['age']
        ke.pool = tf.meta['pool'] if 'pool' in tf.meta else None
        return ke

    @classmethod
    async def from_dict(cls, d: dict, db):
        ke = KillEvent()
        ke.__dict__.update((await super().from_dict(d, db)).__dict__)
        ke.killed = d['killed']
        ke.stake = Decimal(d['stake'])
        ke.age = d['age']
        ke.pool = d['pool']
        return ke

@dataclass(kw_only=True)
class PoolEvent(TransferEvent):
    subtype: str = ''
    addr: str = ''  # needed because the killed identity is not always signer
    stake: Decimal = Decimal(-1)
    age: int = -1
    pool: str = ''
    _notified: bool = False

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'pool'
        d['subtype'] = self.subtype
        d['addr'] = self.addr
        d['stake'] = str(self.stake)
        d['age'] = self.age
        d['pool'] = self.pool
        return d

    @classmethod
    def from_tf(cls, tf: Transfer, subtype: str, age: int = None, stake: Decimal = None):
        pe = PoolEvent()
        pe.tfs = [tf]
        pe.time = tf.timeStamp
        if subtype == IDENA_TAG_KILL:
            pe.addr = tf.meta['killedIdentity']
        else:
            pe.addr = tf.signer
        pe.by = tf.signer
        pe.subtype = subtype
        pe.stake = stake if stake else tf.value(True)
        pe.amount = pe.stake
        pe.age = age if age else tf.meta['age']
        pe.pool = tf.meta['pool']
        return pe

    @classmethod
    async def from_dict(cls, d: dict, db):
        pe = PoolEvent()
        pe.__dict__.update((await super().from_dict(d, db)).__dict__)
        pe.subtype = d['subtype']
        pe.addr = d['addr']
        pe.stake = Decimal(d['stake'])
        pe.age = d['age']
        pe.pool = d['pool']
        return pe

@dataclass(kw_only=True)
class MassPoolEvent(Event):
    subtype: str = ''
    pool: str = ''
    count: int = -1
    stake: Decimal = -1
    age: int = -1
    changes: list[PoolEvent] = field(default_factory=list)

    def join(self, event):
        if type(event) == MassPoolEvent:
            self.changes.extend(event.changes)
        elif type(event) == PoolEvent:
            self.changes.append(event)
        self.count = len(self.changes)
        self.stake = sum([ch.stake for ch in self.changes])
        self.age = sum([ch.age for ch in self.changes])

    def can_join(self, new_ev):
        if type(new_ev) not in [MassPoolEvent, PoolEvent]:
            return False
        if self.pool != new_ev.pool:
            return False
        if self.subtype != new_ev.subtype:
            return False
        return True

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'mass_pool'
        d['subtype'] = self.subtype
        d['pool'] = self.pool
        d['count'] = self.count
        d['stake'] = str(self.stake)
        d['age'] = self.age
        d['changes'] = [c.to_dict() for c in self.changes]
        return d

    @classmethod
    async def from_dict(cls, d: dict, db):
        pe = MassPoolEvent()
        pe.__dict__.update(super().from_dict(d).__dict__)
        pe.subtype = d['subtype']
        pe.pool = d['pool']
        pe.count = d['count']
        pe.age = d['age']
        pe.stake = Decimal(d['stake'])
        pe.changes = [(await PoolEvent.from_dict(c, db)) for c in d['changes']]
        return pe

    def __len__(self):
        return len(self.changes)

    def __post_init__(self):
        super().__init__()

@dataclass(kw_only=True)
class MarketStats:
    quote_currency: str = ''
    buy: Decimal = Decimal(0)
    sell: Decimal = Decimal(0)
    buy_usd: float = 0
    sell_usd: float = 0
    quote_amount: Decimal = Decimal(0)
    avg_price: str = ''  # human readable price with currency, only generated for stats
    avg_price_usd: float = 0

    def calculate_average_price(self, prices: dict[str, float]):
        if self.quote_amount == 0:
            return
        avg_price = self.quote_amount / self.volume_idna()
        quote_price = prices[self.quote_currency]
        if self.quote_currency == 'cg:bitcoin':
            self.avg_price_usd = float(avg_price * quote_price)
            avg_price *= Decimal("1e8")
            self.avg_price = f'{avg_price:,.1f} sats'
        elif self.quote_currency in ['cg:tether', 'cg:binance-usd', 'usd']:
            self.avg_price = f"${avg_price:,.3f}"
            self.avg_price_usd = float(avg_price)

    def volume_idna(self) -> Decimal:
        return self.buy + self.sell

    def volume_usd(self) -> float:
        return self.buy_usd + self.sell_usd

    def to_dict(self) -> dict:
        return {
            'quote_currency': self.quote_currency,
            'buy': str(self.buy),
            'sell': str(self.sell),
            'buy_usd': self.buy_usd,
            'sell_usd': self.sell_usd,
            'quote_amount': str(self.quote_amount),
            'avg_price': self.avg_price,
            'avg_price_usd': self.avg_price_usd,
        }

    @classmethod
    def from_dict(cls, d: dict):
        ms = MarketStats()
        ms.quote_currency = d['quote_currency']
        ms.buy = Decimal(d['buy'])
        ms.sell = Decimal(d['sell'])
        ms.buy_usd = d['buy_usd']
        ms.sell_usd = d['sell_usd']
        ms.quote_amount = Decimal(d['quote_amount'])
        ms.avg_price = d['avg_price']
        ms.avg_price_usd = d['avg_price_usd']
        return ms

@dataclass(kw_only=True)
class CexEvent(Event):
    total_buy_val: float
    total_sell_val: float
    markets: dict[str, MarketStats] = field(default_factory=dict)

    def join(self, event, prices):
        self.total_buy_val += event.total_buy_val
        self.total_sell_val += event.total_sell_val
        for market, stats in event.markets.items():
            if market not in self.markets:
                self.markets[market] = MarketStats()
            self.markets[market].buy += stats.buy
            self.markets[market].sell += stats.sell
            self.markets[market].quote_amount += stats.quote_amount
            self.markets[market].buy_usd += stats.buy_usd
            self.markets[market].sell_usd += stats.sell_usd
            self.markets[market].calculate_average_price(prices)

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'cex'
        d['total_buy_val'] = self.total_buy_val
        d['total_sell_val'] = self.total_sell_val
        d['markets'] = {k: v.to_dict() for k, v in self.markets.items()}
        return d

    @classmethod
    def from_dict(cls, d):
        ce = CexEvent()
        ce.__dict__.update(super().from_dict(d).__dict__)
        ce.total_buy_val = d['total_buy_val']
        ce.total_sell_val = d['total_sell_val']
        m = {k: MarketStats.from_dict(v) for k, v in d['markets'].items()}
        ce.markets = dict(sorted(m.items(), key=lambda m: m[1].volume_idna(), reverse=True))
        return ce

    def __post_init__(self):
        super().__init__()

@dataclass(kw_only=True)
class ClubEvent(Event):
    addr: str
    stake: Decimal
    club: Decimal
    rank: int = 0

    @property
    def club_str(self) -> str:
        club = float(f'{self.club:.3g}')
        magnitude = 0
        while abs(club) >= 1000:
            magnitude += 1
            club /= 1000.0
        club_str = str(club).rstrip('0').rstrip('.')
        return f"{club_str}{['', 'K', 'M', 'B', 'T'][magnitude]}"

    def __post_init__(self):
        super().__init__()

@dataclass(kw_only=True)
class BlockEvent(Event):
    chain: str
    height: int

    def __post_init__(self):
        super().__init__()

@dataclass(kw_only=True)
class StatsEvent(Event):
    period: int = -1  # in seconds
    invites_issued: int = 0
    invites_activated: int = 0
    identities_killed: int = 0
    contract_calls: int = 0
    bridged_to_bsc: int = 0
    bridged_from_bsc: int = 0
    staked: int = 0
    unstaked: int = 0
    burned: int = 0
    markets: dict[str, MarketStats] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'stats'
        d['period'] = self.period
        d['invites_issued'] = self.invites_issued
        d['invites_activated'] = self.invites_activated
        d['identities_killed'] = self.identities_killed
        d['contract_calls'] = self.contract_calls
        d['bridged_to_bsc'] = self.bridged_to_bsc
        d['bridged_from_bsc'] = self.bridged_from_bsc
        d['staked'] = self.staked
        d['unstaked'] = self.unstaked
        d['burned'] = self.burned
        d['markets'] = {k: v.to_dict() for k, v in self.markets.items()}
        return d

    @classmethod
    def from_dict(cls, d: dict):
        se = StatsEvent()
        se.__dict__.update(super().from_dict(d).__dict__)
        se.period = d['period']
        se.invites_issued = d['invites_issued']
        se.invites_activated = d['invites_activated']
        se.identities_killed = d['identities_killed']
        se.contract_calls = d['contract_calls']
        se.bridged_to_bsc = d['bridged_to_bsc']
        se.bridged_from_bsc = d['bridged_from_bsc']
        se.staked = d['staked']
        se.unstaked = d['unstaked']
        se.burned = d['burned']
        m = {k: MarketStats.from_dict(v) for k, v in d['markets'].items()}
        se.markets = dict(sorted(m.items(), key=lambda m: m[1].volume_idna(), reverse=True))
        return se

    def __post_init__(self):
        super().__init__()

@dataclass(kw_only=True)
class TopEvent(Event):
    period: int = -1  # in seconds
    _tfs: list[Transfer] = field(default_factory=list)
    total_usd_value: float = 0
    _long: bool = False

    @property
    def items(self) -> list[Transfer]:
        return self._tfs

    @items.setter
    def items(self, tfs) -> list[dict]:
        self._tfs = tfs

    def __len__(self):
        return len(self.items)

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'top'
        d['period'] = self.period
        d['total_usd_value'] = self.total_usd_value
        d['_tfs'] = [tf.hash for tf in self._tfs]
        d['_long'] = self._long
        return d

    @classmethod
    async def from_dict(cls, d: dict, db):
        te = TopEvent()
        te.__dict__.update(super().from_dict(d).__dict__)
        te.period = d['period']
        if '_tfs' in d:
            te._tfs = await db.get_transfers(d['_tfs'])
            if any([tf is None for tf in te._tfs]):
                raise Exception(f"Could not find all transfers for event: {d=}")
        te.total_usd_value = d['total_usd_value']
        if '_long' in d:
            te._long = d['_long']
        return te

    def __post_init__(self):
        super().__init__()

@dataclass(kw_only=True)
class TopDexEvent(TopEvent):
    buy_usd: float = 0
    sell_usd: float = 0
    lp_usd: float = 0

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'top_dex'
        d['buy_usd'] = self.buy_usd
        d['sell_usd'] = self.sell_usd
        d['lp_usd'] = self.lp_usd
        return d

    @classmethod
    async def from_dict(cls, d: dict, db):
        te = TopDexEvent()
        te.__dict__.update((await super().from_dict(d, db)).__dict__)
        te.buy_usd = d['buy_usd']
        te.sell_usd = d['sell_usd']
        te.lp_usd = d['lp_usd']
        return te

@dataclass(kw_only=True)
class TopKillEvent(TopEvent):
    total_idna: Decimal = Decimal(0)
    total_age: int = 0

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'top_kill'
        d['total_idna'] = str(self.total_idna)
        d['total_age'] = self.total_age
        return d

    @classmethod
    async def from_dict(cls, d: dict, db):
        te = TopKillEvent()
        te.__dict__.update((await super().from_dict(d, db)).__dict__)
        te.total_idna = Decimal(d['total_idna'])
        te.total_age = d['total_age']
        return te

@dataclass(kw_only=True)
class TopStakeEvent(TopEvent):
    total_idna: Decimal = Decimal(0)
    _identities: list[dict] = field(default_factory=list)

    @property
    def items(self) -> list[dict]:
        return self._identities

    @items.setter
    def items(self, identities) -> list[dict]:
        self._identities = identities

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'top_stake'
        d['total_idna'] = str(self.total_idna)
        d['_identities'] = deepcopy(self._identities)
        for i in d['_identities']:
            i['stake'] = str(i['stake'])
        return d

    @classmethod
    async def from_dict(cls, d: dict, db):
        te = TopStakeEvent()
        te.__dict__.update((await super().from_dict(d, db)).__dict__)
        te.total_idna = Decimal(d['total_idna'])
        te._identities = deepcopy(d['_identities'])
        for i in te._identities:
            i['stake'] = Decimal(i['stake'])
        return te

@dataclass(kw_only=True)
class PoolStatsEvent(Event):
    period: int  # in seconds
    killed: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    delegated: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    undelegated: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _long: bool = False

    def sort(self):
        self.killed = dict(sorted(self.killed.items(), key=lambda item: item[1], reverse=True))
        self.delegated = dict(sorted(self.delegated.items(), key=lambda item: item[1], reverse=True))
        self.undelegated = dict(sorted(self.undelegated.items(), key=lambda item: item[1], reverse=True))

    def __len__(self) -> int:
        return max(len(self.killed), len(self.delegated), len(self.undelegated))

    def to_dict(self) -> dict:
        d = super().to_dict()
        d['type'] = 'pool_stats'
        d['period'] = self.period
        d['killed'] = deepcopy(self.killed)
        d['delegated'] = deepcopy(self.delegated)
        d['undelegated'] = deepcopy(self.undelegated)
        d['_long'] = self._long
        return d

    @classmethod
    def from_dict(cls, d: dict):
        ps = PoolStatsEvent()
        ps.__dict__.update(super().from_dict(d).__dict__)
        ps.period = d['period']
        ps.killed = deepcopy(d['killed'])
        ps.delegated = deepcopy(d['delegated'])
        ps.undelegated = deepcopy(d['undelegated'])
        ps._long = d['_long']
        return ps

    def __post_init__(self):
        super().__init__()


async def event_from_dict(d: dict, db) -> Event:
    type_map = {
        'event': {'type': Event, 'need_db': False},
        'transfer': {'type': TransferEvent, 'need_db': True},
        'dex': {'type': DexEvent, 'need_db': True},
        'kill': {'type': KillEvent, 'need_db': True},
        'pool': {'type': PoolEvent, 'need_db': True},
        'mass_pool': {'type': MassPoolEvent, 'need_db': True},
        'cex': {'type': CexEvent, 'need_db': False},
        # 'club': {'type': ClubEvent, 'need_db': False},
        'stats': {'type': StatsEvent, 'need_db': False},
        'pool_stats': {'type': PoolStatsEvent, 'need_db': False},
        'top': {'type': TopEvent, 'need_db': True},
        'top_dex': {'type': TopDexEvent, 'need_db': True},
        'top_kill': {'type': TopKillEvent, 'need_db': True},
        'top_stake': {'type': TopStakeEvent, 'need_db': True},
    }
    ev_type_spec = type_map.get(d['type'])
    if not ev_type_spec:
        raise Exception(f"Unknown event type: {d['type']}")
    ev_type = ev_type_spec['type']

    if ev_type_spec['need_db']:
        ev = await ev_type.from_dict(d=d, db=db)
    else:
        ev = ev_type.from_dict(d=d)
    return ev
