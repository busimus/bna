import asyncio
from copy import deepcopy
from decimal import Decimal
from logging import Logger
from collections import defaultdict, deque
from asyncio.queues import Queue as AsyncQueue
from datetime import datetime, timedelta, timezone

from bna.database import Database
from bna.config import TrackerConfig
from bna.transfer import Transfer
from bna.bsc_listener import NULL_ADDRESS
from bna.cex_listeners import MARKET_BSC, MARKETS
from bna.utils import any_in, aggregate_dex_trades
from bna.event import *
from bna.tags import *

class Tracker:
    def __init__(self, db: Database, conf: TrackerConfig, bot,  bsc: AsyncQueue, idna: AsyncQueue, trades: AsyncQueue, event_chan: AsyncQueue, log: Logger):
        self.db = db
        self.conf = conf
        self.bsc_chan = bsc
        self.idna_chan = idna
        self.trade_chan = trades
        self.log = log.getChild("TR")
        self.tracker_event_chan = event_chan
        # Tracks time of transfer notification for each address
        self.sents = defaultdict(lambda:{'sents': {}, 'recvs': {}})
        self.sents_notified = defaultdict(lambda: {'time': 0, 'kept': False})
        # Tracks time of non-transfer notifications for TX hashes
        self.hashes_notified = defaultdict(lambda: {'time': 0})
        # Stores PoolEvents per pool per identity before notifications for them can be created
        self.pool_events = {'kill': defaultdict(dict), 'delegate': defaultdict(dict), 'undelegate': defaultdict(dict)}
        # To prevent delegate/undelegate spam
        self.identity_pool_events = defaultdict(deque)
        # Tracks time when the last trade notification happened
        self.trades_notified_at = datetime.min.replace(tzinfo=timezone.utc)

    async def run(self):
        trade_task = asyncio.create_task(self.cex_trade_worker(), name="trade_worker")
        stats_task = asyncio.create_task(self.stats_worker(), name="stats_worker")
        while True:
            try:
                chans = [asyncio.create_task(self.idna_chan.get()),
                         asyncio.create_task(self.bsc_chan.get())]
                event_task, pending = await asyncio.wait(chans, return_when=asyncio.FIRST_COMPLETED)
                event: dict = list(event_task)[0].result()
                for f in pending:
                    f.cancel()
                if type(event) != BlockEvent:
                    self.log.debug(f'Received event: {event}')
                try:
                    # if event['type'] == 'transfers' and len(event['tfs']) > 0:
                    if type(event) == ChainTransferEvent and len(event.tfs) > 0:
                        await self.db.insert_transfers(event.tfs)
                        has_tfs = len([tf for tf in event.tfs if not any_in(tf.tags, [IDENA_TAG_SUBMIT_FLIP, IDENA_TAG_ACTIVATE, IDENA_TAG_INVITE])]) > 0
                        is_recent = max([tf.timeStamp for tf in event.tfs]) > datetime.now(tz=timezone.utc) - timedelta(seconds=self.conf.recent_transfers_period)
                        if has_tfs and is_recent:
                            await self.check_events()
                        else:
                            self.log.debug(f"No need to check transfers: {has_tfs=}, {is_recent=}")
                    elif type(event) in [BlockEvent, ClubEvent]:
                        self.tracker_event_chan.put_nowait(event)
                    else:
                        self.log.warning(f'Unsupported event: "{event=}"')
                except Exception as e:
                    self.log.error(f"Insert transfers exception: {e}", exc_info=True)
                    self.log.error(f"Event from the exception: {event}")
                    raise e
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
            except Exception as e:
                self.log.error(f'Chain worker exception: "{e}"', exc_info=True)
        trade_task.cancel()
        stats_task.cancel()

    async def check_events(self):
        "Get recent transfers and generate events if needed"
        tfs = await self.db.recent_transfers(self.conf.recent_transfers_period)
        self.log.debug(f"Checking {len(tfs)} transfers")
        if len(tfs) == 0:
            return
        self.check_interesting_events(tfs)
        self.check_dex_events(tfs)
        self.check_transfers(tfs)

    # I wrote this function from scratch like five times and by the end of each time I couldn't tell
    # you how it worked or if it worked correctly. I'm not sure how it works now. Good luck.
    def check_transfers(self, tfs):
        # Collect amounts of sent and received coins per address per hash, and all tfs for that address
        cur_sents = defaultdict(lambda: {'sents': {}, 'recvs': {}, 'tfs': []})
        for tf in tfs:
            if tf.has_no_effect() or DEX_TAG in tf.tags:
                continue
            if tf.hash in self.hashes_notified:
                continue
            for addr, change in tf.changes.items():
                if change == 0:
                    continue
                if change < 0:
                    cur_sents[addr]['sents'][tf.hash] = change
                else:
                    cur_sents[addr]['recvs'][tf.hash] = change
                cur_sents[addr]['tfs'].append(tf)

        events = []
        for addr, addr_sents in cur_sents.items():
            sent = abs(sum(addr_sents['sents'].values()))
            change = sum(addr_sents['recvs'].values()) - sent
            old_sents = self.sents[addr]
            old_sent_hashes, cur_sent_hashes = old_sents['sents'].keys(), addr_sents['sents'].keys()
            sent_removed = old_sent_hashes - cur_sent_hashes
            sent_added = cur_sent_hashes - old_sent_hashes

            for hash, old_change in old_sents['sents'].items():
                if hash in sent_removed:
                    continue
                change -= old_change
            for hash, old_change in old_sents['recvs'].items():
                if hash in sent_removed:
                    continue
                change -= old_change

            sent_usd = float(sent) * self.db.prices['cg:idena']
            amount_usd = float(-1 * change) * self.db.prices['cg:idena']
            if sent_usd >= self.conf.recent_transfers_threshold and amount_usd < self.conf.recent_transfers_threshold:
                max_time = max(map(lambda tf: tf.timeStamp, addr_sents['tfs']))
                for tf in addr_sents['tfs']:
                    if tf.hash not in self.sents_notified:
                        self.sents_notified[tf.hash]['time'] = max_time
                        self.sents_notified[tf.hash]['kept'] = True

            if amount_usd >= self.conf.recent_transfers_threshold and len(sent_added) > 0:
                tfs = addr_sents['tfs']
                del addr_sents['tfs']
                filtered_tfs = list(filter(lambda tf: tf.hash not in self.sents_notified, tfs))
                if len(filtered_tfs) == 0:
                    continue
                self.sents[addr] = addr_sents
                max_time = max(map(lambda tf: tf.timeStamp, tfs))
                show_tfs = list(filter(lambda tf: tf.hash not in self.sents_notified or self.sents_notified[tf.hash]['kept'] is True, tfs))
                tfs_total_value = sum(map(lambda tf: tf.value(), show_tfs))
                majority_tf = [tf for tf in show_tfs if float(tf.value() / tfs_total_value) >= self.conf.majority_volume_fraction]
                if majority_tf:
                    show_tfs = majority_tf
                ev = TransferEvent(by=addr, amount=abs(change), time=max_time, tfs=show_tfs)
                for tf in filtered_tfs:
                    self.sents_notified[tf.hash]['time'] = max_time
                    self.sents_notified[tf.hash]['kept'] = False
                events.append(ev)

        if len(events) > 0:
            self.log.debug(f"Publishing transfer events: {events}")
            for event in events:
                self.tracker_event_chan.put_nowait(event)
        # clean up old state
        states = [self.sents_notified, self.hashes_notified]
        for state in states:
            keys = list(state.keys())
            now = datetime.now(tz=timezone.utc)
            for k in keys:
                if now - state[k]['time'] > timedelta(seconds=max(self.conf.recent_transfers_period, 4 * 60 * 60)):
                    del state[k]

    def check_interesting_events(self, tfs: list[Transfer]):
        "Picks out interesting transfers from `tfs` and emits events for them if needed"
        evs = []
        new_pool_stats = {'kill': defaultdict(dict), 'delegate': defaultdict(dict), 'undelegate': defaultdict(dict)}
        for tf in tfs:
            try:
                if tf.hash in self.hashes_notified:
                    continue
                if self.db.address_is_interesting(tf.signer, tf.chain):
                    if any_in(tf.tags, [IDENA_TAG_INVITE]):
                        continue
                    e = InterestingTransferEvent(time=tf.timeStamp, tfs=[tf], amount=tf.value(), by=tf.signer)
                    evs.append(e)
                    self.hashes_notified[tf.hash]['time'] = tf.timeStamp
                elif any_in(tf.tags, [IDENA_TAG_KILL, IDENA_TAG_KILL_DELEGATOR]) and len(tf.changes) > 0:
                    e = KillEvent.from_tf(tf)
                    if tf.meta.get('usd_value', 0) >= self.conf.killtx_stake_threshold or e.age >= self.conf.killtx_age_threshold:
                        evs.append(e)
                        self.hashes_notified[tf.hash]['time'] = tf.timeStamp
                    elif e.pool:
                        new_pool_stats['kill'][e.pool][e.killed] = PoolEvent.from_tf(tf, subtype=IDENA_TAG_KILL)
                        new_pool_stats['kill'][e.pool][e.killed]._notified = False
                elif any_in(tf.tags, [IDENA_TAG_STAKE, IDENA_TAG_BURN]):
                    if tf.meta.get('usd_value', 0) > self.conf.recent_transfers_threshold:
                        e = TransferEvent(time=tf.timeStamp, tfs=[tf], by=tf.signer, amount=tf.value())
                        evs.append(e)
                        self.hashes_notified[tf.hash]['time'] = tf.timeStamp
                elif any_in(tf.tags, [IDENA_TAG_DELEGATE, IDENA_TAG_UNDELEGATE]):
                    pool = tf.meta.get('pool')
                    ident = self.db.get_identity(tf.signer)
                    if not ident or ident.get('state') == 'Undefined' or not ident.get('age'):
                        self.log.warning(f"Identity {tf.signer} not found, ignoring pool event")
                        self.hashes_notified[tf.hash]['time'] = tf.timeStamp
                        continue
                    if tf.hash not in [tf.hash for tf in self.identity_pool_events[tf.signer]]:
                        self.identity_pool_events[tf.signer].append(tf)
                    if len([tf for tf in self.identity_pool_events[tf.signer] if datetime.now(tz=timezone.utc) - tf.timeStamp < timedelta(days=2)]) >= 3:
                        self.log.warning(f"Identity {tf.signer} is spamming pool events, ignoring")
                        self.identity_pool_events[tf.signer].popleft()
                        self.hashes_notified[tf.hash]['time'] = tf.timeStamp
                        continue
                    subtype = IDENA_TAG_DELEGATE if IDENA_TAG_DELEGATE in tf.tags else IDENA_TAG_UNDELEGATE
                    if tf.signer not in self.pool_events[subtype][pool]:
                        e = PoolEvent.from_tf(tf, subtype=subtype, age=int(ident['age']), stake=Decimal(ident['stake']))
                        new_pool_stats[subtype][pool][tf.signer] = e
            except Exception as e:
                self.log.error(f"Error while processing interesting tf {tf.hash=}: {e}", exc_info=True)
                continue

        for subtype, pools in new_pool_stats.items():
            for pool, p_events in pools.items():
                self.pool_events[subtype][pool].update(p_events)
                p_events = self.pool_events[subtype][pool].values()
                unnotified = filter(lambda ev: ev._notified is False, p_events)
                unnotified = list(filter(lambda ev: datetime.now(tz=timezone.utc) - ev.time < timedelta(seconds=self.conf.pool_identities_moved_period), unnotified))
                if len(unnotified) >= self.conf.pool_identities_moved_threshold:
                    for ev in unnotified:
                        self.log.debug(f"{ev.addr=} {ev.stake=}")
                    cum_stake = sum([ev.stake for ev in unnotified])
                    cum_age = sum([ev.age for ev in unnotified])
                    self.log.debug(f"Pool {pool} {subtype} event: {cum_stake=}, {cum_age=}, {unnotified=}")
                    e = MassPoolEvent(subtype=subtype, pool=pool, count=len(unnotified), stake=cum_stake, age=cum_age, changes=unnotified)
                    for ev in unnotified:
                        ev._notified = True
                    evs.append(e)
                    for event in unnotified:
                        self.hashes_notified[event.tfs[0].hash]['time'] = event.time
        if len(evs) > 0:
            self.log.debug(f"Publishing picked events: {evs}")
            for event in evs:
                self.tracker_event_chan.put_nowait(event)

        # Remove old events from self.pool_events
        for subtype, pools in self.pool_events.items():
            for pool, p_events in pools.items():
                for addr in list(p_events.keys()):
                    ev = p_events[addr]
                    if datetime.now(tz=timezone.utc) - ev.time > timedelta(seconds=self.conf.pool_identities_moved_period + 60 * 60 * 2):
                        del p_events[addr]

    def check_dex_events(self, tfs: list[Transfer]):
        "Picks out DEX transfers from `tfs` and emits events for them if needed"
        dex_volume = 0
        dex_tfs = []
        to_notify = []
        for tf in tfs:
            if DEX_TAG not in tf.tags or tf.hash in self.hashes_notified:
                continue

            usd_value = abs(tf.meta.get('usd_value', 0))
            if usd_value > self.conf.recent_dex_volume_threshold:
                to_notify.append(tf)
                self.hashes_notified[tf.hash]['time'] = tf.timeStamp
                continue

            dex_volume += usd_value
            dex_tfs.append(tf)

        self.log.debug(f"{dex_volume=}")
        if dex_volume > self.conf.recent_dex_volume_threshold:
            to_notify.append(dex_tfs)
            for tf in dex_tfs:
                self.hashes_notified[tf.hash]['time'] = tf.timeStamp

        for tfs in to_notify:
            tfs = tfs if type(tfs) is list else [tfs]
            ev = DexEvent.from_tfs(tfs, self.db.known)
            self.tracker_event_chan.put_nowait(ev)

    async def cex_trade_worker(self):
        "Consumes trades, checks for trade events"
        while True:
            # If a large trade occurs shortly after a trade notification, it wouldn't be shown until the
            # next trade happens, which could be never. So getting trade events will return on a timeout
            # and a check will happen if any trades arrived after the last check.
            have_trades = False
            trades = None
            try:
                chans = [asyncio.create_task(self.trade_chan.get())]
                event_task, pending = await asyncio.wait(chans, return_when=asyncio.FIRST_COMPLETED, timeout=10)
                for f in pending:
                    f.cancel()
                if event_task:
                    trades: list[dict] | None = list(event_task)[0].result()
                    self.log.info(f"Got trades, {len(trades)=}")
                    await self.db.insert_trades(trades)
                    have_trades = True

                if datetime.now(tz=timezone.utc) - self.trades_notified_at > timedelta(seconds=120) and have_trades:
                    await self.check_cex_events()
                    have_trades = False
                elif trades:
                    have_trades = True
            except Exception as e:
                self.log.error(f'Trade worker exception: "{e}"', exc_info=True)

    async def check_cex_events(self):
        "Get recent trades and generate events if volume is higher than `cex_trades_threshold`"
        now = datetime.now(tz=timezone.utc)
        trades = await self.db.recent_trades(self.trades_notified_at, self.conf.cex_volume_period)
        self.log.debug(f"{[tr.id for tr in trades]}")
        total_usd_val = sum([t.usd_value for t in trades])
        self.log.debug(f"{total_usd_val=}")
        if total_usd_val < self.conf.cex_volume_threshold:
            return

        total_buy_val = 0
        total_sell_val = 0
        markets = {m_name: MarketStats(quote_currency=m['quote']) for (m_name, m) in MARKETS.items()}
        for t in trades:
            if t.buy:
                markets[t.market].buy += t.amount
                markets[t.market].buy_usd += t.usd_value
                total_buy_val += t.usd_value
            else:
                markets[t.market].sell += t.amount
                markets[t.market].sell_usd += t.usd_value
                total_sell_val += t.usd_value
            markets[t.market].quote_amount += t.amount * t.price

        final_markets = {}
        for name, market in markets.items():
            if market.volume_idna() == 0:
                continue
            market.avg_price_usd = market.volume_usd() / float(market.volume_idna())
            if market.volume_usd() / total_usd_val > self.conf.majority_volume_fraction:
                final_markets = {name: market}
                break
            final_markets[name] = market

        final_markets = dict(sorted(final_markets.items(), key=lambda m: m[1].volume_usd(), reverse=True))
        event = CexEvent(markets=final_markets, total_buy_val=total_buy_val, total_sell_val=total_sell_val)
        self.log.info(f"Created trade event: {event}")
        self.trades_notified_at = now
        self.tracker_event_chan.put_nowait(event)

    async def stats_worker(self):
        "Generates stats events every `stats_interval` seconds"
        await asyncio.sleep(5)
        try:
            while True:
                await asyncio.sleep(self.conf.stats_interval)
                ev = await self.generate_stats_event()
                self.tracker_event_chan.put_nowait(ev)
        except Exception as e:
            self.log.error(f'Stats exception: "{e}"', exc_info=True)
            await asyncio.sleep(self.conf.stats_interval)

    async def generate_stats_event(self, period=None) -> StatsEvent:
        if not period:
            period = self.conf.stats_interval
        all_tfs = await self.db.recent_transfers(period)
        tagged = defaultdict(list[Transfer])

        for tf in all_tfs:
            for tag in tf.tags:
                tagged[tag].append(tf)

        stats = StatsEvent(period=period)
        stats.invites_issued = len(tagged[IDENA_TAG_INVITE])
        stats.invites_activated = len(tagged[IDENA_TAG_ACTIVATE])
        stats.identities_killed = len(tagged[IDENA_TAG_KILL])
        stats.contract_calls = len(tagged[IDENA_TAG_CALL])

        to_bsc = int(sum(map(lambda tf: tf.changes[NULL_ADDRESS], tagged[BSC_TAG_BRIDGE_MINT])))
        from_bsc = int(sum(map(lambda tf: tf.changes[NULL_ADDRESS], tagged[BSC_TAG_BRIDGE_BURN])))
        stats.bridged_to_bsc = abs(to_bsc)
        stats.bridged_from_bsc = abs(from_bsc)

        staked = int(sum(map(lambda tf: tf.value(), tagged[IDENA_TAG_STAKE])))
        unstaked = int(sum(map(lambda tf: next(iter(tf.changes.values())), tagged[IDENA_TAG_KILL])))
        stats.staked = abs(staked)
        stats.unstaked = abs(unstaked)

        burned = int(sum(map(lambda tf: tf.value(), tagged[IDENA_TAG_BURN])))
        stats.burned = abs(burned)

        trades = await self.db.recent_trades(datetime.min.replace(tzinfo=timezone.utc), period)
        markets = {m_name: MarketStats(quote_currency=m['quote']) for (m_name, m) in MARKETS.items()}
        for t in trades:
            if t.buy:
                markets[t.market].buy += t.amount
                markets[t.market].buy_usd += t.usd_value
            else:
                markets[t.market].sell += t.amount
                markets[t.market].sell_usd += t.usd_value
            markets[t.market].quote_amount += t.amount * t.price
        for market in markets.values():
            market.calculate_average_price(self.db.prices)

        bsc = aggregate_dex_trades(tagged[DEX_TAG_BUY] + tagged[DEX_TAG_SELL], self.db.known)
        markets[MARKET_BSC].__dict__.update(bsc)
        markets[MARKET_BSC].avg_price = f"${bsc['avg_price']:.3f}"
        markets[MARKET_BSC].avg_price_usd = bsc['avg_price']
        markets[MARKET_BSC].buy = int(bsc['buy'])
        markets[MARKET_BSC].sell = int(bsc['sell'])

        for m_name in list(markets.keys()):
            if markets[m_name].volume_idna() == 0:
                del markets[m_name]

        markets = dict(sorted(markets.items(), key=lambda m: m[1].volume_idna(), reverse=True))
        stats.markets = markets
        return stats

    async def generate_top_event(self, top_type: str, period: int = None, long: bool = False) -> TopEvent | None:
        if not period:
            period = self.conf.stats_interval
        all_tfs = await self.db.recent_transfers(period)
        tfs: list[Transfer] = []
        addresses: list[str] = defaultdict(lambda: {'stake': Decimal(0), 'stake_usd': 0, 'signer': None})

        if top_type == 'dex':
            ev = TopDexEvent(period=period)
            dex_tfs = [tf for tf in all_tfs if DEX_TAG in tf.tags]
            aggr = aggregate_dex_trades(dex_tfs, self.db.known)
            ev.total_idna = aggr['buy'] + aggr['sell']
            ev.total_usd_value = float(aggr['quote_amount'])
            dex_tfs.sort(reverse=True, key=lambda tf: tf.meta.get('usd_value', 0))
            ev.items = dex_tfs
            ev.__dict__.update(aggr)
        elif top_type == 'kill':
            ev = TopKillEvent(period=period)
            for tf in all_tfs:
                if IDENA_TAG_KILL in tf.tags:
                    tfs.append(tf)
                    ev.total_age += tf.meta.get('age')
                    ev.total_usd_value += tf.meta.get('usd_value', 0)
                    ev.total_idna += tf.value(True)
            tfs.sort(reverse=True, key=lambda tf: tf.meta.get('usd_value', 0))
            ev.items = tfs
        elif top_type == 'stake':
            ev = TopStakeEvent(period=period)
            for tf in all_tfs:
                if IDENA_TAG_STAKE in tf.tags:
                    addresses[tf.signer]['signer'] = tf.signer
                    addresses[tf.signer]['stake'] += tf.value()
                    addresses[tf.signer]['stake_usd'] += tf.meta.get('usd_value', 0)
                    ev.total_usd_value += tf.meta.get('usd_value', 0)
                    ev.total_idna += tf.value()
            ev.items = list(sorted(addresses.values(), key=lambda a: a['stake'], reverse=True))
        elif top_type == 'transfer':
            ev = TopEvent(period=period)
            for tf in all_tfs:
                value = tf.meta.get('usd_value', 0)
                if not any_in(tf.tags, [IDENA_TAG_KILL, IDENA_TAG_STAKE, DEX_TAG]) and value != 0:
                    tfs.append(tf)
                    ev.total_usd_value += value
            tfs.sort(reverse=True, key=lambda tf: tf.meta.get('usd_value', 0))
            ev.items = tfs

        if len(ev) == 0:
            return None

        ev._long = long
        return ev

    async def generate_pool_stats(self, period=None, long: bool=False) -> PoolStatsEvent | None:
        if not period:
            period = self.conf.stats_interval
        all_tfs = await self.db.recent_transfers(period)

        ev = PoolStatsEvent(period=period)
        for tf in all_tfs:
            pool = tf.meta.get('pool')
            if not pool:
                continue
            if IDENA_TAG_KILL in tf.tags:
                ev.killed[pool] += 1
            elif IDENA_TAG_DELEGATE in tf.tags:
                ev.delegated[pool] += 1
            elif IDENA_TAG_UNDELEGATE in tf.tags:
                ev.undelegated[pool] += 1

        if len(ev) == 0:
            return None
        ev.sort()
        ev._long = long
        self.log.info(f"Created pools event: {ev}")
        return ev

    def calculate_usd_value(self, tf) -> float:
        value = 0
        if any_in(tf.tags, [DEX_TAG_PROVIDE_LP, DEX_TAG_WITHDRAW_LP]):
            idna_price = self.db.prices['cg:idena']
            for pool_addr, pool_ch in tf.meta['lp'].items():
                price_id = self.db.known[self.db.known[pool_addr]['token1']]['price_id']
                token_price = self.db.prices[price_id]
                value += float(pool_ch['token']) * token_price + float(pool_ch['idna']) * idna_price
        elif any_in(tf.tags, [DEX_TAG_BUY, DEX_TAG_SELL, DEX_TAG_ARB]):
            idna_price = self.db.prices['cg:idena']
            for token_addr, token_amount in tf.meta['lp'].items():
                price_id = self.db.known[token_addr]['price_id']
                token_price = self.db.prices[price_id]
                value += token_price * token_amount
        return value

    def _reset_state(self):
        self.sents.clear()
        self.sents_notified.clear()
        self.hashes_notified.clear()
        self.pool_events = {'kill': defaultdict(dict), 'delegate': defaultdict(dict), 'undelegate': defaultdict(dict)}
        self.trades_notified_at = datetime.min.replace(tzinfo=timezone.utc)

    async def _emit_transfer_event(self, tx_hash: str, ev_type: str = 'transfer'):
        tf = await self.db.get_transfer(tx_hash)
        ev = {'by': tf.signer, 'amount': tf.value(),
              'tfs': [tf], 'time': tf.timeStamp}
        if ev_type == 'transfer':
            ev_constructor = TransferEvent
        elif ev_type == 'interesting_transfer':
            ev_constructor = InterestingTransferEvent
        elif ev_type == 'dex':
            ev_constructor = DexEvent
        elif ev_type == 'kill':
            ev_constructor = KillEvent
        else:
            raise Exception(f"Unknown event type: {ev_type}")
        ev = ev_constructor(**ev)
        self.tracker_event_chan.put_nowait(ev)

