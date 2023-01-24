import asyncio
from copy import deepcopy
from decimal import Decimal
from logging import Logger
from collections import defaultdict
from asyncio.queues import Queue as AsyncQueue
from datetime import datetime, timedelta, timezone

from bna.database import Database
from bna.config import TrackerConfig
from bna.transfer import Transfer
from bna.bsc_listener import NULL_ADDRESS
from bna.cex_listeners import MARKET_BSC, MARKETS
from bna.utils import any_in
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
        self.pool_stats = {'kill': defaultdict(dict), 'delegate': defaultdict(dict), 'undelegate': defaultdict(dict)}  # TODO: leaks slowly
        # Tracks time when the last trade notification happened
        self.trades_notified_at = datetime.min.replace(tzinfo=timezone.utc)

    async def run(self):
        trade_task = asyncio.create_task(self.trade_worker(), name="trade_worker")
        stats_task = asyncio.create_task(self.stats_worker(), name="stats_worker")
        while True:
            try:
                chans = [asyncio.create_task(self.idna_chan.get()),
                         asyncio.create_task(self.bsc_chan.get())]
                event_task, pending = await asyncio.wait(chans, return_when=asyncio.FIRST_COMPLETED)
                event: dict = list(event_task)[0].result()
                for f in pending:
                    f.cancel()
                if event.get('type') != 'block':
                    self.log.debug(f'Received event: {event}')
                try:
                    if event['type'] == 'transfers' and len(event['tfs']) > 0:
                        await self.db.insert_transfers(event['tfs'])
                        has_tfs = len([tf for tf in event['tfs'] if not any_in(tf.tags, [IDENA_TAG_SUBMIT_FLIP, IDENA_TAG_ACTIVATE, IDENA_TAG_INVITE])]) > 0
                        is_recent = max([tf.timeStamp for tf in event['tfs']]) > datetime.now(tz=timezone.utc) - timedelta(seconds=self.conf.recent_transfers_period)
                        if has_tfs and is_recent:
                            await self.check_events()
                        else:
                            self.log.debug(f"No need to check transfers: {has_tfs=}, {is_recent=}")
                    elif event['type'] == 'block':
                        self.tracker_event_chan.put_nowait(event)
                    else:
                        self.log.warning(f'Unsupported event type: "{event["type"]}"')
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
                ev = {'type': 'transfer', 'by': addr, 'amount': abs(change),
                      'tfs': show_tfs, 'time': max_time}
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
                    e = {'type': 'interesting_transfer', 'by': tf.signer, 'time': tf.timeStamp, 'tfs': [tf]}
                    evs.append(e)
                    self.hashes_notified[tf.hash]['time'] = tf.timeStamp
                elif any_in(tf.tags, [IDENA_TAG_KILL, IDENA_TAG_KILL_DELEGATOR]) and len(tf.changes) > 0:
                    stake_receiver, stake = next(iter(tf.changes.items()))
                    killed = tf.meta['killedIdentity']
                    age = tf.meta.get('age')
                    pool = tf.meta.get('pool')
                    e = {'type': 'kill', 'addr': killed, 'hash': tf.hash, 'tfs': [tf],
                        'stake': stake, 'age': age, 'pool': pool, 'time': tf.timeStamp}
                    if tf.meta.get('usd_value', 0) >= self.conf.killtx_stake_threshold or age >= self.conf.killtx_age_threshold:
                        evs.append(e)
                        self.hashes_notified[tf.hash]['time'] = tf.timeStamp
                    elif pool:
                        new_pool_stats['kill'][pool][killed] = e.copy()
                elif IDENA_TAG_STAKE in tf.tags:
                    if tf.meta.get('usd_value', 0) > self.conf.recent_transfers_threshold:
                        e = {'type': 'transfer', 'by': tf.signer, 'hash': tf.hash, 'time': tf.timeStamp, 'tfs': [tf]}
                        evs.append(e)
                        self.hashes_notified[tf.hash]['time'] = tf.timeStamp
                elif any_in(tf.tags, [IDENA_TAG_DELEGATE, IDENA_TAG_UNDELEGATE]):
                    pool = tf.meta.get('pool')
                    ident = self.db.get_identity(tf.signer)
                    subtype = IDENA_TAG_DELEGATE if IDENA_TAG_DELEGATE in tf.tags else IDENA_TAG_UNDELEGATE
                    if tf.signer not in self.pool_stats[subtype][pool]:
                        e = {'type': subtype, 'addr': tf.signer, 'hash': tf.hash, 'tfs': [tf],
                            'stake': float(ident['stake']), 'age': int(ident['age']), 'pool': pool, 'time': tf.timeStamp}
                        new_pool_stats[subtype][pool][tf.signer] = e.copy()
            except Exception as e:
                self.log.error(f"Error while processing interesting tf {tf.hash=}: {e}", exc_info=True)
                continue

        for subtype, pools in new_pool_stats.items():
            for pool, p_events in pools.items():
                self.pool_stats[subtype][pool].update(p_events)
                p_events = self.pool_stats[subtype][pool].values()
                unnotified = filter(lambda ev: ev.get('notified') is None, p_events)
                unnotified = list(filter(lambda ev: datetime.now(tz=timezone.utc) - ev['time'] < timedelta(seconds=self.conf.pool_identities_moved_period), unnotified))
                if len(unnotified) >= self.conf.pool_identities_moved_threshold:
                    count = len(p_events)
                    cum_stake = sum([ev.get('stake', 0) for ev in unnotified])
                    cum_age = sum([ev.get('age', 0) for ev in unnotified])
                    e = {'type': 'mass_pool', 'subtype': subtype, 'pool': pool, 'count': count,
                        'stake': cum_stake, 'age': cum_age, 'tfs': unnotified}
                    for ev in unnotified:
                        ev['notified'] = True
                    evs.append(e)
                    for event in unnotified:
                        self.hashes_notified[event['hash']]['time'] = event['time']
        if len(evs) > 0:
            self.log.debug(f"Publishing picked events: {evs}")
            for event in evs:
                self.tracker_event_chan.put_nowait(event)

    # TODO: Test this
    def check_dex_events(self, tfs: list[Transfer]):
        "Picks out DEX transfers from `tfs` and emits events for them if needed"
        trade_volume = 0
        trade_tfs = []
        lp_volume = 0
        lp_tfs = []
        to_notify = []
        self.log.debug(f"{[tf.hash[:10] for tf in tfs]}")
        for tf in tfs:
            if DEX_TAG not in tf.tags or tf.hash in self.hashes_notified:
                continue

            usd_value = tf.meta.get('usd_value', 0)
            if usd_value > self.conf.recent_dex_volume_threshold:
                to_notify.append(tf)
                self.hashes_notified[tf.hash]['time'] = tf.timeStamp
                continue

            if any_in(tf.tags, [DEX_TAG_PROVIDE_LP, DEX_TAG_WITHDRAW_LP]):
                lp_volume += usd_value
                lp_tfs.append(tf)
            elif any_in(tf.tags, DEX_TRADE_TAGS):
                trade_volume += usd_value
                trade_tfs.append(tf)

        self.log.debug(f"{trade_volume=}, {lp_volume=}")
        if trade_volume > self.conf.recent_dex_volume_threshold:
            to_notify.append(trade_tfs)
            for tf in trade_tfs:
                self.hashes_notified[tf.hash]['time'] = tf.timeStamp
        if lp_volume > self.conf.recent_dex_volume_threshold:
            to_notify.append(lp_tfs)
            for tf in lp_tfs:
                self.hashes_notified[tf.hash]['time'] = tf.timeStamp

        for ev in to_notify:
            # Single DEX event
            if type(ev) is Transfer:
                ev = {'type': 'dex', 'tfs': [ev], 'time': ev.timeStamp}
                self.log.debug(f"Created DEX event: {ev}")
            # Multi DEX event
            else:
                max_time = max(map(lambda tf: tf.timeStamp, ev))
                tfs = sorted(ev, key=lambda tf: tf.meta.get('usd_value', 0),
                reverse=True)
                m = self.aggregate_dex_trades(tfs)
                top_volume = 0
                for i, tf in enumerate(tfs):
                    top_volume += tf.meta.get('usd_value', 0)
                    if top_volume / (m['buy_usd'] + m['sell_usd']) >= self.conf.majority_volume_fraction:
                        break
                top_tfs = tfs[:i + 1]
                truncated = len(tfs) - len(top_tfs)
                ev = {'type': 'dex', 'tfs': top_tfs, 'time': max_time, 'avg_price': m['avg_price'],
                      'buy_usd': m['buy_usd'], 'sell_usd': m['sell_usd'], 'truncated': truncated}
                self.log.debug(f"Created multi DEX event: {ev}")
            self.tracker_event_chan.put_nowait(ev)

    async def trade_worker(self):
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

                if datetime.now(tz=timezone.utc) - self.trades_notified_at > timedelta(seconds=60) and have_trades:
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
        markets = defaultdict(lambda: {'buy': Decimal(0), 'sell': Decimal(0),
                                       'buy_usd_value': 0, 'sell_usd_value': 0, 'avg_price': Decimal(0)})
        for t in trades:
            markets[t.market]['buy' if t.buy else 'sell'] += t.amount
            markets[t.market][f"{'buy' if t.buy else 'sell'}_usd_value"] += t.usd_value
            if t.buy:
                total_buy_val += t.usd_value
            else:
                total_sell_val += t.usd_value

        final_markets = []
        for name, market in markets.items():
            market['name'] = name
            m_vol = market['buy'] + market['sell']
            m_usd_val = market['buy_usd_value'] + market['sell_usd_value']
            market['avg_price'] = m_usd_val / float(m_vol)
            if m_usd_val / total_usd_val > self.conf.majority_volume_fraction:
                final_markets = [market]
            final_markets.append(market)

        final_markets.sort(key=lambda m: m['buy_usd_value'] + m['sell_usd_value'], reverse=True)
        event = {'type': 'cex_trade', 'markets': {m['name']: m for m in final_markets},
                 'total_buy_val': total_buy_val, 'total_sell_val': total_sell_val}
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

    async def generate_stats_event(self, period=None):
        if not period:
            period = self.conf.stats_interval
        all_tfs = await self.db.recent_transfers(period)
        tagged = defaultdict(list[Transfer])

        for tf in all_tfs:
            for tag in tf.tags:
                tagged[tag].append(tf)

        stats = {}
        stats['invites_issued'] = len(tagged[IDENA_TAG_INVITE])
        stats['invites_activated'] = len(tagged[IDENA_TAG_ACTIVATE])
        stats['identities_killed'] = len(tagged[IDENA_TAG_KILL])
        stats['contract_calls'] = len(tagged[IDENA_TAG_CALL])

        to_bsc = int(sum(map(lambda tf: tf.changes[NULL_ADDRESS], tagged[BSC_TAG_BRIDGE_MINT])))
        from_bsc = int(sum(map(lambda tf: tf.changes[NULL_ADDRESS], tagged[BSC_TAG_BRIDGE_BURN])))
        stats['bridged_to_bsc'] = abs(to_bsc)
        stats['bridged_from_bsc'] = abs(from_bsc)

        staked = int(sum(map(lambda tf: tf.value(), tagged[IDENA_TAG_STAKE])))
        unstaked = int(sum(map(lambda tf: next(iter(tf.changes.values())), tagged[IDENA_TAG_KILL])))
        stats['staked'] = abs(staked)
        stats['unstaked'] = abs(unstaked)

        burned = int(sum(map(lambda tf: tf.value(), tagged[IDENA_TAG_BURN])))
        stats['burned'] = abs(burned)

        trades = await self.db.recent_trades(datetime.min.replace(tzinfo=timezone.utc), period)
        markets = defaultdict(lambda: {'buy': Decimal(0), 'sell': Decimal(0), 'buy_usd': 0, 'sell_usd': 0,
                                       'quote_amount': Decimal(0)})
        for t in trades:
            markets[t.market]['buy' if t.buy else 'sell'] += t.amount
            markets[t.market]['buy_usd' if t.buy else 'sell_usd'] += t.usd_value
            markets[t.market]['quote_amount'] += t.amount * t.price
        for m_name, market in markets.items():
            m_vol = market['buy'] + market['sell']
            avg_price = market['quote_amount'] / m_vol
            if MARKETS[m_name]['base'] == 'cg:bitcoin':
                avg_price_usd = float(avg_price) * self.db.prices['cg:bitcoin']
                avg_price *= Decimal("1e8")
                market['avg_price'] = f"{avg_price:.0f} sats"
                market['avg_price_usd'] = f"${avg_price_usd:.3f}"
            elif MARKETS[m_name]['base'] in ['cg:tether', 'cg:binance-usd', 'usd']:
                market['avg_price'] = f"${avg_price:.3f}"
                market['avg_price_usd'] = f"${avg_price:.3f}"
            market['buy'], market['sell'] = int(market['buy']), int(market['sell'])

        # BSC trades aggregation
        bsc = self.aggregate_dex_trades(tagged[DEX_TAG_BUY] + tagged[DEX_TAG_SELL])
        # self.log.debug(f"{bsc=}")
        markets[MARKET_BSC].update(bsc)
        markets[MARKET_BSC]['avg_price'] = f"${bsc['avg_price']:.3f}"
        markets[MARKET_BSC]['avg_price_usd'] = f"${bsc['avg_price']:.3f}"
        markets[MARKET_BSC]['buy'] = int(bsc['buy'])
        markets[MARKET_BSC]['sell'] = int(bsc['sell'])
        if markets[MARKET_BSC]['buy'] + markets[MARKET_BSC]['sell'] == 0:
            del markets[MARKET_BSC]

        markets = dict(sorted(markets.items(), key=lambda m: m[1]['buy'] + m[1]['sell'], reverse=True))
        stats['markets'] = dict(markets)
        return {'type': 'stats', 'period': period, 'stats': stats}

    async def generate_top_event(self, top_type: str, period: int = None, long: bool = False) -> dict | None:
        if not period:
            period = self.conf.stats_interval
        all_tfs = await self.db.recent_transfers(period)
        tfs: list[Transfer] = []
        addresses: list[str] = defaultdict(lambda: {'stake': 0, 'stake_usd': 0, 'signer': None})
        ev = {'event': 'top', 'top_type': top_type, 'period': period,
              'total_usd_value': 0, 'total_idna': Decimal(0)}

        if top_type == 'dex':
            dex_tfs = [tf for tf in all_tfs if DEX_TAG in tf.tags]
            aggr = self.aggregate_dex_trades(dex_tfs)
            ev['total_idna'] = aggr['buy'] + aggr['sell']
            ev['total_usd_value'] = aggr['quote_amount']
            dex_tfs.sort(reverse=True, key=lambda tf: tf.meta.get('usd_value', 0))
            ev['items'] = dex_tfs
            ev.update(aggr)
        elif top_type == 'kill':
            ev['total_age'] = 0
            for tf in all_tfs:
                if IDENA_TAG_KILL in tf.tags:
                    tfs.append(tf)
                    ev['total_age'] += tf.meta.get('age')
                    ev['total_usd_value'] += tf.meta.get('usd_value', 0)
                    ev['total_idna'] += tf.value(True)
            tfs.sort(reverse=True, key=lambda tf: tf.meta.get('usd_value', 0))
            ev['items'] = tfs
        elif top_type == 'stake':
            for tf in all_tfs:
                if IDENA_TAG_STAKE in tf.tags:
                    addresses[tf.signer]['signer'] = tf.signer
                    addresses[tf.signer]['stake'] += int(tf.value())
                    addresses[tf.signer]['stake_usd'] += tf.meta.get('usd_value', 0)
                    ev['total_usd_value'] += tf.meta.get('usd_value', 0)
                    ev['total_idna'] += tf.value()
            ev['items'] = list(sorted(addresses.values(), key=lambda a: a['stake'], reverse=True))
        elif top_type == 'transfer':
            for tf in all_tfs:
                value = tf.meta.get('usd_value', 0)
                if not any_in(tf.tags, [IDENA_TAG_KILL, IDENA_TAG_STAKE, DEX_TAG]) and value != 0:
                    tfs.append(tf)
                    ev['total_usd_value'] += value
            tfs.sort(reverse=True, key=lambda tf: tf.meta.get('usd_value', 0))
            ev['items'] = tfs

        if len(ev['items']) == 0:
            return None

        full_count = len(ev['items'])
        self.log.debug(f"tf count before truncation: {full_count}")
        max_lines = self.conf.top_events_max_lines
        if long:
            max_lines = 100
        ev['items'] = ev['items'][:max_lines]
        ev['truncated'] = full_count - len(ev['items'])
        return ev

    async def generate_pool_stats(self, period=None) -> dict | None:
        if not period:
            period = self.conf.stats_interval
        all_tfs = await self.db.recent_transfers(period)

        s = {'killed': 0, 'delegated': 0, 'undelegated': 0}
        pool_stats = defaultdict(lambda: deepcopy(s))
        for tf in all_tfs:
            pool = tf.meta.get('pool')
            if not pool:
                continue

            if IDENA_TAG_KILL in tf.tags:
                pool_stats[pool]['killed'] += 1
            elif IDENA_TAG_DELEGATE in tf.tags:
                pool_stats[pool]['delegated'] += 1
            elif IDENA_TAG_UNDELEGATE in tf.tags:
                pool_stats[pool]['undelegated'] += 1

        if len(pool_stats) == 0:
            return None
        event = {'type': 'pool_stats', 'period': period, 'stats': pool_stats}
        self.log.info(f"Created pools event: {event}")
        return event

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

    def aggregate_dex_trades(self, tfs: list[Transfer]) -> dict:
        m = {'buy': Decimal(0), 'sell': Decimal(0), 'buy_usd': 0, 'sell_usd': 0, 'quote_amount': 0, 'lp_usd': 0}
        for tf in tfs:
            if any_in(tf.tags, DEX_LP_TAGS):
                self.log.debug(f"{tf=}")
                lp_excess = tf.meta.get('lp_excess', Decimal(0))
                if lp_excess:
                    combined_usd = tf.meta.get('usd_value', 0)
                    traded_usd = combined_usd - float(abs(tf.meta.get('lp_excess', 0))) * tf.meta.get('usd_price', 0)
                    lp = combined_usd - traded_usd
                    m['quote_amount'] += traded_usd
                    if lp_excess > 0:
                        m['buy'] += lp_excess
                        m['buy_usd'] += traded_usd
                    else:
                        m['sell'] += lp_excess
                        m['sell_usd'] += traded_usd
                    m['lp_usd'] += lp * (-1 if DEX_TAG_WITHDRAW_LP in tf.tags else 1)
                else:
                    m['lp_usd'] += tf.meta.get('usd_value', 0) * (-1 if DEX_TAG_WITHDRAW_LP in tf.tags else 1)
            elif any_in(tf.tags, DEX_TRADE_TAGS):
                is_buy = DEX_TAG_BUY in tf.tags
                m['quote_amount'] += tf.meta.get('usd_value', 0)
                m['buy_usd' if is_buy else 'sell_usd'] += tf.meta.get('usd_value', 0)
                for addr, amount in tf.changes.items():
                    if self.db.addr_type(addr) == 'pool':
                        m['buy' if is_buy else 'sell'] += amount * (-1 if is_buy else 1)
        bsc_vol = float(m['buy'] + m['sell'])
        m['avg_price'] = (m['quote_amount'] / bsc_vol) if bsc_vol != 0 else 0
        return m

    def _reset_state(self):
        self.sents.clear()
        self.sents_notified.clear()
        self.hashes_notified.clear()
        self.pool_stats = {'kill': defaultdict(dict), 'delegate': defaultdict(dict), 'undelegate': defaultdict(dict)}
        self.trades_notified_at = datetime.min.replace(tzinfo=timezone.utc)

    async def _emit_transfer_event(self, tx_hash: str, ev_type: str = 'transfer'):
        tf = await self.db.get_transfer(tx_hash)
        ev = {'type': ev_type, 'by': tf.signer, 'amount': tf.value(),
              'tfs': [tf], 'time': tf.timeStamp}
        self.tracker_event_chan.put_nowait(ev)

