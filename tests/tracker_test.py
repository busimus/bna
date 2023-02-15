import os
import time
import asyncio
import pytest
from copy import deepcopy
from decimal import Decimal
from collections import defaultdict
from asyncio import Queue as AsyncQueue
from datetime import datetime, timezone, timedelta

from bna import init_logging
from bna.discord_bot import Bot
from bna.config import Config
from bna.database import Database
from bna.event import *
from bna.tracker import Tracker
from bna.transfer import Transfer
from bna.cex_listeners import Trade, MARKETS

tf_ev = lambda a, v, l: {'_type': TransferEvent, 'by': a, 'amount': Decimal(v), '_tfs_len': l}
ki_ev = lambda a, v: {'_type': KillEvent, 'killed': a, 'stake': v, 'amount': Decimal(v), 'by': a}
po_ev = lambda a, v, g: {'_type': MassPoolEvent, 'subtype': 'kill', 'pool': a, 'stake': v, 'age': g}
in_ev = lambda a, v, l: {'_type': InterestingTransferEvent, 'by': a, 'amount': v, '_tfs_len': l}

def tr_ev(ms: list) -> dict | None:
    if not ms:
        return None
    markets = {m_name: MarketStats(quote_currency=m['quote']) for (m_name, m) in MARKETS.items()}

    total_buy_val, total_sell_val = 0.0, 0.0
    for m in ms:
        print(m)
        markets[m[0]].__dict__.update({
            'buy': Decimal(m[1]), 'sell': Decimal(m[2]),
            'buy_usd': float(m[3]), 'sell_usd': float(m[4]),
            'avg_price_usd': float(m[5]), 'avg_price': "",
            'quote_amount': Decimal(m[1] * float(m[5]) + m[2] * float(m[5]))})
        total_buy_val += m[3]
        total_sell_val += m[4]
    markets = dict(filter(lambda m: m[1].volume_idna() > 0, markets.items()))
    ev = CexEvent(total_buy_val=total_buy_val, total_sell_val=total_sell_val, markets=markets)
    return ev


transfer_cases = {
'config': {'majority_volume_fraction': 1,
           'recent_transfers_threshold': 100},
'cases': [
    # Case 0: Nothing
    [({'from': '0x1', 'to': '0x2', 'amount': '50'}, None)],
    # Case 1: One large
    [({'from': '0x1', 'to': '0x2', 'amount': '100'}, tf_ev('0x1', 100, 1))],
    # Case 2: Multiple small
    [({'from': '0x1', 'to': '0x2', 'amount': '50'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '55'}, tf_ev('0x1', 105, 2)),
     ({'from': '0x1', 'to': '0x2', 'amount': '50'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '50'}, tf_ev('0x1', 100, 2))],
    # Case 3: Receive many small and send
    [({'from': '0x1', 'to': '0x0', 'amount': '50'}, None),
     ({'from': '0x2', 'to': '0x0', 'amount': '50'}, None),
     ({'from': '0x3', 'to': '0x0', 'amount': '50'}, None),
     ({'from': '0x0', 'to': '0x4', 'amount': '150'}, None),
     ({'from': '0x0', 'to': '0x4', 'amount': '200'}, tf_ev('0x0', 200, 5))],
    # Case 4: Back and forth
    [({'from': '0x1', 'to': '0x2', 'amount': '100'}, tf_ev('0x1', 100, 1)),
     ({'from': '0x2', 'to': '0x1', 'amount': '100'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '100'}, None),
     ({'from': '0x2', 'to': '0x1', 'amount': '100'}, None),
     ({'from': '0x1', 'to': '0x3', 'amount': '200'}, tf_ev('0x1', 100, 4))],
    # Case 5: Recv and spam ???
    [({'from': '0x2', 'to': '0x1', 'amount': '50'}, None),
     ({'from': '0x3', 'to': '0x1', 'amount': '50'}, None),
     ({'from': '0x4', 'to': '0x1', 'amount': '50'}, None),
     ({'from': '0x1', 'to': '0x5', 'amount': '200'}, None),
     ({'from': '0x5', 'to': '0x1', 'amount': '200'}, None),
     ({'from': '0x1', 'to': '0x5', 'amount': '250'}, tf_ev('0x1', 100, 6))],
    # Case 6: Slow multiple
    [({'from': '0x1', 'to': '0x2', 'amount': '50'}, None),
     ({'from': 'wait', 'to': '', 'amount': '1'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '50'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '50'}, tf_ev('0x1', 100, 2))],
    # Case 7: Slow multiple and complicated ???
    [({'from': '0x1', 'to': '0x2', 'amount': '50'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '75'}, tf_ev('0x1', 125, 2)),
     ({'from': '0x1', 'to': '0x2', 'amount': '50'}, None),
     ({'from': 'wait', 'to': '', 'amount': '1'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '50'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '50'}, tf_ev('0x1', 100, 2))],
    # Case 8: Large, wait, small
    [({'from': '0x1', 'to': '0x2', 'amount': '500'}, tf_ev('0x1', 500, 1)),
     ({'from': 'wait', 'to': '', 'amount': '1'}, None),
     ({'from': 'wait', 'to': '', 'amount': '1'}, None),
     ({'from': 'wait', 'to': '', 'amount': '1'}, None),
     ({'from': 'wait', 'to': '', 'amount': '1'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '50'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '75'}, tf_ev('0x1', 125, 2))],
    # Case 9: Circle
    [({'from': '0x1', 'to': '0x2', 'amount': '100'}, tf_ev('0x1', 100, 1)),
     ({'from': '0x2', 'to': '0x1', 'amount': '100'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '100'}, None)],
    # Case 10: Big circle
    [({'from': '0x1', 'to': '0x2', 'amount': '500'}, tf_ev('0x1', 500, 1)),
     ({'from': '0x2', 'to': '0x3', 'amount': '500'}, None),
     ({'from': '0x3', 'to': '0x4', 'amount': '500'}, None),
     ({'from': '0x4', 'to': '0x1', 'amount': '500'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '500'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '50'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '80'}, tf_ev('0x1', 130, 4))],
]}

majority_transfer_cases = {
'config': {'majority_volume_fraction': 0.75,
           'recent_transfers_threshold': 100},
'cases': [
    # Case 0: Multiple without majority
    [({'from': '0x1', 'to': '0x2', 'amount': '50'}, None),
     ({'from': '0x1', 'to': '0x3', 'amount': '20'}, None),
     ({'from': '0x1', 'to': '0x4', 'amount': '55'}, tf_ev('0x1', 125, 3))],
    # Case 1: One major and many small
    [({'from': '0x1', 'to': '0x2', 'amount': '5'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '5'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '10'}, None),
     ({'from': '0x1', 'to': '0x2', 'amount': '500'}, tf_ev('0x1', 520, 1))],
]}

FND = "0xcbb98843270812eece07bfb82d26b4881a33aa91"
PRM = "0xc94d32638d71aba05f0bdade498948ef93944428"

interesting_cases = {
'config': None,
'cases': [
    # Case 0: Unintetersing tag
    [({'from': FND, 'to': '0x2', 'amount': '0', 'tags': ['invite']}, None)],
    # Case 1: Foundation
    [({'from': FND, 'to': '0x2', 'amount': '1'}, in_ev(FND, 1, 1))],
    # Case 2: Premine
    [({'from': PRM, 'to': '0x2', 'amount': '1000'}, in_ev(PRM, 1000, 1))],
]}


kill_cases = {
'config': {'killtx_stake_threshold': 50,
           'killtx_age_threshold': 10,
           'pool_identities_moved_threshold': 3,
           'pool_identities_moved_period': 1000},
'cases': [
    # Case 0: Low stake and age
    [({'from': '0x1', 'amount': '-10', 'meta': {'age': 2, 'pool': None}}, None)],
    # Case 1: High enough stake
    [({'from': '0x1', 'amount': '-50'}, ki_ev('0x1', 50))],
    # Case 2: High enough age
    [({'from': '0x1', 'amount': '-1', 'meta': {'age': 10, 'pool': None}}, ki_ev('0x1', 1))],
    # Case 3: Many small from one pool
    [({'from': '0x1', 'amount': '-1', 'meta': {'age': 4, 'pool': '0xD'}}, None),
     ({'from': '0x2', 'amount': '-2', 'meta': {'age': 5, 'pool': '0xD'}}, None),
     ({'from': '0x3', 'amount': '-3', 'meta': {'age': 6, 'pool': '0xD'}}, po_ev('0xD', 6, 15)),
     ({'from': '0x4', 'amount': '-10', 'meta': {'age': 7, 'pool': '0xD'}}, None),
     ({'from': '0x5', 'amount': '-20', 'meta': {'age': 8, 'pool': '0xD'}}, None),
     ({'from': '0x6', 'amount': '-30', 'meta': {'age': 9, 'pool': '0xD'}}, po_ev('0xD', 60, 24))],
    # Case 4: KillDelegatorTx
    [({'from': '0xD', 'to': '0x1', 'amount': '-2', 'meta': {'age': 4, 'pool': '0xD'}}, None),
     ({'from': '0xD', 'to': '0x2', 'amount': '-4', 'meta': {'age': 5, 'pool': '0xD'}}, None),
     ({'from': '0xD', 'to': '0x3', 'amount': '-6', 'meta': {'age': 6, 'pool': '0xD'}}, po_ev('0xD', 12, 15)),
     ({'from': '0xD', 'to': '0x4', 'amount': '-20', 'meta': {'age': 7, 'pool': '0xD'}}, None),
     ({'from': '0xD', 'to': '0x5', 'amount': '-30', 'meta': {'age': 8, 'pool': '0xD'}}, None),
     ({'from': '0xD', 'to': '0x6', 'amount': '-40', 'meta': {'age': 9, 'pool': '0xD'}}, po_ev('0xD', 90, 24))],
]}

for k in kill_cases['cases']:
    for step in k:
        if 'to' in step[0]:  # sent by a pool
            step[0]['tags'] = ['killDelegator']
            killed = step[0]['to']
        else:
            step[0]['tags'] = ['kill']
            killed = step[0]['from']
        if 'meta' not in step[0]:
            step[0]['meta'] = {'age': 1, 'pool': None}
        if step[1] is not None and step[1]['_type'] == KillEvent:
            step[1].update(step[0]['meta'])
        step[0]['meta']['killedIdentity'] = killed

trade_cases = {
'config': {'cex_volume_period': 1000,
           'cex_volume_threshold': 100,
           'majority_volume_fraction': 1},
'cases': [
    # Case 0: Nothing
    [({"price": "1", "amount": "10", "market": "bitmart", "quote": "tether", "buy": True}, None)],
    # Case 1: One large
    [({"price": "1", "amount": "1000", "market": "bitmart", "quote": "tether", "buy": True}, tr_ev([("bitmart", 1000, 0, 1000, 0, 1)]))],
    # Case 2: Multiple small
    [({"price": "1", "amount": "10", "market": "bitmart", "quote": "tether", "buy": True}, None),
     ({"price": "1", "amount": "75", "market": "bitmart", "quote": "tether", "buy": False}, None),
     ({"price": "1", "amount": "55", "market": "bitmart", "quote": "tether", "buy": True}, tr_ev([("bitmart", 65, 75, 65, 75, 1)]))],
    # Case 3: Multiple CEXes
    [({"price": "1", "amount": "10", "market": "hotbit", "quote": "tether", "buy": True}, None),
     ({"price": "1", "amount": "75", "market": "bitmart", "quote": "tether", "buy": False}, None),
     ({"price": "1", "amount": "55", "market": "bitmart", "quote": "tether", "buy": True}, tr_ev([("hotbit", 10, 0, 10, 0, 1), ("bitmart", 55, 75, 55, 75, 1)]))],
]}

def get_default_config():
    c = Config()
    c.db.cached_record_age_limit = 5000
    c.tracker.recent_transfers_period = 1000
    return c


async def get_test_env():
    log = init_logging()
    events = AsyncQueue()
    os.environ['POSTGRES_CONNSTRING'] = 'postgresql://bna:bna@localhost:15432/bna_pytest'
    os.environ['DEV_USER_ID'] = '0'
    db = Database(log, f'/tmp/bna_pytest_conf_{int(time.time())}.json')
    await db.connect(True)
    db.cache_cleaned_at = datetime.max.replace(tzinfo=timezone.utc)
    db.prices['cg:idena'] = 1 # a man can dream
    db.prices['tether'] = 1
    db.prices['bitcoin'] = 20000
    return log, events, db

@pytest.mark.asyncio
async def test_transfer_events():
    "Tests event generation for transfers, kills and interesting cases."
    log, events, db = await get_test_env()

    i = 0
    now = int(time.time())
    bot = Bot(None, Config().discord, db, 0, log)
    bot.stopped = True
    tracker = Tracker(db, None, None, None, None, None, events, log)
    for t, table in enumerate([transfer_cases, majority_transfer_cases,
                               kill_cases, interesting_cases]):
        table_config, cases = table['config'], table['cases']
        conf = get_default_config()
        if table_config:
            conf.tracker.__dict__.update(table_config)
        bot.conf = conf.discord
        tracker.conf = conf.tracker
        timestamp = now
        for c, case in enumerate(cases):
            for s, step in enumerate(case):
                step[0]['timestamp'] = timestamp
                timestamp += 1
                step[0]['hash'] = f"{t}:{c}:{s}"
                if 'kill' in step[0].get('tags', []) or 'killDelegator' in step[0].get('tags', []):
                    if step[1]:
                        step[1]['stake'] = Decimal(step[1]['stake'])
                    tf = Transfer.from_idena_rpc(step[0], 100 + i, 0, tags=step[0]['tags'])
                    tf.meta['usd_value'] = abs(float(step[0]['amount']))
                    if 'meta' in step[0]:
                        tf.meta = step[0]['meta']
                else:
                    if step[1] and 'amount' in step[1]:
                        step[1]['amount'] = Decimal(step[1]['amount'])
                    tf = Transfer.from_idena_rpc(step[0], 100 + i, 0, tags=step[0].get('tags', []))
                case[s] = (tf, step[1])
                i += 1

        for i, case in enumerate(cases):
            print(f"###   Transfer Case {t}_{i}   ###")
            await run_transfer_case(tracker, case, bot)
            tracker._reset_state()
    await db.close()

async def run_transfer_case(t: Tracker, tf_evs: list[(Transfer, dict)], bot: Bot):
    """
    Takes a tracker and a list of tuples (Transfer, event), inserts transfers
    and checks for events. Then removes transfers in reverse and checks for no events.
    """
    chan = t.tracker_event_chan
    prev_tf = None
    for i, step in enumerate(tf_evs):
        print(f"######   Step {i}   ######")
        tf, ev = step[0], step[1]
        if tf.changes.get('wait') is not None:
            await t.db._remove_transfer(prev_tf)
            continue
        prev_tf = tf
        print(tf)
        await t.db.insert_transfers([tf])
        await t.check_events()
        got_ev = compare_transfer_event(chan, ev)
        if got_ev:
            await bot._publish_event(got_ev) # to test that it doesn't crash

    print("Going backwards")
    # removing transfers
    for step in tf_evs:
        tf, ev = step[0], step[1]
        await t.db._remove_transfer(tf)
        await t.check_events()
        compare_transfer_event(chan, None)
    await t.db.store.conn.commit()

def compare_transfer_event(chan: AsyncQueue, expected_event: dict | None) -> dict | None:
    full_ev, cmp_ev = None, None
    try:
        full_ev = chan.get_nowait()
        cmp_ev = deepcopy(full_ev)
        assert type(cmp_ev) == expected_event['_type']
        del expected_event['_type']
        if expected_event and expected_event.get('_tfs_len'):
            assert len(cmp_ev.tfs) == expected_event['_tfs_len']
            del expected_event['_tfs_len']
        for item in ['id', 'time', 'hash', 'tfs', 'changes', 'kills', 'count', '_recv']:
            try:
                del cmp_ev.__dict__[item]
            except:
                pass
    except asyncio.queues.QueueEmpty:
        pass
    except Exception as e:
        print('exception: ', e)
        raise e

    if cmp_ev is not None:
        assert cmp_ev.__dict__ == expected_event
    else:
        assert cmp_ev == expected_event
    return full_ev

@pytest.mark.asyncio
async def test_cex_trade_events():
    log, events, db = await get_test_env()

    tracker = Tracker(db, None, None, None, None, None, events, log)
    bot = Bot(None, Config().discord, db, 0, log)
    bot.stopped = True
    i = 0
    now = int(time.time())
    for t, table in enumerate([trade_cases]):
        table_config, cases = table['config'], table['cases']
        conf = get_default_config()
        if table_config:
            conf.tracker.__dict__.update(table_config)
        bot.conf = conf.discord
        tracker.conf = conf.tracker
        timestamp = now - 100
        for c, case in enumerate(cases):
            for s, step in enumerate(case):
                step[0]['id'] = timestamp
                step[0]['timeStamp'] = timestamp
                step[0]['usd_value'] = float(step[0]['price']) * float(step[0]['amount'])
                timestamp += 1
                tf = Trade.from_dict(step[0])
                case[s] = (tf, step[1])
                i += 1

        for i, case in enumerate(cases):
            print(f"###   Trade Case {t}_{i}   ###")
            await run_cex_trade_case(tracker, case, bot)
            tracker._reset_state()
    await db.close()

async def run_cex_trade_case(t: Tracker, tr_evs: list[(Trade, dict)], bot: Bot):
    """
    Takes a tracker and a list of tuples (Trade, event), inserts trades
    and checks for events. Then removes trades in reverse and checks for no events.
    """
    chan = t.tracker_event_chan
    prev_tr = None
    for i, step in enumerate(tr_evs):
        print(f"######   Step {i}   ######")
        tr, ev = step[0], step[1]
        prev_tr = tr
        print(tr)
        await t.db.insert_trades([tr])
        await t.check_cex_events()
        ev = compare_trade_event(chan, ev)
        if ev:
            await bot._publish_event(ev) # to test that it doesn't crash

    print("Going backwards")
    # removing trades
    for step in tr_evs:
        tr, ev = step[0], step[1]
        await t.db._remove_trade(tr)
        await t.check_cex_events()
        compare_trade_event(chan, None)
    await t.db.store.conn.commit()

def compare_trade_event(chan: AsyncQueue, expected_event: dict | None) -> dict | None:
    full_ev, cmp_ev = None, None
    try:
        full_ev = chan.get_nowait()
        cmp_ev = deepcopy(full_ev)
        print('got_ev=', cmp_ev)
        for item in ['id']:
            try:
                del cmp_ev.__dict__[item]
            except:
                pass
            try:
                del expected_event.__dict__[item]
            except:
                pass
    except asyncio.queues.QueueEmpty:
        pass
    except Exception as e:
        print('exception: ', e)
        raise e
    assert cmp_ev == expected_event
    return full_ev
