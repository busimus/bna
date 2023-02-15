import os
import json
import pytest
from bna.tags import *
from bna.event import *
from bna import init_logging
from bna.transfer import Transfer
from bna.database import Database

from tests.tr_events import *

pairs = [
    ('{"type": "event", "id": 123123123123123}', Event(id=123123123123123)),
]

async def get_env():
    log = init_logging()
    os.environ['POSTGRES_CONNSTRING'] = 'postgresql://bna:bna@localhost:15432/bna_pytest'
    db = Database(log, f'/tmp/bna_pytest_conf_{int(time.time())}.json')
    await db.connect(drop_existing=True)
    tests = json.load(open("tests/db_test_items.json"))
    tf_dicts = tests['transfers']
    await db.insert_transfers(list(map(lambda td: Transfer.from_dict(td), tf_dicts)))
    by_tag = defaultdict(list)
    for td in tf_dicts:
        for tag in td.get('tags', []):
            by_tag[tag].append(td)
    return log, db, tf_dicts, by_tag

@pytest.mark.asyncio
async def test_kill_event():
    log, db, tf_dicts, by_tag = await get_env()
    kptf = Transfer.from_dict(json.loads(kill_pooled_tf_json))
    assert json.dumps(kptf.to_dict()) == kill_pooled_tf_json
    await db.insert_transfers([kptf])

    kpev = await KillEvent.from_dict(json.loads(kill_pooled_event_json), db=db)
    assert kpev.to_dict() == kill_pooled_event_dict
    assert json.dumps(kpev.to_dict()) == kill_pooled_event_json
    assert kpev == await event_from_dict(json.loads(kill_pooled_event_json), db=db)
    kpev.id = 123456
    kill_pooled_event.id = 123456
    assert kpev == kill_pooled_event

    kpev = KillEvent.from_tf(kptf)
    kpev.id = 123456
    assert kpev == kill_pooled_event

    await db.insert_transfers([kill_delegator_tf])
    kdev = KillEvent.from_tf(kill_delegator_tf)
    kdev.id = 123456
    kill_delegator_event.id = 123456
    assert kdev == kill_delegator_event

@pytest.mark.asyncio
async def test_mass_pool_event():
    log, db, tf_dicts, by_tag = await get_env()
    await db.insert_transfers([mpe1_tf1, mpe1_tf2, mpe2_tf1, mpe2_tf2])
    # Testing PoolEvent creation from TF
    ev1_1 = PoolEvent.from_tf(mpe1_tf1, subtype=IDENA_TAG_KILL, age=mpe1_tf1.meta['age'], stake=mpe1_tf1.value(recv=True))
    ev1_2 = PoolEvent.from_tf(mpe1_tf2, subtype=IDENA_TAG_KILL, age=mpe1_tf2.meta['age'], stake=mpe1_tf2.value(recv=True))
    ev2_1 = PoolEvent.from_tf(mpe2_tf1, subtype=IDENA_TAG_KILL, age=mpe2_tf1.meta['age'], stake=mpe2_tf1.value(recv=True))
    ev2_2 = PoolEvent.from_tf(mpe2_tf2, subtype=IDENA_TAG_KILL, age=mpe2_tf2.meta['age'], stake=mpe2_tf2.value(recv=True))
    ev1_1.id = 123456
    mpe1_pe1.id = 123456
    ev1_2.id = 123456
    mpe1_pe2.id = 123456
    ev2_1.id = 123456
    mpe2_pe1.id = 123456
    ev2_2.id = 123456
    mpe2_pe2.id = 123456

    assert ev1_1 == mpe1_pe1
    assert ev1_2 == mpe1_pe2
    assert ev2_1 == mpe2_pe1
    assert ev2_2 == mpe2_pe2

    # Testing PoolEvent saving/loading from JSON
    assert json.dumps(ev1_1.to_dict()) == mpe1_pe1_json
    ev1_1_j = await PoolEvent.from_dict(json.loads(mpe1_pe1_json), db=db)
    ev1_1_j.id = ev1_1.id
    assert ev1_1 == ev1_1_j
    # assert ev1_1 == await event_from_dict(json.loads(mpe1_pe1_json), db=db)

    # Testing MassPoolEvent creation and saving/loading from JSON
    mev1 = MassPoolEvent(subtype=IDENA_TAG_KILL, pool=ev1_1.pool, count=2, stake=ev1_1.stake + ev1_2.stake, age=ev1_1.age + ev1_2.age, changes=[ev1_1, ev1_2])
    mev1.id = 456
    mass_pool_event_1.id = 456

    assert mev1 == mass_pool_event_1
    assert json.dumps(mev1.to_dict()) == mass_pool_event_1_json
    mpe1_j = await MassPoolEvent.from_dict(json.loads(mass_pool_event_1_json), db=db)
    mpe1_j.id = mev1.id
    assert mpe1_j == mev1

    mev2 = MassPoolEvent(subtype=IDENA_TAG_KILL, pool=ev2_1.pool, count=2, stake=ev2_1.stake + ev2_2.stake, age=ev2_1.age + ev2_2.age, changes=[ev2_1, ev2_2])
    mev2.id = 789
    mass_pool_event_2.id = 789

    assert mev2 == mass_pool_event_2
    assert json.dumps(mev2.to_dict()) == mass_pool_event_2_json
    mpe2_j = await MassPoolEvent.from_dict(json.loads(mass_pool_event_2_json), db=db)
    mpe2_j.id = mev2.id
    assert mpe2_j == mev2

    # Testing MassPoolEvent joining
    assert mev1.can_join(mev2)
    assert mev2.can_join(mev1)

    cmev1 = deepcopy(mev1)
    cmev1.join(mev2)

    cmev2 = deepcopy(mev2)
    cmev2.join(mev1)
    cmev1.id = cmev2.id
    cmev1.changes.sort(key=lambda ev: ev.stake)
    cmev2.changes.sort(key=lambda ev: ev.stake)

    assert cmev1 == cmev2
    mass_pool_joined_event.id = cmev1.id
    for ch in mass_pool_joined_event.changes:
        ch.id = ev1_1.id
    assert cmev1 == mass_pool_joined_event

    mpej = await MassPoolEvent.from_dict(json.loads(mass_pool_joined_json), db)
    for ch in mpej.changes:
        ch.id = ev1_1.id
    mpej.id = cmev1.id
    assert mpej == cmev1

@pytest.mark.asyncio
async def test_top_event():
    log, db, tf_dicts, by_tag = await get_env()

    # Test TopKillEvent creation and saving/loading from JSON
    ktf = Transfer.from_dict(json.loads(kill_pooled_tf_json))
    await db.insert_transfers([ktf, kill_delegator_tf])

    total_idna = ktf.value(recv=True) + kill_delegator_tf.value(recv=True)
    kev = TopKillEvent(period=123, total_usd_value=float(total_idna), total_idna=total_idna, total_age=ktf.meta['age'] + kill_delegator_tf.meta['age'])
    kev.items = [ktf, kill_delegator_tf]
    kev.id = 456

    assert json.dumps(kev.to_dict()) == top_kill_json
    assert kev == await TopKillEvent.from_dict(json.loads(top_kill_json), db)

    # Test TopStakeEvent creation and saving/loading from JSON
    sev = TopStakeEvent(period=3600, total_usd_value=sum([i['stake_usd'] for i in top_stake_idents]), total_idna=sum([i['stake'] for i in top_stake_idents]))
    sev.items = top_stake_idents
    sev.id = 567

    assert json.dumps(sev.to_dict()) == top_stake_json
    assert sev == await TopStakeEvent.from_dict(json.loads(top_stake_json), db)


@pytest.mark.asyncio
async def test_dex_event():
    log, db, tf_dicts, by_tag = await get_env()

    # Test DexEvent creation and saving/loading from JSON
    await db.insert_transfers([dex_arb_tf, dex_buy_tf, dex_sell_tf])

    dev = await DexEvent.from_dict(json.loads(dex_json), db)
    assert json.dumps(dev.to_dict()) == dex_json
    tfdev = DexEvent.from_tfs([dex_arb_tf, dex_buy_tf, dex_sell_tf], db.known)
    tfdev.id = dev.id
    assert dev == tfdev


@pytest.mark.asyncio
async def test_stats_event():
    log, db, tf_dicts, by_tag = await get_env()
    stats_dict = json.loads(stats_json)
    ev = StatsEvent.from_dict(stats_dict)
    assert ev.to_dict() == stats_dict
    assert json.dumps(ev.to_dict()) == stats_json

@pytest.mark.asyncio
async def test_event_parse():
    log, db, tf_dicts, by_tag = await get_env()

    for pair in pairs:
        ev_dict = json.loads(pair[0])
        ev_orig = pair[1]
        ev_parsed = await event_from_dict(ev_dict, db=db)
        assert ev_parsed.__dict__ == ev_orig.__dict__
        assert pair[0] == json.dumps(ev_parsed.to_dict())

    for tf_dict in tf_dicts:
        tf = Transfer.from_dict(tf_dict)
        ev_orig = TransferEvent(time=tf.timeStamp, tfs=[tf], amount=tf.value(), by=tf.signer)
        ev_dict = ev_orig.to_dict()
        ev_parsed = await TransferEvent.from_dict(ev_dict, db=db)
        log.debug(f"{ev_orig=}")
        log.debug(f"{ev_parsed=}")
        log.debug(f"{ev_dict=}")
        log.debug(f"{ev_parsed.__dict__=}")
        log.debug(f"{ev_orig.id=}, {ev_parsed.id=}")
        assert ev_orig.id != -1
        assert ev_parsed.id != -1
        assert ev_orig.id == ev_parsed.id
        assert ev_parsed.__dict__ == ev_orig.__dict__

    for tag, tf_dicts in by_tag.items():
        if tag == IDENA_TAG_KILL:
            for tf_dict in tf_dicts:
                tf = Transfer.from_dict(tf_dict)
                ev = KillEvent.from_tf(tf)
                log.debug(ev)
                assert ev.id != -1
                ev = PoolEvent.from_tf(tf=tf, subtype=IDENA_TAG_KILL)
                log.debug(ev)
                assert ev.id != -1
        elif tag in [IDENA_TAG_DELEGATE, IDENA_TAG_UNDELEGATE]:
            for tf_dict in tf_dicts:
                tf = Transfer.from_dict(tf_dict)
                ev = PoolEvent.from_tf(tf=tf, subtype=tag, age=123, stake=456)
                log.debug(ev)
                assert ev.id != -1
        elif tag == DEX_TAG:
            for tf_dict in tf_dicts:
                tf = Transfer.from_dict(tf_dict)
                ev = DexEvent.from_tfs(tfs=[tf], known_addr=db.known)
                log.debug(ev)
                assert ev.id != -1

    ev = DexEvent.from_tfs(tfs=[Transfer.from_dict(td) for td in by_tag[DEX_TAG]], known_addr=db.known)

@pytest.mark.asyncio
async def test_event_join():
    log, db, tf_dicts, by_tag = await get_env()

    buy_tf = by_tag[DEX_TAG_BUY][0]
    buy_tf = Transfer.from_dict(buy_tf)
    de = DexEvent.from_tfs(tfs=[buy_tf], known_addr=db.known)
    de_orig = DexEvent.from_tfs(tfs=[buy_tf], known_addr=db.known)
    de.join(de_orig, known_addr=db.known)
    assert de.buy_usd == de_orig.buy_usd * 2
    assert de.sell_usd == de_orig.sell_usd * 2
    assert len(de.tfs) == 2 * len(de_orig.tfs)
