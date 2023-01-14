import os
import json
import time
import pytest
from datetime import datetime, timezone
from bna import init_logging
from bna.database import Database
from bna.transfer import Transfer
from bna.cex_listeners import Trade

@pytest.mark.asyncio
async def test_database():
    log = init_logging()
    os.environ['POSTGRES_CONNSTRING'] = 'postgresql://postgres:123@localhost:5432/bna_pytest'
    db = Database(log, f'/tmp/bna_pytest_conf_{int(time.time())}.json')
    await db.connect(drop_existing=True)
    tests = json.load(open("tests/db_test_items.json"))
    transfers, trades, identities = tests['transfers'], tests['trades'], tests['identities']
    await db.insert_transfers(list(map(lambda t: Transfer.from_dict(t), transfers)))
    await db.insert_trades(list(map(lambda t: Trade.from_dict(t), trades)))
    await db.insert_identities(identities)

    tfs = await db.recent_transfers(9999999999)
    assert list(map(lambda tf: tf.to_dict(), tfs)) == transfers

    trs = await db.recent_trades(datetime.min.replace(tzinfo=timezone.utc), 9999999999)
    assert list(map(lambda tr: tr.to_dict(), trs)) == trades

    ids = db.get_identities()
    assert list(ids.values()) == identities

    test_addr = '0x4779a32c42f1b323800ea62042db7cac44949207'
    test_ident = None
    for id in identities:
        if id['address'] == test_addr:
            test_ident = id
            break
    else:
        raise "Test identity not found in test data"

    assert (db.get_identity(test_addr.lower())) == test_ident
    assert (db.get_identity(test_addr.upper())) == test_ident
