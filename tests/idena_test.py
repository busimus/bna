import os
import time
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta

from bna import init_logging
from bna.transfer import Transfer
from bna.database import Database
from bna.idena_listener import IdenaListener

@pytest.mark.asyncio
async def test_fetch_and_process_block():
    table = {
        5307151: [
            {"hash": "0x48f28aa41c83d81c947dbf824cd14160cc1f8faa14683be4169ccbe8dc36b642",
             "meta": {"pool": '0x9a0ba21324db763aa1e53be61d742a8132d61649', 'usd_value': 0},
             "tags": ["delegate"], "chain": "idena",
             "changes": {'0xa7d4e6e927c4310bbd323ed90225c18e0b9f9805': "0"},
             "logIndex": 0, "timeStamp": 1670510248, "blockNumber": 5307151},
        ],
        5315556: [
            {"hash": "0x15cf6cd731f400b41a91f11bec1f6595ee6d206cb59e7a97d85fe8b4ac0f5bea",
             "meta": {'usd_value': 24}, "tags": ["send"], "chain": "idena",
             "changes": {"0x1b0cbe67dbecc16b2fff61de0c98670a24bc0ce5": "-24",
                         "0x664d30b74a1876d8f846a00e56916129229cd1b4": "24"},
             "logIndex": 2, "timeStamp": 1670676763, "blockNumber": 5315556},
            {"hash": "0xfed36978f6eb12ab57f437a2123adeb8fb37b9e7c23d309939a06c8c99d06173",
             "meta": {'usd_value': 18}, "tags": ["replenishStake"], "chain": "idena",
             "changes": {"0x9a480ac094cd4579d9c5f4994217b1572c22937a": "-18"},
             "logIndex": 1, "timeStamp": 1670676763, "blockNumber": 5315556},
            {"hash": "0xc85b1de7125d3db27ce12a4a5b609cff66ed1d92cef9efae62cab6e977b3d3fc",
             "meta": {'usd_value': 0}, "tags": ["invite"], "chain": "idena",
             "changes": {"0x49ca3324d96c16618f2e2c64f04df4180a968f8c": "0",
                         "0x2b082d6e13f2e58df7791e4a94ebf3c7c56e8ba4": "0"},
             "logIndex": 0, "timeStamp": 1670676763, "blockNumber": 5315556}
        ]
    }
    log = init_logging()
    os.environ['POSTGRES_CONNSTRING'] = 'postgresql://postgres:123@localhost:5432/bna_pytest'
    db = Database(log, f'/tmp/bna_pytest_conf_{int(time.time())}.json')
    db.prices['cg:idena'] = 1
    await db.connect(True)
    conf = db.get_config()
    i = IdenaListener(conf=conf.idena, log=log, db=db)
    for block_num, expected_transfers in table.items():
        block = await i.rpc_req("bcn_blockAt", [block_num])
        trs = await i.process_block(block)
        assert len(trs) == len(expected_transfers)
        trs.sort(key=lambda tr: tr.hash)
        expected_transfers.sort(key=lambda trd: trd['hash'])

        for tr, expected_tr in zip(trs, expected_transfers):
            tr = tr.to_dict()
            for key, expected_value in expected_tr.items():
                print(tr, key)
                assert tr[key] == expected_value

# @TODO: More cases
@pytest.mark.asyncio
async def test_process_tx():
    table = [
        (   # KillTx with pool
            {
                "hash": "0xbf2b1539de425375f3c4191c64638caf3b505aa9ac15ca1276425a1a9a5d9766",
                "type": "kill",
                "from": "0xf85921efdc17c54dc7ebbf01c451e76f0fbd216c",
                "to": None,
                "amount": "0",
                "tips": "0",
                "maxFee": "0",
                "nonce": 1,
                "epoch": 97,
                "payload": "0x",
                "blockHash": "0xcdb983b87a5bdc0a6caa01d02c7a149290df864a42228725b55a29489f470168",
                "usedFee": "0",
                "timestamp": 1670626028
            },
            Transfer(hash="0xbf2b1539de425375f3c4191c64638caf3b505aa9ac15ca1276425a1a9a5d9766",
                meta={
                    "killedIdentity": "0xf85921efdc17c54dc7ebbf01c451e76f0fbd216c",
                    "age": 20,
                    "pool": "0xddddaddb856901ac3e2251b8234efeab2188b22a",
                    "usd_value": 1581.545067883599849852
                },
                tags=["kill"],
                chain="idena",
                signer="0xf85921efdc17c54dc7ebbf01c451e76f0fbd216c",
                changes={
                    "0xf85921efdc17c54dc7ebbf01c451e76f0fbd216c": Decimal("1581.545067883599849852")
                },
                logIndex=48,
                timeStamp=datetime.fromtimestamp(1670626028, tz=timezone.utc),
                blockNumber=5312992
            )
        ),
        (   # KillDelegatorTx
            {
                "hash": "0xbf2b1539de425375f3c4191c64638caf3b505aa9ac15ca1276425a1a9a5d9766",
                "type": "killDelegator",
                "from": "0xddddaddb856901ac3e2251b8234efeab2188b22a",
                "to": "0xf85921efdc17c54dc7ebbf01c451e76f0fbd216c",
                "amount": "0",
                "tips": "0",
                "maxFee": "0",
                "nonce": 1,
                "epoch": 97,
                "payload": "0x",
                "blockHash": "0xcdb983b87a5bdc0a6caa01d02c7a149290df864a42228725b55a29489f470168",
                "usedFee": "0",
                "timestamp": 1670626028
            },
            Transfer(hash="0xbf2b1539de425375f3c4191c64638caf3b505aa9ac15ca1276425a1a9a5d9766",
                meta={
                    "killedIdentity": "0xf85921efdc17c54dc7ebbf01c451e76f0fbd216c",
                    "age": 20,
                    "pool": "0xddddaddb856901ac3e2251b8234efeab2188b22a",
                    "usd_value": 1581.545067883599849852
                },
                tags=["kill"],
                chain="idena",
                signer="0xddddaddb856901ac3e2251b8234efeab2188b22a",
                changes={
                    "0xddddaddb856901ac3e2251b8234efeab2188b22a": Decimal("1581.545067883599849852")
                },
                logIndex=49,
                timeStamp=datetime.fromtimestamp(1670626028, tz=timezone.utc),
                blockNumber=5312992
            )
        )
    ]

    log = init_logging()
    os.environ['POSTGRES_CONNSTRING'] = 'postgresql://postgres:123@localhost:5432/bna_pytest'
    db = Database(log, f'/tmp/bna_pytest_conf_{int(time.time())}.json')
    await db.connect(True)
    conf = db.get_config()
    db.cache['identities']['0xf85921efdc17c54dc7ebbf01c451e76f0fbd216c'] = {'stake': "1581.545067883599849852", "delegatee": "0xddddaddb856901ac3e2251b8234efeab2188b22a", "age": 20, "_fetchTime": 999999999999999, "address": "0xf85921efdc17c54dc7ebbf01c451e76f0fbd216c"}
    db.prices['cg:idena'] = 1
    i = IdenaListener(conf=conf.idena, log=log, db=db)
    for tx, expected_transfer in table:
        tr = await i.process_tx(tx, expected_transfer.blockNumber, expected_transfer.logIndex)

        tr = tr.to_dict()
        for key, expected_value in expected_transfer.to_dict().items():
            print(f"{tr[key]} == {expected_value}")
            assert tr[key] == expected_value
