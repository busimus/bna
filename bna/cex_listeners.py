import random
import aiohttp
import asyncio
import datetime
from copy import deepcopy
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict

from bna.config import CexConfig

MARKET_QTRADE = "qtrade"
MARKET_HOTBIT = "hotbit"
MARKET_BITMART = "bitmart"
MARKET_VITEX = "vitex"
MARKET_PROBIT = "probit"
MARKET_BSC = "bsc"
MARKETS = {
    MARKET_HOTBIT: {
        "name": "Hotbit",
        "quote": "cg:bitcoin",
        "link": "[Hotbit](https://www.hotbit.io/exchange?symbol=IDNA_BTC)"
    },
    MARKET_BITMART: {
        "name": "BitMart",
        "quote": "cg:tether",
        "link": "[BitMart](https://www.bitmart.com/trade/en?layout=basic&symbol=IDNA_USDT)"
    },
    MARKET_BSC: {
        "name": "BSC",
        "quote": "cg:tether",
        "link": "[BSC](https://app.1inch.io/#/56/unified/swap/0x0de08c1abe5fb86dd7fd2ac90400ace305138d5b/BUSD)",
    },
    MARKET_VITEX: {
        "name": "ViteX",
        "quote": "cg:bitcoin",
        "link": "[ViteX](https://x.vite.net/trade?symbol=IDNA-000_BTC-000)"
    },
    MARKET_PROBIT: {
        "name": "ProBit",
        "quote": "cg:bitcoin",
        "link": "[ProBit](https://www.probit.com/app/exchange/IDNA-BTC)"
    },
    MARKET_QTRADE: {
        "name": "qTrade",
        "quote": "cg:bitcoin",
        "link": "[qTrade](https://qtrade.io/market/IDNA_BTC)"
    },
}

@dataclass
class Trade:
    id: str
    market: str
    timeStamp: datetime
    amount: Decimal
    price: Decimal
    usd_value: float
    quote: str
    buy: bool

    def from_hotbit(trade, quote_price: float):
        trade['amount'] = Decimal(trade['amount'])
        trade['price'] = Decimal(trade['price'])
        trade['buy'] = trade['type'] == 'buy'
        trade['timeStamp'] = datetime.fromtimestamp(trade['time'], tz=timezone.utc)
        trade = {k: trade[k] for k in trade if k in Trade.__match_args__}
        trade['market'] = MARKET_HOTBIT
        trade['quote'] = MARKETS[MARKET_HOTBIT]['quote']
        trade['usd_value'] = float(trade['amount'] * trade['price'] * Decimal(quote_price))
        return Trade(**trade)

    def from_bitmart(trade, quote_price: float):
        trade['id'] = int(trade['order_time'])  # shrug
        trade['amount'] = Decimal(trade['count'])
        trade['price'] = Decimal(trade['price'])
        trade['buy'] = trade['type'] == 'buy'
        trade['timeStamp'] = int(trade['order_time'] / 1000)
        trade['timeStamp'] = datetime.fromtimestamp(trade['order_time'] / 1000, tz=timezone.utc)
        trade = {k: trade[k] for k in trade if k in Trade.__match_args__}
        trade['market'] = MARKET_BITMART
        trade['quote'] = MARKETS[MARKET_BITMART]['quote']
        trade['usd_value'] = float(trade['amount'] * trade['price'] * Decimal(quote_price))
        return Trade(**trade)

    def from_vitex(trade, quote_price: float):
        trade['id'] = int(trade['timestamp'])  # shrug
        trade['amount'] = Decimal(trade['amount'])
        trade['price'] = Decimal(trade['price'])
        trade['buy'] = trade['side'] == 0
        trade['timeStamp'] = int(trade['timestamp'] / 1000)
        trade['timeStamp'] = datetime.fromtimestamp(trade['timestamp'] / 1000, tz=timezone.utc)
        trade = {k: trade[k] for k in trade if k in Trade.__match_args__}
        trade['market'] = MARKET_VITEX
        trade['quote'] = MARKETS[MARKET_VITEX]['quote']
        trade['usd_value'] = float(trade['amount'] * trade['price'] * Decimal(quote_price))
        return Trade(**trade)

    def from_probit(trade, quote_price: float):
        trade['id'] = int(trade['id'].split(":")[1])
        trade['amount'] = Decimal(trade['quantity'])
        trade['price'] = Decimal(trade['price'])
        trade['buy'] = trade['side'] == 'buy'
        trade['timeStamp'] = datetime.strptime(trade['time'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
        trade = {k: trade[k] for k in trade if k in Trade.__match_args__}
        trade['market'] = MARKET_PROBIT
        trade['quote'] = MARKETS[MARKET_PROBIT]['quote']
        trade['usd_value'] = float(trade['amount'] * trade['price'] * Decimal(quote_price))
        return Trade(**trade)

    # def from_qtrade(trade, quote_price: float):
    #     trade['amount'] = Decimal(trade['amount'])
    #     trade['price'] = Decimal(trade['price'])
    #     trade['timeStamp'] = int(datetime.datetime.strptime(trade['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).timestamp())
    #     trade['buy'] = trade['side'] == 'buy'
    #     trade = {k: trade[k] for k in trade if k in Trade.__match_args__}
    #     trade['market'] = MARKET_QTRADE
    #     trade['quote'] = MARKETS[MARKET_QTRADE]['quote']
    #     trade['usd_value'] = float(trade['amount'] * trade['price'] * Decimal(quote_price))
    #     return Trade(**trade)

    def from_dict(d: dict):
        d = deepcopy(d)
        if 'base' in d:
            d['quote'] = d['base']
            del d['base']
        d['quote'] = d['quote']
        d['amount'] = Decimal(d['amount'])
        d['price'] = Decimal(d['price'])
        d['timeStamp'] = datetime.fromtimestamp(d['timeStamp'], tz=timezone.utc)
        if 'usd_value' not in d:
            d['usd_value'] = 0
        return Trade(**d)

    def to_dict(self):
        d = asdict(self)
        d['amount'] = str(d['amount'])
        d['price'] = str(d['price'])
        d['timeStamp'] = d['timeStamp'].timestamp()
        return d

# Hotbit trades result:
# {"error":null,"result":[
#   {"id":5143035131,"time":1658497520,"price":"0.00000151","amount":"742.76","type":"buy"},
#   {"id":5143033788,"time":1658497483,"price":"0.00000154","amount":"726.88","type":"sell"}]}

# BitMart trades result:
# {"message":"OK","code":1000,"trace":"7cfd180a-357b-406c-98cb-30ac9ac3b7b8","data":{"trades":[
#   {"amount":"32.505743","order_time":1658497856533,"price":"0.035307","count":"920.66","type":"buy"},
#   {"amount":"42.054249","order_time":1658497840425,"price":"0.035260","count":"1192.69","type":"buy"}]}}

# ViteX trades result:
# {"code":0,"msg":"ok","data":[{"timestamp":1672361525000,"price":"0.00000051","amount":"535.82320000","side":1},{"timestamp":1671230363000,"price":"0.00000125","amount":"1066.68880000","side":0}]}

# Probit trades result:
# {"data": [{"id": "IDNA-BTC:319985", "price": "0.0000007077", "quantity": "961.2376",
#            "time": "2023-01-03T14:16:43.908Z", "side": "buy", "tick_direction": "down"}]}

# qTrade trades result:
# {"data":{"trades": [
#   {"id":497652,"amount":"50","price":"0.00000206","base_volume":"0.000103","seller_taker":true,
#    "side":"sell","created_at":"2022-01-27T12:01:24.177168Z","created_at_ts":1643284884177168},
#   {"id":497222,"amount":"100","price":"0.00000224","base_volume":"0.000224","seller_taker":false,
#    "side":"buy","created_at":"2022-01-24T11:58:39.879154Z","created_at_ts":1643025519879154}]}


async def hotbit_trades(log, conf: CexConfig, prices: dict, event_chan):
    log = log.getChild('HT')
    log.info('Hotbit trade listener started')
    s = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=10))
    last_trade_id = 1
    while True:
        try:
            await asyncio.sleep(conf.interval + random.random())
            r = await s.get(f'https://api.hotbit.io/api/v1/market.deals?market=IDNA/BTC&limit=100&last_id={last_trade_id}')
            trades = (await r.json(content_type=None))['result']
            if trades and len(trades) > 0:
                quote_price = prices[MARKETS[MARKET_HOTBIT]['quote']]
                trades = list(map(lambda t: Trade.from_hotbit(t, quote_price), trades))
                event_chan.put_nowait(trades)
                last_trade_id = trades[0].id
        except asyncio.CancelledError:
            log.debug("Cancelled")
            break
        except Exception as e:
            log.error(f'Hotbit exception: "{e}"', exc_info=True)
            await asyncio.sleep(conf.interval)
    await s.close()

async def vitex_trades(log, conf: CexConfig, prices: dict, event_chan):
    log = log.getChild('VX')
    log.info('ViteX trade listener started')
    s = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=10))
    last_trade_time = 0
    while True:
        try:
            await asyncio.sleep(conf.interval * 3 + random.random())
            r = await s.get('https://api.vitex.net/api/v2/trades?symbol=IDNA-000_BTC-000&limit=100')
            assert r.status == 200
            raw_trades = (await r.json(content_type=None))['data']
            quote_price = prices[MARKETS[MARKET_VITEX]['quote']]
            trades = [Trade.from_vitex(t, quote_price) for t in raw_trades if t['timestamp'] > last_trade_time]
            if trades and len(trades) > 0:
                event_chan.put_nowait(trades)
                last_trade_time = raw_trades[0]['timestamp']
        except asyncio.CancelledError:
            log.debug("Cancelled")
            break
        except Exception as e:
            log.error(f'ViteX exception: "{e}"', exc_info=False)
            await asyncio.sleep(conf.interval)
    await s.close()

async def probit_trades(log, conf: CexConfig, prices: dict, event_chan):
    log = log.getChild('PB')
    log.info('ProBit trade listener started')
    s = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=10))
    start_time = datetime.utcnow() - timedelta(days=7)
    start_time = start_time.isoformat(timespec='milliseconds')
    resp = None
    while True:
        try:
            await asyncio.sleep(conf.interval + random.random())
            url = f'https://api.probit.com/api/exchange/v1/trade?market_id=IDNA-BTC&start_time={start_time}Z&end_time=9999-12-21T03:00:00.000Z&limit=1000'
            r = await s.get(url)
            resp = await r.json(content_type=None)
            trades = resp['data']
            if trades and len(trades) > 0:
                quote_price = prices[MARKETS[MARKET_HOTBIT]['quote']]
                trades = list(map(lambda t: Trade.from_probit(t, quote_price), trades))
                event_chan.put_nowait(trades)
                start_time = (trades[0].timeStamp.replace(tzinfo=None) + timedelta(microseconds=1000)).isoformat(timespec='milliseconds')
        except asyncio.CancelledError:
            log.debug("Cancelled")
            break
        except Exception as e:
            log.error(f'ProBit exception: "{e}"', exc_info=True)
            log.debug(f"{resp=}")
            await asyncio.sleep(conf.interval)
    await s.close()

async def bitmart_trades(log, conf: CexConfig, prices: dict, event_chan):
    log = log.getChild('BT')
    log.info('BitMart trade listener started')
    s = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=10))
    last_trade_id = 1
    while True:
        try:
            await asyncio.sleep(conf.interval + random.random())
            r = await s.get('https://api-cloud.bitmart.com/spot/v1/symbols/trades?symbol=IDNA_USDT')
            trades = (await r.json())['data']['trades']
            if trades and len(trades) > 0:
                quote_price = prices[MARKETS[MARKET_BITMART]['quote']]
                trades = list(map(lambda t: Trade.from_bitmart(t, quote_price),
                           filter(lambda t: t['order_time'] > last_trade_id, trades)))
                if len(trades) == 0:
                    continue
                event_chan.put_nowait(trades)
                last_trade_id = trades[0].id
        except asyncio.CancelledError:
            log.debug("Cancelled")
            break
        except Exception as e:
            log.error(f'BitMart exception: "{e}"', exc_info=True)
            await asyncio.sleep(conf.interval)
    await s.close()


# async def qtrade_trades(log, conf: CexConfig, prices: dict, event_chan):
#     log = log.getChild('QT')
#     log.info('qTrade trade listener started')
#     s = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=10))
#     last_trade_id = 1
#     while True:
#         try:
#             await asyncio.sleep(conf.interval + random.random())
#             r = await s.get(f'https://api.qtrade.io/v1/market/IDNA_BTC/trades?newer_than={last_trade_id}')
#             trades = (await r.json())['data']['trades']
#             if len(trades) > 0:
#                 quote_price = prices[MARKETS[MARKET_QTRADE]['quote']]
#                 trades = list(map(lambda t: Trade.from_qtrade(t, quote_price), trades))
#                 event_chan.put_nowait(trades)
#                 last_trade_id = trades[0].id
#         except asyncio.CancelledError:
#             log.debug("Cancelled")
#             break
#         except Exception as e:
#             log.error(f'qTrade exception: "{e}"', exc_info=True)
#             await asyncio.sleep(conf.interval)
#     await s.close()
