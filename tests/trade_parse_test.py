import pytest
import aiohttp
from bna.cex_listeners import Trade

# q_case = {"data":{"trades": [
#   {"id":497652,"amount":"50","price":"0.00000206","base_volume":"0.000103","seller_taker":True,
#    "side":"sell","created_at":"2022-01-27T12:01:24.177168Z","created_at_ts":1643284884177168},
#   {"id":497222,"amount":"100","price":"0.00000224","base_volume":"0.000224","seller_taker":False,
#    "side":"buy","created_at":"2022-01-24T11:58:39.879154Z","created_at_ts":1643025519879154}]}}

# h_case = {"error":None, "result":[
#   {"id":5143035131,"time":1658497520,"price":"0.00000151","amount":"742.76","type":"buy"},
#   {"id":5143033788,"time":1658497483,"price":"0.00000154","amount":"726.88","type":"sell"}]}

# b_case = {"message":"OK","code":1000,"trace":"7cfd180a-357b-406c-98cb-30ac9ac3b7b8","data":{"trades":[
#   {"amount":"32.505743","order_time":1658497856533,"price":"0.035307","count":"920.66","type":"buy"},
#   {"amount":"42.054249","order_time":1658497840425,"price":"0.035260","count":"1192.69","type":"buy"}]}}

@pytest.mark.asyncio
async def test_trade_parse():
    s = aiohttp.ClientSession()
    prices = {'cg:bitcoin': 17500, 'cg:idena': 0.02, 'cg:binancecoin': 270, 'cg:binance-usd': 1, 'cg:tether': 1}
    vxt = await (await s.get("https://api.vitex.net/api/v2/trades?symbol=IDNA-000_BTC-000&limit=100")).json(content_type=None)
    for trade in vxt['data']:
        t = Trade.from_vitex(trade, prices['cg:bitcoin'])
    pbt = await (await s.get("https://api.probit.com/api/exchange/v1/trade?market_id=IDNA-USDT&start_time=2023-07-17T16:31:23.087Z&end_time=9999-12-21T03:00:00.000Z&limit=1000")).json(content_type=None)
    for trade in pbt['data']:
        t = Trade.from_probit(trade, prices['cg:bitcoin'])
    bts = await (await s.get("https://api-cloud.bitmart.com/spot/v1/symbols/trades?symbol=IDNA_USDT")).json()
    for trade in bts['data']['trades']:
        t = Trade.from_bitmart(trade, prices['cg:tether'])
    await s.close()
