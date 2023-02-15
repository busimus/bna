import random
import aiohttp
import asyncio
import logging
from bna.database import Database

REFRESH_PERIOD = 60

class Oracle:
    def __init__(self, db: Database, log: logging.Logger):
        self.db = db
        self.log = log.getChild("OR")

    async def run(self, watch: list[dict]):
        self.log.info(f"Oracle started for coins {watch=}")
        cg_watch = ["bitcoin"]
        dexscreener_watch = []
        for addr, token in watch:
            price_id = token['price_id']
            if price_id.startswith("cg:"):
                cg_watch.append(price_id.split('cg:')[1])
            elif price_id.startswith("ds:"):
                dexscreener_watch.append(price_id.split('ds:')[1])
        cg_watch = "%2C".join(cg_watch)
        dexscreener_watch = ",".join(dexscreener_watch)
        asyncio.create_task(self.run_cg(self.db, cg_watch), name="oracle_cg")
        asyncio.create_task(self.run_dexscreener(self.db, dexscreener_watch), name="oracle_dexscreener")

    async def run_cg(self, db: Database, watch: str):
        s = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=10))
        while True:
            try:
                r = await s.get(f'https://api.coingecko.com/api/v3/simple/price?ids={watch}&vs_currencies=usd')
                prices = await r.json()
                # self.log.debug(f"CG {prices=}")
                for coin, price in prices.items():
                    self.db.prices[f"cg:{coin}"] = price['usd']
                await asyncio.sleep(REFRESH_PERIOD + random.random() * 2)
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
            except Exception as e:
                self.log.error(f'Oracle refresh exception: "{e}"', exc_info=True)
                await asyncio.sleep(REFRESH_PERIOD / 2)
        await s.close()

    async def run_dexscreener(self, db: Database, watch: str):
        s = aiohttp.ClientSession(headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=10))
        while True:
            try:
                r = await s.get(f'https://api.dexscreener.com/latest/dex/pairs/bsc/{watch}')
                info = await r.json()
                # self.log.debug(f"DS {info=}")
                for pair in info['pairs']:
                    pair_addr = pair['pairAddress'].lower()
                    self.db.prices[f"ds:{pair_addr}"] = float(pair['priceUsd'])
                await asyncio.sleep(REFRESH_PERIOD + random.random() * 2)
            except asyncio.CancelledError:
                self.log.debug("Cancelled")
                break
            except Exception as e:
                self.log.error(f'Oracle refresh exception: "{e}"', exc_info=True)
                await asyncio.sleep(REFRESH_PERIOD / 2)
        await s.close()
