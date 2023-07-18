import os
import sys
import asyncio
from decimal import getcontext
from logging import Logger
from asyncio.queues import Queue as AsyncQueue

from bna import init_logging
from bna.config import Config
from bna.database import Database
from bna.discord_bot import Bot, create_bot
from bna.bsc_listener import BscListener
from bna.idena_listener import IdenaListener
from bna.tracker import Tracker
from bna.cex_listeners import bitmart_trades, probit_trades, vitex_trades
from bna.oracle import Oracle

# TODO:
# - Contract events (built-in for old contracts and WASM ones)
# - Pool events should be edited more often than on MassPoolEvents
# - Persistence of tracker state (and others)
# - Important oracle events
# - Bridge to Idena destination decoding needs TX data, but we're getting the signer anyway?
# - Old address movement notifications
# - Moving average volume detection?
# - Active exchange address tagging?
# BUGS:
# - None (as a 10x engineer)
# - Signer field breaks MEV bots. But without signer buyer addresses would be pools
# - Plenty of memory leaks (some tagged with todo, others left as an exercise to the reader)
# - Removed logs don't remove notifications or roll back state, but there's a time buffer before finality
# - Impossible to tell which pool the node undelegated from unless you catch the TX live
# HARD-CODED:
# - No separation of transfers between chains
# - Assumption (stemming from no chain separation) that zero address is the bridge
# - Stored invites don't identify the sender and the recepient, but it's not needed anywhere
# - Mass pool event threshold is bounded by regular transfers threshold, but can be solved by state persistence
# - When fetching old TXes their values are calculated based on the current price

async def main(db: Database, bot: Bot, conf: Config, log: Logger):
    log.info("Starting tasks")
    passive = '-passive' in sys.argv
    try:
        await db.connect(drop_existing=False)
        bsc = BscListener(conf=conf.bsc, db=db, log=log)
        idna = IdenaListener(conf=conf.idena, db=db, log=log)
        oracle = Oracle(db, log)
        bsc_event_chan = AsyncQueue()
        idna_event_chan = AsyncQueue()
        trade_event_chan = AsyncQueue()
        tracker_event_chan = AsyncQueue()
        cex_log = log.getChild("CX")
        if not passive:
            asyncio.create_task(bitmart_trades(cex_log, conf.cex, db.prices, trade_event_chan), name="bitmart_trades")
            asyncio.create_task(probit_trades(cex_log, conf.cex, db.prices, trade_event_chan), name="probit_trades")
            asyncio.create_task(vitex_trades(cex_log, conf.cex, db.prices, trade_event_chan), name="vitex_trades")
            asyncio.create_task(idna.run(idna_event_chan), name="idna_run")
            asyncio.create_task(bsc.run(bsc_event_chan), name="bsc_run")
        cg_tokens = db.addrs_of_type('token', full=True)
        # log.info(f"{cg_tokens=}")
        asyncio.create_task(oracle.run(cg_tokens), name="oracle_run")
        t = Tracker(bsc=bsc_event_chan, conf=conf.tracker, bot=bot, idna=idna_event_chan,
                    trades=trade_event_chan, event_chan=tracker_event_chan, db=db, log=log)
        bot.tracker = t  # @TODO: DIRTY
        bot.bsc_listener = bsc  # @TODO: DIRTY
        bot.idena_listener = idna  # @TODO: DIRTY
        asyncio.create_task(t.run(), name="tracker_run")

        if not passive:
            log.debug("Tasks started, waiting to check for events...")
            await asyncio.sleep(5)
            await t.check_events()

        await bot.run_publisher(tracker_event_chan)
        log.info("Main stopping")
        await db.close()
    except Exception as e:
        log.error(f"Main exception: {e}", exc_info=True)
    log.info("Main loop returned")

if __name__ == '__main__':
    log = init_logging()
    log.info("Starting")
    getcontext().prec = 30  # beeg number
    if '-env' in sys.argv:
        env_file = sys.argv[sys.argv.index('-env') + 1]
        log.warning(f"Loading env file {env_file}")
        for line in open(env_file, "r").read().splitlines():
            try:
                if not line or line.startswith("#"):
                    continue
                spl = line.split("=")
                key, value = spl[0], "=".join(spl[1:])
                os.environ[key] = value
            except Exception as e:
                log.error(f"Failed to parse env file line '{line}': {e}", exc_info=True)
    # log.debug(f'{os.environ}')

    conf_file = os.environ.get('BNA_CONFIG_FILE', 'config.json')
    db = Database(log, conf_file)

    # TODO: THIS SUCKS
    bot = create_bot(db, db.conf, log)
    main_task = bot.disbot.loop.create_task(main(db, bot, db.conf, log))

    bot.disbot.run(os.environ['DISCORD_BOT_TOKEN'])
    log.info("disbot.run() stopped, stopping main() task")
    main_task.cancel()
