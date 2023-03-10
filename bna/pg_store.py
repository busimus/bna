import json
import asyncio
import psycopg
import datetime
from bna.transfer import Transfer
from bna.cex_listeners import Trade


class PgStore:
    "PostgreSQL backing store for transfers, trades, and identities"
    def __init__(self, conninfo, log):
        self.log = log.getChild("PG")
        self.conninfo = conninfo
        self.dbname = psycopg.conninfo.conninfo_to_dict(conninfo).get('dbname', 'bna')
        self.conn = None

    async def connect(self, drop_existing=False):
        self.log.debug(f"{self.conninfo=}")
        # First connect without a database to create it
        no_db = psycopg.conninfo.conninfo_to_dict(self.conninfo)
        if 'dbname' in no_db:
            del no_db['dbname']
        no_db_str = psycopg.conninfo.make_conninfo(**no_db)
        while True:
            try:
                self.log.debug(f"Connecting to {no_db_str}")
                self.conn = await psycopg.AsyncConnection.connect(no_db_str, autocommit=True)
                break
            except Exception as e:
                self.log.error(f"Conn exc: '{e}'", exc_info=True)
                await asyncio.sleep(2)
        try:
            self.log.info(f'Trying to creating database "{self.dbname}"')
            await self.conn.execute(f"create database {self.dbname}")
        except psycopg.errors.DuplicateDatabase as e:
            self.log.debug("Database exists")
            if drop_existing:
                self.log.debug("Dropping existing database")
                await self.conn.execute(f'SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid();', (self.dbname,))
                await self.conn.execute(f'drop database {self.dbname}')
                await self.conn.execute(f'create database {self.dbname}')
                await self.conn.commit()
        self.log.debug(f"Connecting to {self.conninfo}")
        self.conn = await psycopg.AsyncConnection.connect(self.conninfo, autocommit=False)
        await self.conn.execute(open("bna/sql/create_tables.sql", 'r').read())
        await self.conn.commit()

    async def get_transfers(self, after: datetime.datetime, until=datetime.datetime.max) -> dict[(int, int), Transfer]:
        rows = await (await self.conn.execute('SELECT * from public."Transfers" WHERE time > (%s) AND time < (%s)', (after, until))).fetchall()
        return dict(map(lambda r: ((r[0], r[1]), Transfer.from_dict(r[3])), rows))

    async def get_transfers_by_hash(self, hashes: list[str]) -> list[Transfer]:
        if len(hashes) == 0:
            return []
        placeholders = ', '.join(['%s'] * len(hashes))
        query = f'SELECT * FROM public."Transfers" WHERE data->>\'hash\' IN ({placeholders})'
        rows = await (await self.conn.execute(query, tuple(hashes))).fetchall()
        hashes = {h: None for h in hashes}
        for row in rows:
            tf = Transfer.from_dict(row[3])
            hashes[tf.hash] = tf
        return list(hashes.values())

    async def get_trades(self, after: datetime.datetime, until=datetime.datetime.max) -> dict[(int, str), Trade]:
        rows = await (await self.conn.execute('SELECT * from public."Trades" WHERE time > (%s) AND time < (%s)', (after, until))).fetchall()
        return dict(map(lambda r: ((r[0], r[1]), Trade.from_dict(r[3])), rows))

    async def get_identity(self, addr: str) -> dict:
        row = await (await self.conn.execute('SELECT * from public."Identities" WHERE address = (%s)', (addr.lower(),))).fetchone()
        return row[2]

    async def get_identities(self) -> dict[str, dict]:
        rows = await (await self.conn.execute('SELECT * from public."Identities"')).fetchall()
        return dict(map(lambda r: (r[0], r[2]), rows))

    async def get_latest_block(self, chain: str) -> int:
        tf = await (await self.conn.execute(f'select (data) from public."Transfers" where data ->> \'chain\' = %s order by "time" desc limit 1', (chain,))).fetchone()
        if tf is None:
            return None
        return tf[0]['blockNumber']

    async def get_event(self, ev_id):
        row = await (await self.conn.execute('SELECT (channel, message, event) from public."Events" WHERE id = (%s)', (ev_id,))).fetchone()
        if not row:
            self.log.warning(f"Event not found")
            return None, None, None
        row = row[0]
        ev = ev if type(row[2]) == dict else json.loads(row[2])  # why not done automatically?
        return row[0], row[1], ev

    async def insert_transfers(self, tfs: list[Transfer]):
        cur = self.conn.cursor()
        seq = map(lambda tf: (tf.blockNumber, tf.logIndex, tf.timeStamp,
                              json.dumps(tf.to_dict())), tfs)
        await cur.executemany(\
            """
INSERT INTO public."Transfers" (blocknum, logindex, time, data)
VALUES (%s, %s, %s, %s)
ON CONFLICT (blocknum, logindex) DO UPDATE
  SET time = excluded.time,
      data = excluded.data;
            """, seq)
        await self.conn.commit()

    async def insert_trades(self, trs: list[Trade]):
        cur = self.conn.cursor()
        seq = map(lambda tr: (tr.id, tr.market, tr.timeStamp,
                              json.dumps(tr.to_dict())), trs)
        await cur.executemany(\
            """
INSERT INTO public."Trades" (id, market, time, data)
VALUES (%s, %s, %s, %s)
ON CONFLICT (id, market) DO UPDATE
  SET time = excluded.time,
      data = excluded.data;
            """, seq)
        await self.conn.commit()

    async def insert_identities(self, idents: list[dict], full=False):
        cur = self.conn.cursor()
        seq = map(lambda ident: (ident['address'].lower(), datetime.datetime.fromtimestamp(ident['_fetchTime'], tz=datetime.timezone.utc),
                              json.dumps(ident)), idents)

        if full:
            self.log.debug("Dropping identities")
            await cur.execute('DELETE FROM public."Identities";')

        await cur.executemany("""
INSERT INTO public."Identities" (address, fetch_time, data)
VALUES (%s, %s, %s)
ON CONFLICT (address) DO UPDATE
  SET fetch_time = excluded.fetch_time,
      data = excluded.data;
        """, seq)
        await self.conn.commit()

    async def insert_event(self, ev_dict: dict, chan_id: int, msg_id: int):
        cur = self.conn.cursor()

        await cur.execute("""
INSERT INTO public."Events" (id, channel, message, event)
VALUES (%s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE
  SET event = excluded.event,
      channel = excluded.channel,
      message = excluded.message;
        """, (ev_dict['id'], chan_id, msg_id, json.dumps(ev_dict)))
        await self.conn.commit()

    async def close(self):
        await self.conn.close()

    async def _remove_transfer(self, tf: Transfer):
        await self.conn.execute('DELETE FROM public."Transfers" WHERE blocknum=%s AND logindex=%s', (tf.blockNumber, tf.logIndex))

    async def _remove_trade(self, tr: Trade):
        await self.conn.execute('DELETE FROM public."Trades" WHERE id=%s AND market=%s', (tr.id, tr.market))
