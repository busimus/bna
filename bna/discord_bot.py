import os
import time
import json
import asyncio
import disnake
from logging import Logger
from functools import wraps
from datetime import datetime, timezone, timedelta
from disnake import Embed, Color
from collections import defaultdict, deque
from itertools import chain
from disnake.ext import commands

from bna.cex_listeners import MARKETS
from bna.database import Database
from bna.config import Config, DiscordBotConfig
from bna.tags import *
from bna.event import *
from bna.tracker import Tracker
from bna.transfer import CHAIN_BSC, CHAIN_IDENA, Transfer
from bna.utils import any_in, average_color, shorten, get_identity_color, trade_color

MAX_PUBLISH_ATTEMPTS = 3


class Bot:
    def __init__(self, bot: commands.InteractionBot, conf: DiscordBotConfig, db: Database, dev_user: int, log: Logger):
        from bna.bsc_listener import BscListener
        from bna.idena_listener import IdenaListener
        self.disbot = bot
        self.conf = conf
        self.db = db
        self.dev_user = dev_user
        self.tracker: Tracker = None
        self.bsc_listener: BscListener = None
        self.idena_listener: IdenaListener = None
        self.log = log.getChild('DI')
        self.stopped = False
        # For ratelimiting
        queue_size = max(16, self.conf.user_ratelimit_command_number)  # large size to allow for variable sleep length
        self.user_cmd_times = defaultdict(lambda: deque(maxlen=queue_size))  # TODO: leaks slowly
        self.activity = {'block': {CHAIN_IDENA: 0, CHAIN_BSC: 0}}
        self.last_activity_update = time.time()
        # Cache of events and messages. Items and their buttons are removed after two days
        self.ev_to_msg: dict[int, (Event, disnake.Message)] = {}
        self.event_cache_cleaned_at = datetime.now(tz=timezone.utc)

    async def run_publisher(self, tracker_event_chan: asyncio.Queue):
        self.log.info("Publisher started")
        start = datetime.now()
        while True:
            try:
                ev = await tracker_event_chan.get()
            except asyncio.CancelledError:
                break
            if datetime.now() - start < timedelta(seconds=5) and type(ev) != BlockEvent:
                self.log.warning(f"Not publishing old event: {ev}")
                import sys
                if '-old' not in sys.argv:
                    continue
            try:
                await self.publish_event(ev)
            except Exception as e:
                if type(e) == asyncio.CancelledError:
                    break
                self.log.error(f"Tracker sent bad events {ev=}: e", exc_info=True)
                pass
        self.log.debug("Cancelled")

    async def send_response(self, msg: disnake.CommandInteraction, resp: dict):
        if not msg.response.is_done():
            await msg.response.send_message(**resp)
        else:
            await msg.followup.send(**resp)
        msg_resp = await msg.original_response()
        if not msg_resp:
            self.log.warning(f"Resp to {msg} is None")
        return msg_resp

    async def ratelimit(self, msg: disnake.CommandInteraction) -> bool:
        "Log user's time of request, and ratelimit them by sleeping if needed. Returns False if the command should not be executed (like when the bot is stopped), but will still ratelimit."
        if msg.author.id == self.dev_user:
            return True
        times = self.user_cmd_times[msg.author.id]
        now = time.time()
        recent_reqs = sum([1 if now - t < self.conf.user_ratelimit_command_time else 0 for t in times])
        if recent_reqs >= self.conf.user_ratelimit_command_number or self.stopped:
            await msg.response.defer(ephemeral=True)
            if self.conf.user_ratelimit_command_number <= 1:
                recent_reqs -= 3  # allow some commands without ratelimiting in ephemeral mode
            sleeps = recent_reqs - (self.conf.user_ratelimit_command_number if not self.stopped else 0)
            await asyncio.sleep(sleeps * self.conf.user_ratelimit_sleep)
        times.append(time.time())
        return not self.stopped

    async def publish_event(self, ev: Event):
        attempt = 0
        while attempt < MAX_PUBLISH_ATTEMPTS:
            try:
                if type(ev) != BlockEvent:
                    self.log.debug(f"Publishing event {ev=}")
                if type(ev) in [DexEvent, MassPoolEvent, PoolEvent, CexEvent, TransferEvent]:
                    try:
                        if await self.replace_recent(ev):
                            await asyncio.sleep(0.5)
                            break
                    except disnake.errors.NotFound:
                        self.log.warning(f"Message not found for event {ev=}, publishing new one")
                        pass
                await self._publish_event(ev)
                if type(ev) != BlockEvent:
                    await asyncio.sleep(0.5)
                break
            except Exception as e:
                self.log.error(f"Publish error: {e}", exc_info=True)
                if ev.id in self.ev_to_msg:
                    self.log.warning(f"Error happened but event got published: {ev['_id']}")
                    break
                await asyncio.sleep(5)
                attempt += 1

    async def _publish_event(self, ev: Event):
        msg = None
        if type(ev) == StatsEvent:
            msg = self.build_stats_message(ev)
        elif type(ev) in [TransferEvent, DexEvent, InterestingTransferEvent]:
            msg = self.build_transfer_message(ev)
        elif type(ev) == KillEvent:
            msg = self.build_kill_message(ev)
        elif type(ev) == MassPoolEvent:
            msg = self.build_mass_pool_message(ev)
        elif type(ev) == CexEvent:
            msg = self.build_cex_trade_message(ev)
        elif type(ev) == ClubEvent:
            msg = self.build_club_message(ev)
        elif type(ev) == BlockEvent:
            await self.update_activity(ev)
        else:
            self.log.warning(f"Unknown event type: {ev=}")
        if msg:
            sent_msg = await self.send_notification(msg)
            await self.save_event(ev, sent_msg)
        await self.clean_event_cache()

    async def replace_recent(self, new_ev: DexEvent | MassPoolEvent | TransferEvent) -> bool:
        for ev_id, (ev, msg) in self.ev_to_msg.items():
            if new_ev.id == ev.id:
                self.log.warning(f"Event {ev_id} already published, {ev=}")
                # continue
            if type(ev) != type(new_ev):
                continue
            if type(ev) in [MassPoolEvent, PoolEvent]:
                if datetime.now(tz=timezone.utc) - msg.created_at > timedelta(seconds=self.conf.pool_event_replace_period):
                    continue
            else:
                if datetime.now(tz=timezone.utc) - msg.created_at > timedelta(seconds=self.conf.event_replace_period):
                    continue

            if type(ev) in [MassPoolEvent, PoolEvent] and ev.can_join(new_ev):
                ev.join(new_ev)
                await msg.edit(**self.build_mass_pool_message(ev))
                await self.save_event(ev, msg)
                return True
            elif type(ev) == DexEvent and ev.can_join(new_ev):
                ev.join(new_ev, self.db.known)
                await msg.edit(**self.build_transfer_message(ev))
                await self.save_event(ev, msg)
                return True
            elif type(ev) == CexEvent:
                ev.join(new_ev, self.db.prices)
                await msg.edit(**self.build_cex_trade_message(ev))
                await self.save_event(ev, msg)
                return True
            elif type(ev) == TransferEvent and ev.can_join(new_ev):
                ev.join(new_ev)
                await msg.edit(**self.build_transfer_message(ev))
                await self.save_event(ev, msg)
                return True
        return False

    async def send_notification(self, msg: dict) -> disnake.Message:
        if self.stopped:
            self.log.warning("Stopped, not sending the notification")
            return
        channel = self.disbot.get_channel(self.conf.notif_channel)
        if channel is None:
            self.log.warning("No channel, can't send messages yet")
            return
        return await channel.send(**msg)

    def build_stats_message(self, ev: StatsEvent, usd = True) -> dict:
        self.log.debug(ev)
        self.log.debug(ev.to_dict())
        color = Color.blurple()
        hours = int(round(ev.period / (60 * 60), 0))
        embeds = []
        ce = Embed(color=color, title=f"Chain stats for the past {hours} hours")
        embeds.append(ce)
        ce.add_field('Stake +/-', f"{ev.staked:,}/{ev.unstaked:,} iDNA", inline=True)
        ce.add_field('Bridged from/to BSC', f"{ev.bridged_from_bsc:,}/{ev.bridged_to_bsc:,} iDNA", inline=True)
        ce.add_field('Burned', f"{ev.burned:,} iDNA", inline=True)
        ce.add_field('Terminated', f"{ev.identities_killed:,} identities", inline=True)
        ce.add_field('Invites sent/activated', f"{ev.invites_issued:,}/{ev.invites_activated:,}", inline=True)
        ce.add_field('Contract calls', f"{ev.contract_calls}", inline=True)

        if len(ev.markets) > 0:
            if usd:
                buy_usd_vol = sum([m.buy_usd for m in ev.markets.values()])
                sell_usd_vol = sum([m.sell_usd for m in ev.markets.values()])
                desc = f"Buy/Sell volume: **${buy_usd_vol:,.0f}** / **${sell_usd_vol:,.0f}**"
            else:
                buy_vol = sum([m.buy for m in ev.markets.values()])
                sell_vol = sum([m.sell for m in ev.markets.values()])
                desc = f"Buy/Sell volume: **{buy_vol:,.0f}** / **{sell_vol:,.0f} iDNA**"
            me = Embed(color=Color.og_blurple(), title=f"Market stats for the past {hours} hours", description=desc)
            embeds.append(me)
            for m_name, m in ev.markets.items():
                me.add_field('Market', MARKETS[m_name]['link'], inline=True)
                if usd:
                    me.add_field('Buy/Sell', f"${m.buy_usd:,.0f} / ${m.sell_usd:,.0f}", inline=True)
                else:
                    me.add_field('Buy/Sell', f"{m.buy:,.0f} / {m.sell:,.0f} iDNA", inline=True)
                if m.avg_price != 0:
                    if usd:
                        me.add_field('Avg Price', f"${m.avg_price_usd:,.3f}", inline=True)
                    else:
                        me.add_field('Avg Price', m.avg_price, inline=True)
                else:
                    me.add_field('​', '​', inline=True)

        comps = [disnake.ui.Button(label=f'{"iDNA" if usd else "USD"} amounts', style=disnake.ButtonStyle.blurple, custom_id=f'{ev.id}:change_stats:{"idna" if usd else "usd"}'),
            disnake.ui.Button(label='Change period', custom_id=f'{ev.id}:change_stats:period', style=disnake.ButtonStyle.grey)]
        return {'embeds': embeds, 'components': comps}

    def build_transfer_message(self, ev: TransferEvent | InterestingTransferEvent | DexEvent) -> dict:
        dt = ev.time
        tfs = ev.tfs
        by = ev.by
        comps = []
        if type(ev) == TransferEvent:
            addr = self.get_addr_text(ev.tfs[0].signer, 'address', chain=ev.tfs[0].chain)
            if getattr(ev, '_recv', False):
                desc = f"Receiver: {addr}"
                title = 'Large receive volume'
            else:
                desc = f"Address: {addr}"
                title = 'Large transfer volume'
        elif type(ev) == InterestingTransferEvent:
            addr = self.get_addr_text(ev.tfs[0].signer, 'address', chain=ev.tfs[0].chain)
            desc = f"Address: {addr}"
            title = 'Interesting trasnfer'
        elif type(ev) == DexEvent:
            addr = None
            desc = None
            title = 'Large DEX volume'
        else:
            raise Exception('Unsupported event type')

        try:
            dtfs = list(map(lambda tf: self.describe_tf(tf, by), tfs))
            if len(tfs) == 1:
                dtf = dtfs[0]
                em = Embed(title=dtf['title'], description=dtf.get('desc'), color=dtf['color'], url=dtf['url'], timestamp=dt)
                em.add_field("Address", addr if addr else self.get_addr_text(dtf['by'], 'address', chain=ev.tfs[0].chain), inline=True)
                em.add_field("Value", f"${dtf['value']:,.2f}", inline=True)
                if 'price' in dtf:
                    em.add_field("Price", f"${dtf['price']:.3f}", inline=True)
                if 'thumbnail' in dtf:
                    em.set_thumbnail(dtf['thumbnail'])
            else:
                em = Embed(title=title, timestamp=dt)

                if type(ev) == DexEvent:
                    em.color = trade_color(ev.buy_usd, ev.sell_usd)
                    em.description = f"Buy/sell volume: **${ev.buy_usd:,.0f}** / **${ev.sell_usd:,.0f}**"
                    if ev.lp_usd:
                        em.description += f"\nLiquidity change: **{'-' if ev.lp_usd < 0 else '+'}${abs(ev.lp_usd):,.0f}**"
                    em.description += f"\nAverage price: **${ev.avg_price:.3f}**"
                    if ev.last_price:
                        em.description += f"\nLast traded price: **${ev.last_price:.3f}**"
                else:
                    em.description = f"Address: {self.get_addr_text(ev.by, chain=ev.tfs[0].chain)}\nAmount: **{ev.amount:,.0f}** iDNA"
                    em.color = average_color(map(lambda tf: tf['color'], dtfs))
                    total_value = sum([tf.meta['usd_value'] for tf in tfs])
                    em.add_field("Value", f"${total_value:,.2f}", inline=True)

                comps = [disnake.ui.Button(label='Show transactions', style=disnake.ButtonStyle.blurple, custom_id=f'{ev.id}:show_tfs')]
        except Exception as exc:
            self.log.error(f"Failed to describe tfs: {exc}", exc_info=True)
            tf_text = ''
            for i, tf in enumerate(tfs):
                tf_text += f"\n{i+1}. {self.get_tx_text(tf)} ({int(tf.value())} iDNA)"
                if i >= 4 and len(tfs) > 5:
                    tf_text += f'\nAnd {len(tfs) - 5} others'
                    break
            em = Embed(color=Color.blue(), title=title, timestamp=dt)
            if desc:
                em.description = desc
            em.add_field("Transactions", tf_text, inline=False)
        if type(ev) == InterestingTransferEvent:
            em.color = Color.from_rgb(79, 36, 203)
            if em.title != 'Transfer':
                em.title = f"Interesting transfer: {em.title}"
            else:
                em.title = f"Interesting transfer"
        ev._color = em.color
        output = {'embed': em, 'components': comps}
        return output

    def build_kill_message(self, ev: KillEvent) -> dict:
        color = Color.from_rgb(32, 32, 32)
        dt = ev.time
        desc = f"Identity: {self.get_addr_text(ev.killed, 'identity')}"
        if IDENA_TAG_KILL_DELEGATOR in ev.tfs[0].tags:
            title = 'Delegator termination'
        else:
            title = 'Termination'

        e = Embed(color=color, title=title, description=desc,
                  url=f"https://scan.idena.io/transaction/{ev.tfs[0].hash}", timestamp=dt)
        e.set_thumbnail(f'https://robohash.idena.io/{ev.killed}')
        e.add_field("Age", ev.age, inline=True)
        if ev.stake > 0:
            e.add_field("Stake", f"{abs(ev.stake):,.0f} iDNA", inline=True)
        if ev.pool:
            e.add_field("Pool", self.get_addr_text(ev.pool, 'pool'), inline=True)
        return {'embed': e}

    def build_mass_pool_message(self, ev: MassPoolEvent) -> dict:
        if ev.subtype == 'kill':
            color = Color.dark_red()
            name = 'termination'
        elif ev.subtype == 'delegate':
            color = Color.dark_green()
            name = 'delegation'
        elif ev.subtype == 'undelegate':
            color = Color.dark_red()
            name = 'undelegation'
        else:
            raise Exception(f"Unknown mass pool subtype: {ev.subtype}")

        dt = max([ch.time for ch in ev.changes])
        desc = f"Pool: {self.get_addr_text(ev.pool, 'pool')}"
        if ev.stake or ev.age:
            desc += f"\nTotal stake: **{ev.stake:,.0f}** iDNA, Total age: **{ev.age:,}**"
        desc += f'\nNumber of identities: **{ev.count}**'
        e = Embed(title=f"Mass pool {name}", description=desc, color=color,
                  url=f"https://scan.idena.io/pool/{ev.pool}", timestamp=dt)
        e.set_thumbnail(f'https://robohash.idena.io/{ev.pool}')
        comps = [disnake.ui.Button(label='Show identities', style=disnake.ButtonStyle.blurple, custom_id=f'{ev.id}:show_idents')]
        return {'embed': e, 'components': comps}

    def build_ident_list_embed(self, ev: MassPoolEvent, start_index: int, lines: int = 10) -> disnake.Embed:
        if ev.subtype == 'kill':
            color = Color.dark_red()
            title = 'Terminated identities'
        elif ev.subtype == 'delegate':
            color = Color.dark_green()
            title = 'Delegated identities'
        elif ev.subtype == 'undelegate':
            color = Color.dark_red()
            title = 'Undelegated identities'
        idents_text = ""
        changes = list(sorted(ev.changes, key=lambda ch: ch.stake, reverse=True))
        changes = changes[start_index:]
        for i, pev in enumerate(changes):
            idents_text += '\n' if i != 0 else ''
            idents_text += f"{i+1+start_index}. {self.get_addr_text(pev.addr, 'identity', short=False)}"
            idents_text += f" ({int(pev.stake):,} iDNA, age: {pev.age})"
            if i >= (lines - 1) and len(changes) > lines:
                idents_text += f'\nAnd {len(changes) - lines:,} more'
                break
        em = Embed(title=title, description=idents_text, color=color)
        return em

    def build_tf_list_embed(self, ev: TransferEvent, start_index: int, lines: int = 10) -> disnake.Embed:
        title = 'Transactions (oldest first)'
        tf_text = ""
        tfs = ev.tfs[start_index:]
        for i, tf in enumerate(tfs):
            dtf = self.describe_tf(tf, ev_by=ev.by)
            tf_text += '\n' if i != 0 else ''
            price = f" _(at ${tf.meta.get('usd_price', 0):,.4f})_" if tf.meta.get('usd_price') is not None else ''
            tf_text += f"{i+1+start_index}. {dtf['short']}{price}"
            if i >= (lines - 1) and len(tfs) > lines:
                tf_text += f'\nAnd {len(tfs) - lines:,} more'
                break
        em = Embed(title=title, description=tf_text, color=ev._color)
        return em

    def build_pool_stats_message(self, ev: PoolStatsEvent, start_index: int = 0) -> dict:
        color = Color.dark_blue()
        fields = [{'title': "Terminated identities", 'field': 'killed'},
                  {'title': "Delegated identities", 'field': 'delegated'},
                  {'title': "Undelegated identities", 'field': 'undelegated'},]
        lines = 5 if not ev._long else 10  # TODO: this can fail sometimes because of address names
        hours = int(round(ev.period / (60 * 60), 0))
        start_index = max(0, min(start_index, len(ev) - lines))
        e = Embed(color=color, title=f"Pool stats for the past {hours} hours")
        for field in fields:
            field_text = ''
            field_pools = list(getattr(ev, field['field']).items())
            page = field_pools[start_index:]
            for i, (pool, count) in enumerate(page):
                if count == 0:
                    break
                field_text += '\n' if i != 0 else ''
                field_text += f"{i+start_index+1}. {self.get_addr_text(pool, 'pool', no_link=False, short_len=4, short=True)}: {count:,}"
                if i == lines - 1 and len(page) > lines:
                    rest_sum = sum([p[1] for p in page[i+1:]])
                    field_text += f'\nAnd {len(page) - lines} others with {rest_sum:,} identitites'
                    break
            if len(field_text) > 0:
                e.add_field(field['title'], field_text, inline=True)
        comps = self.build_list_buttons(ev.id, 'pool_seek', start_index, len(ev), lines)
        return {'embed': e, 'components': comps}

    def build_cex_trade_message(self, ev: CexEvent) -> dict:
        color = trade_color(ev.total_buy_val, ev.total_sell_val)

        desc = f'Total buy/sell volume: **${ev.total_buy_val:,.0f}** / **${ev.total_sell_val:,.0f}**'
        em = Embed(color=color, title=f"Large CEX volume", description=desc)
        for m_name, m in ev.markets.items():
            em.add_field('Market', MARKETS[m_name]['link'], inline=True)
            em.add_field('Buy/Sell Volume', f"${m.buy_usd:,.0f} / ${m.sell_usd:,.0f}", inline=True)
            em.add_field('Avg Price', f"${m.avg_price_usd:.3f}", inline=True)
        return {'embed': em}

    def build_club_message(self, ev: ClubEvent) -> dict:
        desc = f'Identity {self.get_addr_text(ev.addr, "identity")} has entered the **{ev.club_str}** stake club!'
        if ev.rank > 0:
            desc += f'\nRanked **#{ev.rank}** by stake'
        color = get_identity_color(ev.addr)
        em = Embed(color=color, title=f"{ev.club_str} club membership", description=desc)
        em.set_thumbnail(f'https://robohash.idena.io/{ev.addr}')
        em.add_field('Stake', f"{ev.stake:,.0f} iDNA", inline=True)
        return {'embed': em}

    def build_rank_message(self, ident: dict, rewards: float) -> dict:
        addr = ident['address']
        title = f'Rank of {shorten(addr)}'
        network_size = self.db.count_alive_identities()
        self.log.debug(f"{network_size=}")

        color = get_identity_color(addr)
        em = Embed(color=color, title=title, url=f'https://scan.idena.io/identity/{addr}')
        em.set_thumbnail(f'https://robohash.idena.io/{addr}')

        stake = Decimal(ident['stake'])
        rank = self.db.count_identities_with_stake(stake)
        stake = int(stake)  # to round down
        perc = rank / network_size * 100
        em.add_field('Stake', f'**{stake:,.0f}** iDNA – **#{rank}** (top **{perc:.2f}%**)', inline=False)

        age = int(ident['age'])
        rank = self.db.count_identities_with_age(age)
        perc = rank / network_size * 100
        em.add_field('Age', f'**{age:,.0f}** epochs – **#{rank}** (top **{perc:.2f}%**)', inline=False)

        rewards = f'**{rewards[0]:,.0f}** iDNA (**{rewards[1]:.1f}%** APY)'
        if ident['state'] in ['Newbie', 'Verified', 'Human']:
            em.add_field('Epoch rewards', f'{rewards}', inline=False)
        else:
            em.add_field('Epoch rewards', f'~~{rewards}~~ ({ident["state"]})', inline=False)

        return {'embed': em}

    def build_top_message(self, ev: TopEvent, start_index: int = 0) -> dict:
        DESC_LIMIT = 4096
        LINES = 10 if not ev._long else 20
        desc = f'Total value: **${ev.total_usd_value:,.0f}**'
        if type(ev) in [TopKillEvent, TopStakeEvent]:
            desc += f' (**{ev.total_idna:,.0f}** iDNA)'
        if type(ev) == TopKillEvent:
            desc += f'. Total age: **{ev.total_age:,}**'
        elif type(ev) == TopDexEvent:
            desc = f"Buy/Sell volume: **${ev.buy_usd:,.0f}** / **${ev.sell_usd:,.0f}**"
            if ev.lp_usd != 0:
                desc += f"\nLiquidity change: **{'-' if ev.lp_usd < 0 else '+'}${abs(ev.lp_usd):,.0f}**"
        tf_texts = []
        total_len = 0
        didnt_fit = 0
        start_index = max(0, min(start_index, len(ev) - LINES))
        page_items = ev.items[start_index:]
        for i, item in enumerate(page_items):
            if type(ev) != TopStakeEvent:
                tf: Transfer = item

            index = f"{i + start_index + 1}"
            if type(ev) == TopKillEvent:
                killed = self.get_addr_text(tf.meta['killedIdentity'], type_='identity', short_len=4)
                stake = abs(list(tf.changes.values())[0])
                tf_texts.append(f"{index}. {killed} ({stake:,.0f} iDNA, age: {tf.meta['age']})")
            elif type(ev) == TopStakeEvent:
                addr = self.get_addr_text(item['signer'], type_='address')
                tf_texts.append(f"{index}. {addr} ({item['stake']:,.0f} iDNA)")
            elif type(ev) == TopDexEvent:
                dtf = self.describe_tf(tf)
                price = f" _(at ${tf.meta.get('usd_price', 0):,.4f})_" if tf.meta.get('usd_price') is not None else ''
                tf_texts.append(f"{index}. {dtf['short']}{price}")
            elif type(ev) == TopEvent:
                try:
                    dtf = self.describe_tf(tf)
                except Exception as e:
                    self.log.error(f"{tf.hash}")
                    continue
                tf_texts.append(f"{index}. {dtf['short']}")
            total_len += len(tf_texts[-1])
            if total_len >= DESC_LIMIT - 500 or i == LINES - 1:  # some margin
                didnt_fit = len(ev) - i
                break
        if didnt_fit - start_index - 1 > 0:
            tf_texts.append(f'And {didnt_fit - start_index - 1:,} more')
        tf_text = '\n'.join(tf_texts)
        past = f'for the past {int(ev.period / 60 / 60)} hours'
        desc = f"{desc}\n{tf_text}"
        if type(ev) == TopKillEvent:
            em = Embed(title=f'Top kills {past}', description=desc, color=Color.dark_red())
        elif type(ev) == TopStakeEvent:
            em = Embed(title=f'Top stake replenishments {past}', description=desc, color=Color.dark_green())
        elif type(ev) == TopDexEvent:
            color = trade_color(ev.buy_usd, ev.sell_usd)
            em = Embed(title=f'Top DEX transactions {past}', description=desc, color=color)
        elif type(ev) == TopEvent:
            em = Embed(title=f'Top transfers {past}', description=desc, color=Color.from_rgb(129, 190, 238))

        comps = self.build_list_buttons(ev.id, 'top_seek', start_index, len(ev), LINES)
        return {'embed': em, 'components': comps}

    async def build_bna_airdrop_message(self, address: str) -> dict:
        raw_tx = await self.idena_listener.build_bna_airdrop_tx(address.lower())
        if not raw_tx.startswith('0'):
            return {'content': f'This address cannot claim the airdrop: {raw_tx}'}
        tx_web_url = f"https://app.idena.io/dna/raw?tx={raw_tx}&callback_url=https%3A%2F%2Fdiscord.com%2Fchannels%2F634481767352369162%2F634497771457609759"
        comps = [
            disnake.ui.Button(label='Claim!', url=tx_web_url),
        ]
        return {'components': comps, 'ephemeral': True}

    async def build_bna_send_message(self, from_address: str, to_address: str, amount: Decimal) -> dict:
        raw_tx, amount = await self.idena_listener.build_bna_send_tx(from_address, to_address, amount)
        tx_web_url = f"https://app.idena.io/dna/raw?tx={raw_tx}&callback_url=https%3A%2F%2Fdiscord.com%2Fchannels%2F634481767352369162%2F634497771457609759"
        comps = [
            disnake.ui.Button(label=f'Send {amount:f} BNA', url=tx_web_url),
        ]
        return {'components': comps, 'ephemeral': True}

    def build_list_buttons(self, ev_id, cmd: str, start_index: int, item_count: int, lines: int, last_index: int | None = None):
        back_disabled = start_index <= 0
        if last_index:
            forward_disabled = last_index == item_count
        else:
            forward_disabled = start_index + lines >= item_count
        comps = []
        if not back_disabled or not forward_disabled:
            end_index = 9999999  # needed to avoid an id collision, can't specify an actual index
            comps.extend([
                disnake.ui.Button(label='◁', style=disnake.ButtonStyle.green, custom_id=f'{ev_id}:{cmd}:{max(start_index - lines, 0)}', disabled=back_disabled),
                disnake.ui.Button(label='▷', style=disnake.ButtonStyle.green, custom_id=f'{ev_id}:{cmd}:{start_index + lines}', disabled=forward_disabled),
                disnake.ui.Button(label='End' if not forward_disabled else 'Start', style=disnake.ButtonStyle.grey, custom_id=f'{ev_id}:{cmd}:{end_index if not forward_disabled else -1}')])
        return comps

    def describe_tf(self, tf: Transfer, ev_by: str = '') -> dict:
        "Turns a Transfer into a dict that can [almost] be displayed as an Embed."
        self.log.debug(f"Describing: {tf}")
        d = {'title': 'Transfer', 'desc': '', 'short': self.get_tx_text(tf, 'Unknown transfer'),
             'value': tf.meta.get('usd_value', 0), 'by': tf.signer if not ev_by else ev_by, 'tf': tf,
             'chain': tf.chain, 'color': Color.from_rgb(129, 190, 238), 'url': self.get_tf_url(tf)}
        if 'usd_price' in tf.meta:
            d['price'] = tf.meta['usd_price']
        if IDENA_TAG_SEND in tf.tags or len(tf.tags) == 0:
            if tf.from_(single=True) != d['by']:
                desc = f'Received **{tf.value():,.0f}** iDNA from {self.get_addr_text(tf.from_(single=True), chain=tf.chain)}'
                short = f'Received {tf.value():,.0f} iDNA from {shorten(tf.from_(single=True), 3)}'
            else:
                desc = f'Sent **{tf.value():,.0f}** iDNA to {self.get_addr_text(tf.to(single=True), chain=tf.chain)}'
                short = f'Sent {tf.value():,.0f} iDNA to {shorten(tf.to(single=True), 3)}'
            d.update({'title': 'Transfer', 'desc': desc, 'short': self.get_tx_text(tf, desc=short)})
        elif IDENA_TAG_BRIDGE_BURN in tf.tags:
            d.update({'title': 'Bridge transfer to BSC',
                      'desc': f'Bridged **{tf.value():,.0f}** iDNA to BSC address {self.get_addr_text(tf.meta["bridge_to"], chain="bsc")}',
                      'short': self.get_tx_text(tf, f'Bridged to BSC: {shorten(tf.meta["bridge_to"])}'),
                      'color': Color.light_grey()})
        elif IDENA_TAG_BRIDGE_MINT in tf.tags:
            d.update({'title': 'Bridge transfer from BSC',
                      'desc': f'Bridged **{tf.value():,.0f}** iDNA to Idena address {self.get_addr_text(tf.to(single=True), chain="idena")}',
                      'short': self.get_tx_text(tf, f'Bridged from BSC: {shorten(tf.to(single=True))}'),
                      'color': Color.light_grey()})
        elif IDENA_TAG_STAKE in tf.tags:
            receiver = tf.to(single=False)
            if not receiver:
                receiver = tf.signer
                desc = f'Replenished stake with **{tf.value():,.0f}** iDNA'
                short = f'Replenished stake with {tf.value():,.0f} iDNA'
            else:
                receiver = receiver[0]
                desc = f'Replenished stake of {self.get_addr_text(receiver, type_="identity")} with **{tf.value():,.0f}** iDNA'
                short = f'Replenished stake of {shorten(receiver, length=3)} with {tf.value():,.0f} iDNA'
            if 'cur_stake' in tf.meta:
                cur_stake = Decimal(tf.meta['cur_stake'])
                desc += f'\nCurrent stake: **{cur_stake:,.0f}** iDNA'
            d.update({'title': 'Stake replenishment',
                      'desc': desc, 'short': self.get_tx_text(tf, desc=short),
                      'thumbnail': f'https://robohash.idena.io/{receiver}',
                      'color': Color.dark_green()})
        elif IDENA_TAG_BURN in tf.tags:
            d.update({'title': 'Coin burn',
                      'desc': f'Burned **{tf.value():,.0f}** iDNA',
                      'short': self.get_tx_text(tf, f'Burned coins'),
                      'color': Color.orange()})
        elif any_in(tf.tags, [IDENA_TAG_KILL, IDENA_TAG_KILL_DELEGATOR]):
            if IDENA_TAG_KILL in tf.tags:
                title = 'Identity killed'
            else:
                title = 'Delegator killed'
            d.update({'title': title,
                      'desc': f'Killed at age **{tf.meta.get("age", "?")}** and unlocked **{tf.value(recv=True):,.0f}** iDNA',
                      'short': self.get_tx_text(tf, f'{title} for {tf.value(recv=True)} iDNA'),
                      'color': Color.from_rgb(32, 32, 32)})
        elif IDENA_TAG_DEPLOY in tf.tags:
            d.update({'title': 'Contract deployment',
                      'desc': f'Deployed contract {self.get_addr_text(tf.meta["call"]["contract"], type_="contract")}' + f' with **{tf.value():,.0f}** iDNA' if int(tf.value()) > 0 else '',
                      'short': self.get_tx_text(tf, f'Deployed contract'),
                      'color': Color.blue()})
        elif IDENA_TAG_CALL in tf.tags:
            d.update({'title': 'Contract call',
                      'desc': f'Called contract {self.get_addr_text(tf.meta["call"]["contract"], type_="contract")}' + f' with **{tf.value():,.0f}** iDNA' if int(tf.value()) > 0 else '',
                      'short': self.get_tx_text(tf, f'Called contract'),
                      'color': Color.blue()})
        elif BSC_TAG_BRIDGE_BURN in tf.tags:
            d.update({'title': 'Bridge transfer to Idena',
                      'desc': f'Bridged **{tf.value():,.0f}** iDNA to Idena',
                      'short': self.get_tx_text(tf, f'Bridged to Idena'),
                      'color': Color.light_grey()})
        elif BSC_TAG_BRIDGE_MINT in tf.tags:
            d.update({'title': 'Bridge transfer from Idena',
                      'desc': f'Bridged **{tf.value():,.0f}** iDNA to BSC address {self.get_addr_text(tf.to(True), chain="bsc")}',
                      'short': self.get_tx_text(tf, f'Bridged from Idena: {shorten(tf.to(single=True))}'),
                      'color': Color.light_grey()})
        elif DEX_TAG not in tf.tags:
            self.log.warning(f"Can't describe transfer: {tf}")
        if 'usd_value' in tf.meta:
            d['short'] += f" (${tf.meta['usd_value']:,.2f})"
        else:
            d['short'] += f" ({tf.value():,.0f} iDNA)"

        if DEX_TAG in tf.tags:
            liq_str = ''
            if any_in(tf.tags, DEX_LP_TAGS):
                for pool_addr, pool_ch in tf.meta['lp'].items():
                    liq_str += ', ' if liq_str else ''
                    ticker = self.db.known[self.db.known[pool_addr]['token1']]['name']
                    liq_str += f"**{abs(float(pool_ch['token'])):,.2f}** {ticker} + **{abs(int(pool_ch['idna'])):,.0f}** iDNA"
                excess = tf.meta.get('lp_excess', 0)
                if DEX_TAG_PROVIDE_LP in tf.tags and excess != 0:
                    liq_str = f"{'Bought' if excess > 0 else 'Sold'} **{int((abs(excess))):,}** iDNA and deposited {liq_str}"
                elif DEX_TAG_WITHDRAW_LP in tf.tags and excess != 0:
                    liq_str = f"Withdrawn {liq_str} and {'bought' if excess > 0 else 'sold'} **{abs(excess):,.0f}** iDNA"
                else:
                    liq_str = f"{'Deposited' if DEX_TAG_PROVIDE_LP in tf.tags else 'Withdrawn'} {liq_str}"
            if DEX_TAG_WITHDRAW_LP in tf.tags:
                d.update({'title': 'DEX liquidity withdrawal',
                          'desc': liq_str,
                          'short': self.get_tx_text(tf, f'Withdrawn LP'),
                          'color': Color.dark_red()})
            elif DEX_TAG_PROVIDE_LP in tf.tags:
                d.update({'title': 'DEX liquidity deposit',
                          'desc': liq_str,
                          'short': self.get_tx_text(tf, f'Deposited LP'),
                          'color': Color.dark_green()})
            elif DEX_TAG_ARB in tf.tags:
                d.update({'title': 'DEX arbitrage',
                          'short': self.get_tx_text(tf, f'Arbitraged iDNA'),
                          'color': Color.light_grey()})
            elif DEX_TAG_BUY in tf.tags:
                d.update({'title': 'DEX buy',
                          'short': self.get_tx_text(tf, f'Bought iDNA'),
                          'color': Color.green()})
            elif DEX_TAG_SELL in tf.tags:
                d.update({'title': 'DEX sell',
                          'short': self.get_tx_text(tf, f'Sold iDNA'),
                          'color': Color.red()})
            if 'usd_value' in tf.meta:
                d['short'] += f" (${tf.meta['usd_value']:,.2f})"
            else:
                d['short'] += f" ({tf.value():,.0f} iDNA)"

            if any_in(tf.tags, DEX_TRADE_TAGS) and not any_in(tf.tags, DEX_LP_TAGS):
                for_tokens = ''
                # Sorting the change by name for consistent order
                tokens = map(lambda i: (self.db.known[i[0]]['name'], abs(float(i[1]))), tf.meta.get('token', {}).items())
                tokens = sorted(list(tokens), key=lambda t: t[0])
                for ticker, change in tokens:
                    for_tokens += ' + ' if for_tokens else ''
                    for_tokens += f'**{change:,.2f}** {ticker}'
                d['desc'] = f'{"Bought" if DEX_TAG_BUY in tf.tags else "Sold"} **{tf.value():,.0f}** iDNA for {for_tokens}'

        return d

    def get_addr_text(self, addr: str, type_='address', chain='idena', short=False, short_len=5, no_link=False) -> str:
        info = self.db.known.get(addr)
        if info:
            name = info.get('name')
            if type(name) is str:
                name = info['name']
            elif type(name) is dict:
                name = info['name'].get(chain)

        if chain == 'idena':
            url = 'https://scan.idena.io'
        elif chain == 'bsc':
            url = 'https://bscscan.com'
        else:
            raise Exception("Unsupported chain")
        if info and name and info.get('hidden', False) is False:
            if not short:
                return f"[{name}]({url}/{type_}/{addr} \'{addr}\')"
            else:
                return f"[{name}]({url}/{type_}/{addr})"
        else:
            if no_link:
                return f"{shorten(addr)}"
            elif not short:
                return f"[{shorten(addr, short_len)}]({url}/{type_}/{addr} \'{addr}\')"
            else:
                return f"[{shorten(addr, short_len)}]({url}/{type_}/{addr})"

    def get_tx_text(self, tf: Transfer, desc=None) -> str:
        if tf.chain == 'idena':
            url = 'https://scan.idena.io'
            type = 'transaction'
        elif tf.chain == 'bsc':
            url = 'https://bscscan.com'
            type = 'tx'
        else:
            raise Exception("Unsupported chain")
        return f"[{shorten(tf.hash) if not desc else desc}]({url}/{type}/{tf.hash})"

    def get_tf_url(self, tf: Transfer) -> str:
        if tf.chain == 'idena':
            url = 'https://scan.idena.io'
            type = 'transaction'
        elif tf.chain == 'bsc':
            url = 'https://bscscan.com'
            type = 'tx'
        else:
            raise Exception("Unsupported chain")
        return f"{url}/{type}/{tf.hash}"

    async def update_activity(self, ev: BlockEvent):
        self.activity['block'].update({ev.chain: ev.height})
        if time.time() - self.last_activity_update < 10:
            return
        act_str = f"IDNA: {self.activity['block'][CHAIN_IDENA]} BSC: {self.activity['block'][CHAIN_BSC]}"
        self.last_activity_update = time.time()
        await self.disbot.change_presence(status=disnake.Status.online, activity=disnake.Activity(type=disnake.ActivityType.watching, name=act_str))

    async def get_event(self, ev_id: int):
        self.log.debug(f"Getting event {ev_id}")
        if ev_id in self.ev_to_msg:
            return self.ev_to_msg[ev_id]
        else:
            try:
                chan_id, msg_id, ev = await self.db.get_event(ev_id)
                if not ev:
                    return (None, None)
                chan = self.disbot.get_channel(chan_id)
                if not chan:
                    chan = await self.disbot.fetch_channel(chan_id)
                msg = await chan.fetch_message(msg_id)
                self.ev_to_msg[ev.id] = (ev, msg)
                return (ev, msg)
            except Exception as e:
                self.log.error(f"Failed to get event {ev_id}: {e}", exc_info=True)
                return (None, None)

    async def save_event(self, ev: Event, resp: disnake.Message):
        if type(ev) == BlockEvent:
            return
        self.log.debug(f"Saving event {ev.id=} {resp=}")
        if not resp:
            self.log.warning(f"Invalid message for ev: {ev.id=} {ev=} {resp=}")
            return
        self.ev_to_msg[ev.id] = (ev, resp)
        await self.db.insert_event(ev=ev, msg=resp)

    async def clean_event_cache(self):
        items = list(self.ev_to_msg.items())
        if datetime.now(tz=timezone.utc) - self.event_cache_cleaned_at < timedelta(minutes=20):
            return
        self.event_cache_cleaned_at = datetime.now(tz=timezone.utc)
        self.log.debug("Cleaning event cache")
        for ev_id, (ev, ev_msg) in items:
            if not ev or not ev_msg:
                self.log.warning(f"Event {ev_id=} {ev=} {ev_msg=} is invalid")
                del self.ev_to_msg[ev_id]
                continue
            if datetime.now(tz=timezone.utc) - ev_msg.created_at > timedelta(days=3):
                self.log.debug(f"Removing event {ev_id=} {ev_msg.id=}")
                try:
                    if ev_msg.components:
                        await ev_msg.edit(components=None)
                except Exception as e:
                    self.log.error(f"Failed to remove buttons from event {ev_id=} {ev_msg.id=}: {e}")
                del self.ev_to_msg[ev_id]

    def reset_state(self):
        self.last_activity_update = 0
        self.ev_to_msg.clear()
        self.event_cache_cleaned_at = datetime.now(tz=timezone.utc)


# None of this looks right
def create_bot(db: Database, conf: Config, root_log: Logger):
    disbot = commands.InteractionBot()
    log = root_log.getChild("DI")
    DEV_USER = int(os.environ["DEV_USER_ID"])
    bot = Bot(disbot, conf.discord, db, DEV_USER, root_log)
    admin_users = conf.discord.admin_users
    admin_roles = conf.discord.admin_roles
    command_roles = lambda: chain(conf.discord.command_roles, admin_roles)

    @disbot.event
    async def on_ready():
        log.info(f"Disbot is ready: {disbot.user}")

    @disbot.event
    async def on_application_command(inter):
        log.debug(f"{inter=}")
        await disbot.process_application_commands(inter)

    @disbot.event
    async def on_button_click(inter: disnake.MessageInteraction):
        spl = inter.component.custom_id.split(':')
        log.debug(f"BTN {spl=} by {inter.author.name}#{inter.author.discriminator}")
        ev_id, cmd = int(spl[0]), spl[1]
        ev, _ = await bot.get_event(ev_id)
        if not ev:
            log.warning(f"Event {ev_id} not found")
            log.debug(inter.message)
            await inter.message.edit(components=[disnake.ui.Button(label="Not available anymore", disabled=True, style=disnake.ButtonStyle.grey)])
            # Interaction must be responded to otherwise it will look like it failed
            await inter.send(content=f"This event is no longer available", ephemeral=True)
            return
        btn_msg = None
        first_resp = False  # if True, response sends a new message, otherwise edit the existing response

        if cmd in ['show_idents', 'show_tfs']:
            LINES = 10
            start_index = 0
            first_resp = True
            if len(spl) > 2:
                start_index = int(spl[2])
                first_resp = False
            start_index = max(0, min(start_index, len(ev) - LINES))
            if cmd == 'show_idents':
                list_embed = bot.build_ident_list_embed(ev, start_index=start_index, lines=LINES)
            elif cmd == 'show_tfs':
                list_embed = bot.build_tf_list_embed(ev, start_index=start_index, lines=LINES)
            comps = bot.build_list_buttons(ev.id, cmd, start_index, len(ev), LINES)
            btn_msg = {'embed': list_embed, 'components': comps, 'ephemeral': True}
        elif cmd == 'top_seek':
            start_index = int(spl[2])
            btn_msg = bot.build_top_message(ev, start_index=start_index)
        elif cmd == 'pool_seek':
            start_index = int(spl[2])
            btn_msg = bot.build_pool_stats_message(ev, start_index=start_index)
        elif cmd == 'change_stats':
            sub_cmd = spl[2]
            if sub_cmd == 'usd':
                btn_msg = bot.build_stats_message(ev, usd=True)
            elif sub_cmd == 'idna':
                btn_msg = bot.build_stats_message(ev, usd=False)
            elif sub_cmd == 'period':
                hours = int(round(ev.period / (60 * 60), 0))
                input = disnake.ui.TextInput(label='Hours', custom_id='hours', placeholder=f"{hours}", value=f"{hours}", min_length=1, max_length=4, style=disnake.TextInputStyle.short)
                modal = disnake.ui.Modal(title="Change period", custom_id=f"{ev_id}:change_stats:period_set", components=[input])
                await inter.response.send_modal(modal)
            elif sub_cmd == 'period_set':
                breakpoint()

        if btn_msg:
            if first_resp:
                await inter.send(**btn_msg)
            else:
                if 'ephemeral' in btn_msg:
                    del btn_msg['ephemeral']
                await inter.response.edit_message(**btn_msg)
        else:
            log.warning(f"No button message for command {spl}!")

    @disbot.event
    async def on_modal_submit(inter: disnake.ModalInteraction):
        spl = inter.custom_id.split(':')
        ev_id, cmd = int(spl[0]), spl[1]
        log.debug(f"MOD {spl=} by {inter.author.name}#{inter.author.discriminator}")
        ev, ev_msg = await bot.get_event(ev_id)
        inter.data.components[0]['components'][0]['value']
        if cmd == 'change_stats':
            sub_cmd = spl[2]
            if sub_cmd == 'period_set':
                hours = None
                try:
                    hours = inter.data.components[0]['components'][0]['value']
                    hours = int(hours)
                except Exception as e:
                    log.error(f"Invalid period \"{hours}\": {e}")
                if hours < 1:
                    hours = 1
                elif hours > 720:
                    hours = 720

                new_ev = await bot.tracker.generate_stats_event(period=hours * 60 * 60)
                new_ev.id = ev.id
                new_ev_msg = bot.build_stats_message(new_ev)
                await inter.response.edit_message(**new_ev_msg)
                await bot.save_event(new_ev, ev_msg)

    def protect(users=admin_users, roles=admin_roles, public=False):
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                roles_list = list(roles()) if callable(roles) else list(roles)  # not great
                msg = kwargs['msg']
                log.info(f'CMD {msg.data.name}({msg.options}) by {msg.author.name}#{msg.author.discriminator} ({msg.author.id})')
                log.debug(f"{users=}, {roles_list=}, {public=}")
                allowed = False
                if msg.author.id == DEV_USER:
                    allowed = True
                elif msg.author.id in users:
                    allowed = True
                elif msg.guild and any_in(map(lambda r: r.id, msg.author.roles), roles_list):
                    allowed = True
                elif msg.channel.type == disnake.ChannelType.private and public:
                    allowed = True

                if allowed:
                    return await func(**kwargs)
                else:
                    log.warning(f"User {msg.author.id} tried to call a command, denying")
                    return await msg.response.send_message(f"Can't call this command: permission denied", ephemeral=True)
            return wrapper
        return decorator

    @disbot.slash_command(options=[disnake.Option("type", description="Type of event", choices=['transfer', 'dex', 'kill', 'stake'], required=True), disnake.Option("hours", description="Collect stats from this far back", required=False, type=disnake.OptionType.integer, min_value=1, max_value=720), disnake.Option("long", description="Show as many lines as possible", required=False, type=disnake.OptionType.boolean)])
    @protect(roles=command_roles, public=True)
    async def top(msg: disnake.CommandInteraction, type: str, hours: int = 24, long: bool = False):
        "Show the top recent events of a specific type"
        if not await bot.ratelimit(msg):
            await bot.send_response(msg, {'content': 'Command execution not allowed', 'ephemeral': True})
            return

        ev = await bot.tracker.generate_top_event(type, hours * 60 * 60, long=long)
        if not ev:
            await bot.send_response(msg, {'content': 'No events in the given time range', 'ephemeral': True})
            return

        log.debug(json.dumps(ev.to_dict()))
        ev_msg = bot.build_top_message(ev)
        apply_ephemeral(msg, ev_msg)
        if long:
            ev_msg['ephemeral'] = True
        sent_msg = await bot.send_response(msg, ev_msg)
        await bot.save_event(ev, sent_msg)

    @disbot.slash_command(options=[disnake.Option("hours", description="Collect stats from this far back", required=False, type=disnake.OptionType.integer, min_value=1, max_value=720), disnake.Option("usd", description="Show market stats in USD", required=False, type=disnake.OptionType.boolean)])
    @protect(roles=command_roles, public=True)
    async def stats(msg: disnake.CommandInteraction, hours: int = 24, usd: bool = True):
        "Show recent chain and market stats"
        if not await bot.ratelimit(msg):
            await bot.send_response(msg, {'content': 'Command execution not allowed', 'ephemeral': True})
            return
        ev = await bot.tracker.generate_stats_event(period=hours * 60 * 60)
        ev_msg = bot.build_stats_message(ev, usd=usd)
        apply_ephemeral(msg, ev_msg)
        sent_msg = await bot.send_response(msg, ev_msg)
        await bot.save_event(ev, sent_msg)

    @disbot.slash_command(options=[disnake.Option("hours", description="Collect pool stats from this far back", required=False, type=disnake.OptionType.integer, min_value=1, max_value=720), disnake.Option("long", description="Show as many lines as possible", required=False, type=disnake.OptionType.boolean)])
    @protect(roles=command_roles, public=True)
    async def pools(msg: disnake.CommandInteraction, hours: int = 24, long: bool = False):
        "Show recent pool stats"
        if not await bot.ratelimit(msg):
            await bot.send_response(msg, {'content': 'Command execution not allowed', 'ephemeral': True})
            return
        ev = await bot.tracker.generate_pool_stats(period=hours * 60 * 60, long=long)
        if not ev:
            await bot.send_response(msg, {'content': 'No pool events in the given time range', 'ephemeral': True})
        ev_msg = bot.build_pool_stats_message(ev)
        apply_ephemeral(msg, ev_msg)
        if long:
            ev_msg['ephemeral'] = True
        sent_msg = await bot.send_response(msg, ev_msg)
        await bot.save_event(ev, sent_msg)

    @disbot.slash_command(options=[disnake.Option("address", description="Show rank of this identity", required=False, type=disnake.OptionType.string), disnake.Option("remember", description="Use this address by default in the future", required=False, type=disnake.OptionType.boolean), disnake.Option("private", description="Only show the rank to you", required=False, type=disnake.OptionType.boolean)])
    @protect(roles=command_roles, public=True)
    async def rank(msg: disnake.CommandInteraction, address: str = '', remember: bool = False, private: bool = False):
        "Show the stake and age rank of an identity"
        if not await bot.ratelimit(msg):
            await bot.send_response(msg, {'content': 'Command execution not allowed', 'ephemeral': True})
            return
        if address and len(address) != 42 and address[:2] != '0x':
            await bot.send_response(msg, {'content': 'Invalid address', 'ephemeral': True})
            return
        if not address:
            if str(msg.author.id) not in bot.conf.rank_addresses:
                await bot.send_response(msg, {'content': "You don't have a remembered address.\nUse the `address` parameter to specify an address. You can also set `remember` to `True` to use it by default in the future.", 'ephemeral': True})
                return
            address = bot.conf.rank_addresses[str(msg.author.id)]
        if remember:
            bot.conf.rank_addresses[str(msg.author.id)] = address

        try:
            await bot.idena_listener.update_identity(address.lower())
            rewards = await bot.idena_listener.get_rewards_for_identity(address.lower())
        except Exception as e:
            log.error(f"Error updating identity: {e}", exc_info=True)
        ident = bot.db.get_identity(address.lower())
        if not ident or ident['state'].lower() == 'undefined':
            await bot.send_response(msg, {'content': f"Identity with address `{address}` not found", 'ephemeral': True})
            return
        ev_msg = bot.build_rank_message(ident, rewards)
        apply_ephemeral(msg, ev_msg)
        if private:
            ev_msg['ephemeral'] = True
        sent_msg = await bot.send_response(msg, ev_msg)

    @disbot.slash_command(dm_permission=False)
    async def token(msg: disnake.CommandInteraction):
        "BNA token"
        pass

    @token.sub_command(options=[disnake.Option("address", description="Your validated address", required=True, type=disnake.OptionType.string)], public=True)
    async def airdrop(msg: disnake.CommandInteraction, address: str):
        "Claim BNA tokens (only during epoch 103!)"
        if not await bot.ratelimit(msg):
            await bot.send_response(msg, {'content': 'Command execution not allowed', 'ephemeral': True})
            return
        if address and len(address) != 42 and address[:2] != '0x':
            await bot.send_response(msg, {'content': 'Invalid address', 'ephemeral': True})
            return
        ev_msg = await bot.build_bna_airdrop_message(address)
        ev_msg['ephemeral'] = True
        sent_msg = await bot.send_response(msg, ev_msg)

    @token.sub_command(options=[disnake.Option("address", description="Your validated address", required=True, type=disnake.OptionType.string)], public=True)
    async def balance(msg: disnake.CommandInteraction, address: str):
        "Get the BNA balance of an address"
        if not await bot.ratelimit(msg):
            await bot.send_response(msg, {'content': 'Command execution not allowed', 'ephemeral': True})
            return
        if address and len(address) != 42 and address[:2] != '0x':
            await bot.send_response(msg, {'content': 'Invalid address', 'ephemeral': True})
            return

        # ev_msg = await bot.build_airdrop_message(tx_proof, address)
        balance = await bot.idena_listener.get_bna_balance(address)
        ev_msg = {'content': f'Balance of {bot.get_addr_text(address)}: **{balance:,f}** BNA'}
        if balance < 0.1:
            ev_msg['ephemeral'] = True
        sent_msg = await bot.send_response(msg, ev_msg)

    @token.sub_command(public=True)
    async def supply(msg: disnake.CommandInteraction):
        "Get the total supply of the BNA token"
        if not await bot.ratelimit(msg):
            await bot.send_response(msg, {'content': 'Command execution not allowed', 'ephemeral': True})
            return

        supply = await bot.idena_listener.get_bna_supply()
        ev_msg = {'content': f'Total supply of the BNA token: **{supply:,f}** BNA'}
        # ev_msg['ephemeral'] = True
        sent_msg = await bot.send_response(msg, ev_msg)

    @token.sub_command(options=[disnake.Option("from_address", description="Sending address", required=True, type=disnake.OptionType.string), disnake.Option("to_address", description="Destination address", required=True, type=disnake.OptionType.string), disnake.Option("amount", description="Amount of BNA tokens to send", required=True, type=disnake.OptionType.string)], public=True)
    async def send(msg: disnake.CommandInteraction, from_address: str, to_address: str, amount: str):
        "Send your BNA tokens to an address"
        if not await bot.ratelimit(msg):
            await bot.send_response(msg, {'content': 'Command execution not allowed', 'ephemeral': True})
            return
        if to_address and len(to_address) != 42 and to_address[:2] != '0x':
            await bot.send_response(msg, {'content': 'Invalid address', 'ephemeral': True})
            return

        try:
            dec_amount = Decimal(amount)
        except Exception as e:
            await bot.send_response(msg, {'content': 'Bad amount format', 'ephemeral': True})
            return

        ev_msg = await bot.build_bna_send_message(from_address, to_address, dec_amount)
        ev_msg['ephemeral'] = True
        sent_msg = await bot.send_response(msg, ev_msg)

    @disbot.slash_command(dm_permission=False)
    async def xxdev(msg: disnake.CommandInteraction):
        "Development stuff"
        pass

    @xxdev.sub_command()
    @protect(roles=[], users=[DEV_USER])
    async def check(msg: disnake.CommandInteraction):
        "Check for transfer events"
        await bot.tracker.check_events()
        await bot.tracker.check_cex_events()
        await bot.send_response(msg, {'content': 'Ok', 'ephemeral': True})

    @xxdev.sub_command(options=[disnake.Option("keep_bot_state", description="Don't reset bot's state", required=False, type=disnake.OptionType.boolean)])
    @protect(roles=[], users=[DEV_USER])
    async def reset_tracker(msg: disnake.CommandInteraction, keep_bot_state: bool = False):
        "Reset tracker state and check for notifications"
        bot.tracker._reset_state()
        if not keep_bot_state:
            bot.reset_state()
        await bot.tracker.check_events()
        await bot.tracker.check_cex_events()
        await bot.send_response(msg, {'content': 'Ok', 'ephemeral': True})

    @xxdev.sub_command(options=[disnake.Option("tx_hashes", description="TX hashes, comma separated", required=True, type=disnake.OptionType.string), disnake.Option("ev_type", description="Event type", required=False, type=disnake.OptionType.string)])
    @protect(roles=[], users=[DEV_USER])
    async def show_txs(msg: disnake.CommandInteraction, tx_hashes: str, ev_type: str = 'transfer'):
        "Show a notification for a given transaction, if possible"
        await bot.send_response(msg, {'content': 'Ok', 'ephemeral': True})
        for tx_hash in tx_hashes.split(','):
            if not tx_hash:
                continue
            await bot.tracker._emit_transfer_event(tx_hash, ev_type)

    @xxdev.sub_command(options=[disnake.Option("tx_hashes", description="TX hashes, comma separated", required=True, type=disnake.OptionType.string), disnake.Option("ev_type", description="Event type", required=False, type=disnake.OptionType.string), disnake.Option("chain", description="Chain", required=False, type=disnake.OptionType.string)])
    @protect(roles=[], users=[DEV_USER])
    async def fetch_txs(msg: disnake.CommandInteraction, tx_hashes: str, ev_type: str = 'transfer', chain: str = 'bsc'):
        "Fetch TXes and show notifications for them, if possible"
        tx_hashes = tx_hashes.split(',')
        await bot.send_response(msg, {'content': 'Fetching...', 'ephemeral': True})
        if chain == 'bsc':
            tfs = await bot.bsc_listener._fetch_txs(tx_hashes)
        elif chain == 'idena':
            tfs = await bot.idena_listener._fetch_txs(tx_hashes)
        else:
            raise Exception(f"Unknown chain: {chain}")
        await db.insert_transfers(tfs)
        for tf in tfs:
            if ev_type == 'transfer':
                ev_constructor = TransferEvent
            elif ev_type == 'interesting_transfer':
                ev_constructor = InterestingTransferEvent
            elif ev_type == 'dex':
                ev_constructor = DexEvent
            ev = ev_constructor(time=tf.timeStamp, tfs=[tf])

            bot.tracker.tracker_event_chan.put_nowait(ev)

    @xxdev.sub_command(options=[disnake.Option("event", description="Event in JSON", required=True, type=disnake.OptionType.string)])
    @protect(roles=[], users=[DEV_USER])
    async def emit(msg: disnake.CommandInteraction, event: str):
        "Post a message for a given event"
        j = json.loads(event)
        parsed_tfs = []
        for raw_tf in j['tfs']:
            log.debug(f"Parsing tf={raw_tf['hash']}")
            parsed_tf = Transfer.from_dict(raw_tf)
            parsed_tfs.append(parsed_tf)
        j['tfs'] = parsed_tfs
        bot.tracker.tracker_event_chan.put_nowait(j)
        await bot.send_response(msg, {'content': 'Ok', 'ephemeral': True})

    @xxdev.sub_command(options=[disnake.Option("message_ids", description="Messages to delete, comma separated", required=True, type=disnake.OptionType.string)])
    @protect(roles=[], users=[DEV_USER])
    async def delete(msg: disnake.CommandInteraction, message_ids: str):
        "Delete messages by ID"
        message_id_list = message_ids.split(',')
        for message_id in message_id_list:
            try:
                log.debug(f"Deleting message \"{message_id}\"")
                if '-' in message_id:
                    channel_id, message_id = message_id.split('-')
                message_id = int(message_id)
                if not message_id:
                    continue
                del_msg = disbot.get_message(message_id)
                if del_msg is None:
                    del_msg = await msg.channel.fetch_message(message_id)
                await del_msg.delete()
            except Exception as e:
                log.error(f"Failed to delete message {message_id}: {e}", exc_info=True)
        await bot.send_response(msg, {'content': 'Ok', 'ephemeral': True})

    @xxdev.sub_command()
    @protect()
    async def remove_buttons(msg: disnake.CommandInteraction, range: int=50):
        "Remove buttons from missing messages"
        await bot.send_response(msg, {'content': f'Removing buttons...', 'ephemeral': True})
        cached_messages = set([ev_msg[1].id for ev_msg in bot.ev_to_msg.values() if ev_msg[1]])
        log.debug(f"{cached_messages=}")
        async for old_msg in msg.channel.history(limit=range):
            if old_msg.author != disbot.user:
                continue
            if len(old_msg.components) > 0 and old_msg.id not in cached_messages:
                try:
                    await old_msg.edit(components=None)
                    await asyncio.sleep(0.5)
                except Exception as e:
                    log.error(f"Failed to edit message {old_msg.id}: {e}", exc_info=True)
        log.debug("Finished removing buttons")

    @xxdev.sub_command(options=[disnake.Option("add", description="User or role to add to admins", required=False, type=disnake.OptionType.mentionable), disnake.Option("remove", description="User or role to remove from admins", required=False, type=disnake.OptionType.mentionable)])
    @protect(roles=[], users=[DEV_USER])
    async def change_admins(msg: disnake.CommandInteraction, add=None, remove=None):
        "Get/set users and roles who can run xsettings commands"
        log.debug(f"Changing admins: {add=}, {remove=}")
        admin_users = bot.conf.admin_users
        admin_roles = bot.conf.admin_roles
        if add:
            if isinstance(add, disnake.Role):
                if add.id not in admin_roles:
                    admin_roles.append(add.id)
            else:
                if add.id not in admin_users:
                    admin_users.append(add.id)
        if remove:
            if isinstance(remove, disnake.Role):
                if remove.id in admin_roles:
                    admin_roles.remove(remove.id)
            else:
                if remove.id in admin_users:
                    admin_users.remove(remove.id)
        await bot.send_response(msg, {'content': f"Admin users: {', '.join(map(lambda u: f'<@{u}>', admin_users))}\nAdmin roles: {', '.join(map(lambda r: f'<@&{r}>', admin_roles))}", 'ephemeral': True})

    @disbot.slash_command(dm_permission=False)
    async def xsettings(msg: disnake.CommandInteraction):
        "Change settings"
        pass

    @xsettings.sub_command(options=[disnake.Option("add", description="Role to add to allowed", required=False, type=disnake.OptionType.mentionable), disnake.Option("remove", description="Role to remove from allowed", required=False, type=disnake.OptionType.mentionable)])
    @protect()
    async def change_command_roles(msg: disnake.CommandInteraction, add=None, remove=None):
        "Get/set roles who can run general commands"
        log.debug(f"Changing command roles: {add=}, {remove=}")
        command_roles = bot.conf.command_roles
        if add:
            if isinstance(add, disnake.Role):
                if add.id not in command_roles:
                    command_roles.append(add.id)
            else:
                await bot.send_response(msg, {'content': 'Only roles are allowed', 'ephemeral': True})
                return
        if remove:
            if isinstance(remove, disnake.Role):
                if remove.id in command_roles:
                    command_roles.remove(remove.id)
            else:
                await bot.send_response(msg, {'content': 'Only roles are allowed', 'ephemeral': True})
                return
        await bot.send_response(msg, {'content': f"Command roles: {', '.join(map(lambda r: f'<@&{r}>', command_roles))} (and all admin roles)", 'ephemeral': True})

    @xsettings.sub_command()
    @protect()
    async def stop(msg: disnake.CommandInteraction):
        "Stop (and resume) the bot."
        if not bot.stopped:
            bot.stopped = True
            await bot.send_response(msg, {'content': "Stopped the bot. Run the command again to resume it", 'ephemeral': True})
            await disbot.change_presence(status=disnake.Status.dnd, activity=disnake.Activity(type=disnake.ActivityType.watching, name="Stopped"))
        else:
            bot.stopped = False
            await bot.send_response(msg, {'content': "Resumed the bot", 'ephemeral': True})
            await disbot.change_presence(status=disnake.Status.online, activity=disnake.Activity(type=disnake.ActivityType.watching, name="⏳"))

    @xsettings.sub_command(options=[disnake.Option("hours", description="Hours between automatic stats messages (default: 8)", required=False, type=disnake.OptionType.integer, min_value=1)])
    @protect()
    async def stats_interval(msg: disnake.CommandInteraction, hours: int | None = None):
        "Get/set time in hours between automatic stats messages"
        cur_h = int(conf.tracker.stats_interval / 3600)
        if hours is None:
            await bot.send_response(msg, {'content': f"Current interval is `{cur_h}` hours", 'ephemeral': True})
        else:
            new = hours * 60 * 60 if hours > 0 else 60 * 60
            conf.tracker.stats_interval = new
            await bot.send_response(msg, {'content': f"Changed interval from `{cur_h}` to `{hours}` hours", 'ephemeral': True})

    @xsettings.sub_command(options=[disnake.Option("seconds", description="Trade notification lookback seconds (default: 600)", required=False, type=disnake.OptionType.integer, min_value=1, max_value=86400)])
    @protect()
    async def cex_volume_period(msg: disnake.CommandInteraction, seconds: int | None = None):
        "Get/set time (in seconds) how far back to look for CEX trades"
        cur = conf.tracker.cex_volume_period
        if seconds is None:
            await bot.send_response(msg, {'content': f"Current period is `{cur}` seconds", "ephemeral": True})
        else:
            conf.tracker.cex_volume_period = seconds
            await bot.send_response(msg, {'content': f"Changed period from `{cur}` to `{seconds}` seconds", "ephemeral": True})

    @xsettings.sub_command(options=[disnake.Option("amount", description="Trade volume threshold for notification, in USD (default: 500)", required=False, type=disnake.OptionType.integer, min_value=0)])
    @protect()
    async def cex_volume_threshold(msg: disnake.CommandInteraction, amount: int | None = None):
        "Get/set minimum CEX trade volume that creates a notification"
        cur = conf.tracker.cex_volume_threshold
        if amount is None:
            await bot.send_response(msg, {'content': f"Current threshold is `{cur}` USD", 'ephemeral': True})
        else:
            conf.tracker.cex_volume_threshold = int(amount)
            await bot.send_response(msg, {'content': f"Changed threshold from `{cur}` to `{amount}` USD", 'ephemeral': True})

    @xsettings.sub_command(options=[disnake.Option("hours", description="Transfer notification lookback hours (default: 1)", required=False, type=disnake.OptionType.integer, min_value=1, max_value=120)])
    @protect()
    async def recent_transfers_period(msg: disnake.CommandInteraction, hours: int | None = None):
        "Get/set time (in hours) how far back to look for transfers"
        cur = int(conf.tracker.recent_transfers_period / 3600)
        if hours is None:
            await bot.send_response(msg, {'content': f"Current period is `{cur}` hours", "ephemeral": True})
        else:
            new = hours * 60 * 60 if hours > 0 else 60 * 60
            conf.tracker.recent_transfers_period = new
            await bot.send_response(msg, {'content': f"Changed period from `{cur}` to `{int(new / 60 / 60)}` hours", "ephemeral": True})

    @xsettings.sub_command(options=[disnake.Option("amount", description="Transfer amount threshold for notification, in USD (default: 1000)", required=False, type=disnake.OptionType.integer, min_value=0)])
    @protect()
    async def recent_transfers_threshold(msg: disnake.CommandInteraction, amount: int | None = None):
        "Get/set minimum transfer amount that creates a notification"
        cur = conf.tracker.recent_transfers_threshold
        if amount is None:
            await bot.send_response(msg, {'content': f"Current threshold is `{cur}` USD", 'ephemeral': True})
        else:
            conf.tracker.recent_transfers_threshold = int(amount)
            await bot.send_response(msg, {'content': f"Changed threshold from `{cur}` to `{amount}` USD", 'ephemeral': True})

    @xsettings.sub_command(options=[disnake.Option("age", description="Termination age threshold for notification (default: 10)", required=False, type=disnake.OptionType.integer, min_value=0)])
    @protect()
    async def killtx_age_threshold(msg: disnake.CommandInteraction, age: int | None = None):
        "Get/set minimum age of terminated identity that creates a notification"
        cur = conf.tracker.killtx_age_threshold
        if age is None:
            await bot.send_response(msg, {'content': f"Current threshold is `{cur}` epochs", 'ephemeral': True})
        else:
            conf.tracker.killtx_age_threshold = int(age)
            await bot.send_response(msg, {'content': f"Changed threshold from `{cur}` to `{age}` epochs", 'ephemeral': True})

    @xsettings.sub_command(options=[disnake.Option("stake", description="Termination stake threshold for notification, in USD (default: 500)", required=False, type=disnake.OptionType.integer, min_value=0)])
    @protect()
    async def killtx_stake_threshold(msg: disnake.CommandInteraction, stake: int | None = None):
        "Get/set minimum stake of terminated identity that creates a notification"
        cur = conf.tracker.killtx_stake_threshold
        if stake is None:
            await bot.send_response(msg, {'content': f"Current threshold is `{cur}` USD", 'ephemeral': True})
        else:
            conf.tracker.killtx_stake_threshold = int(stake)
            await bot.send_response(msg, {'content': f"Changed threshold from `{cur}` to `{stake}` USD", 'ephemeral': True})

    @xsettings.sub_command(options=[disnake.Option("number", description="Threshold for the number of identities (default: 10)", required=False, type=disnake.OptionType.integer, min_value=1)])
    @protect()
    async def pool_mass_threshold(msg: disnake.CommandInteraction, number: int | None = None):
        "Get/set minimum number of identities terminated/delegated by a pool that creates a notification"
        cur = conf.tracker.pool_identities_moved_threshold
        if number is None:
            await bot.send_response(msg, {'content': f"Current threshold is `{cur}` identities", 'ephemeral': True})
        else:
            conf.tracker.pool_identities_moved_threshold = number
            await bot.send_response(msg, {'content': f"Changed threshold from `{cur}` to `{number}` identities", 'ephemeral': True})

    @xsettings.sub_command(options=[disnake.Option("volume", description="DEX volume threshold for notification, in USD (default: 500)", required=False, type=disnake.OptionType.integer, min_value=0)])
    @protect()
    async def dex_volume_threshold(msg: disnake.CommandInteraction, volume: int | None = None):
        "Get/set minimum trade volume on a DEX that creates a notification"
        cur = conf.tracker.recent_dex_volume_threshold
        if volume is None:
            await bot.send_response(msg, {'content': f"Current threshold is `{cur}` USD", 'ephemeral': True})
        else:
            conf.tracker.recent_dex_volume_threshold = int(volume)
            await bot.send_response(msg, {'content': f"Changed threshold from `{cur}` to `{volume}` USD", 'ephemeral': True})

    @xsettings.sub_command(options=[disnake.Option("channel", description="Channel where to send notifications", required=False, type=disnake.OptionType.channel)])
    @protect()
    async def notification_channel(msg: disnake.CommandInteraction, channel: disnake.TextChannel | None = None):
        "Get/set channel where to send event notifications"
        cur = disbot.get_channel(conf.discord.notif_channel)
        if channel is None:
            await bot.send_response(msg, {'content': f"Current channel is {cur.mention}", 'ephemeral': True})
        else:
            conf.discord.notif_channel = channel.id
            await bot.send_response(msg, {'content': f"Changed channel from {cur.mention if cur else 'no channel'} to {channel.mention}", 'ephemeral': True})

    @xsettings.sub_command(options=[disnake.Option("number", description="Number of command responses that will be shown to everyone (0 to not show)", required=False, type=disnake.OptionType.integer, min_value=0)])
    @protect()
    async def user_command_limit(msg: disnake.CommandInteraction, number: int | None = None):
        "Get/set the number of recent commands a user can run before being rate limited"
        cur = conf.discord.user_ratelimit_command_number
        if number is None:
            await bot.send_response(msg, {'content': f"Current number is `{cur}` commands", 'ephemeral': True})
        else:
            conf.discord.user_ratelimit_command_number = number
            await bot.send_response(msg, {'content': f"Changed channel from `{cur}` to `{number}` commands", 'ephemeral': True})

    @xsettings.sub_command(options=[disnake.Option("minutes", description="Delete messages from this many minutes ago", required=False, type=disnake.OptionType.integer, min_value=1)])
    @protect()
    async def delete_recent(msg: disnake.CommandInteraction, minutes: int = 1):
        "Delete messages sent in the last X minutes"
        await bot.send_response(msg, {'content': f'Deleting sent messages for the past `{minutes}` minutes', 'ephemeral': True})
        async for del_msg in msg.channel.history(limit=100):
            if del_msg.author != disbot.user:
                continue
            if datetime.now(timezone.utc) - del_msg.created_at.replace(tzinfo=timezone.utc) < timedelta(minutes=minutes):
                try:
                    await del_msg.delete()
                except Exception as e:
                    log.error(f"Failed to delete message {del_msg.id}: {e}", exc_info=True)
        log.debug("Finished deleting recent")

    def apply_ephemeral(in_msg: disnake.CommandInteraction, out_msg: dict):
        if in_msg.guild is None:
            # No need to hide responses in DMs
            out_msg['ephemeral'] = False
        else:
            if bot.conf.user_ratelimit_command_number <= 0:
                out_msg['ephemeral'] = True

    return bot
