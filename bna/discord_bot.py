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
from bna.tracker import Tracker
from bna.transfer import CHAIN_BSC, CHAIN_IDENA, Transfer
from bna.utils import any_in, average_color, shorten

MAX_PUBLISH_ATTEMPTS = 3



class Bot:
    def __init__(self, bot: commands.InteractionBot, conf: DiscordBotConfig, db: Database, dev_user: int, log: Logger):
        from bna.bsc_listener import BscListener
        self.disbot = bot
        self.conf = conf
        self.db = db
        self.dev_user = dev_user
        self.tracker: Tracker = None
        self.bsc_listener: BscListener = None
        self.log = log.getChild('DI')
        self.stopped = False
        # For ratelimiting
        queue_size = max(16, self.conf.user_ratelimit_command_number)  # large size to allow for variable sleep length
        self.user_cmd_times = defaultdict(lambda: deque(maxlen=queue_size))  # TODO: leaks slowly
        self.activity = {'block': {CHAIN_IDENA: 0, CHAIN_BSC: 0}}
        self.last_activity_update = time.time()

    async def run_publisher(self, tracker_event_chan: asyncio.Queue):
        self.log.info("Publisher started")
        start = datetime.now()
        while True:
            try:
                ev = await tracker_event_chan.get()
            except asyncio.CancelledError:
                break
            if datetime.now() - start < timedelta(seconds=5) and ev['type'] != 'block':
                self.log.warning(f"Not publishing old event: {ev}")
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

    async def publish_event(self, ev: dict):
        attempt = 0
        while attempt < MAX_PUBLISH_ATTEMPTS:
            try:
                await self._publish_event(ev)
                if ev['type'] != 'block':
                    await asyncio.sleep(0.5)
                break
            except Exception as e:
                self.log.error(f"Publish error: {e}", exc_info=True)
                await asyncio.sleep(5)
                attempt += 1

    async def _publish_event(self, ev: dict):
        msg = None
        if ev['type'] == 'stats':
            msg = self.build_stats_message(ev)
        elif ev['type'] in ['transfer', 'dex', 'interesting_transfer']:
            msg = self.build_transfer_message(ev)
        elif ev['type'] == 'kill':
            msg = self.build_kill_message(ev)
        elif ev['type'] == 'mass_pool':
            msg = self.build_mass_pool_message(ev)
        elif ev['type'] == 'cex_trade':
            msg = self.build_cex_trade_message(ev)
        elif ev['type'] == 'block':
            await self.update_activity(ev)
        else:
            self.log.warning(f"Unknown event type: {ev['type']}")
        if msg:
            await self.send_notification(msg)

    async def send_notification(self, msg: dict):
        if self.stopped:
            self.log.warning("Stopped, not sending the notification")
            return
        channel = self.disbot.get_channel(self.conf.notif_channel)
        if channel is None:
            self.log.warning("No channel, can't send messages yet")
            return
        await channel.send(**msg)

    def build_stats_message(self, ev: dict, usd = True) -> dict:
        color = Color.blurple()
        hours = int(round(ev['period'] / (60 * 60), 0))
        embeds = []
        ce = Embed(color=color, title=f"Chain stats for the past {hours} hours")
        embeds.append(ce)
        s = ev['stats']
        ce.add_field('Stake +/-', f"{s['staked']:,}/{s['unstaked']:,} iDNA", inline=True)
        ce.add_field('Bridged to/from BSC', f"{s['bridged_to_bsc']:,}/{s['bridged_from_bsc']:,} iDNA", inline=True)
        ce.add_field('Burned', f"{s['burned']:,} iDNA", inline=True)
        ce.add_field('Terminated', f"{s['identities_killed']:,} identities", inline=True)
        ce.add_field('Invites sent/activated', f"{s['invites_issued']:,}/{s['invites_activated']:,}", inline=True)
        ce.add_field('Contract calls', f"{s['contract_calls']}", inline=True)

        if len(s['markets']) > 0:
            if usd:
                buy_usd_vol = sum([m['buy_usd'] for m in s['markets'].values()])
                sell_usd_vol = sum([m['sell_usd'] for m in s['markets'].values()])
                desc = f"Buy/Sell volume: **${buy_usd_vol:,.0f}** / **${sell_usd_vol:,.0f}**"
            else:
                buy_vol = sum([m['buy'] for m in s['markets'].values()])
                sell_vol = sum([m['sell'] for m in s['markets'].values()])
                desc = f"Buy/Sell volume: **{buy_vol:,.0f}** / **{sell_vol:,.0f} iDNA**"
            me = Embed(color=Color.og_blurple(), title=f"Market stats for the past {hours} hours", description=desc)
            embeds.append(me)
            for m_name, m in s['markets'].items():
                me.add_field('Market', MARKETS[m_name]['link'], inline=True)
                if usd:
                    me.add_field('Buy/Sell', f"${m['buy_usd']:,.0f} / ${m['sell_usd']:,.0f}", inline=True)
                else:
                    me.add_field('Buy/Sell', f"{m['buy']:,.0f} / {m['sell']:,.0f} iDNA", inline=True)
                if m.get('avg_price'):
                    if usd:
                        me.add_field('Avg Price', m['avg_price_usd'], inline=True)
                    else:
                        me.add_field('Avg Price', m['avg_price'], inline=True)
                else:
                    me.add_field('​', '​', inline=True)

        return {'embeds': embeds}

    def build_transfer_message(self, ev: dict) -> dict:
        MAX_TOP_TFS = 10
        dt = ev['time']
        tfs = ev['tfs']
        if ev['type'] == 'transfer':
            addr = self.get_addr_text(ev['by'], 'address', chain=ev['tfs'][0].chain)
            desc = f"Address: {addr}"
            title = 'Large transfer volume'
        elif ev['type'] == 'interesting_transfer':
            addr = self.get_addr_text(ev['by'], 'address', chain=ev['tfs'][0].chain)
            desc = f"Address: {addr}"
            title = 'Interesting trasnfer'
        elif ev['type'] == 'dex':
            addr = None
            desc = None
            title = 'Large DEX volume'
        else:
            raise Exception('Unsupported event type')

        try:
            tfs.sort(key=lambda tf: tf.meta.get('usd_value', 0), reverse=True)
            dtfs = list(map(self.describe_tf, tfs))
            if len(tfs) == 1:
                dtf = dtfs[0]
                em = Embed(title=dtf['title'], description=dtf.get('desc'), color=dtf['color'], url=dtf['url'], timestamp=dt)
                em.add_field("Address", addr if addr else self.get_addr_text(dtf['by'], 'address', chain=ev['tfs'][0].chain), inline=True)
                em.add_field("Value", f"${dtf['value']:,.2f}", inline=True)
                if 'price' in dtf:
                    em.add_field("Price", f"${dtf['price']:.3f}", inline=True)
            else:
                em = Embed(title=title, timestamp=dt)
                tf_text = ''
                truncated = ev.get('truncated', 0)
                for i, dtf in enumerate(dtfs):
                    tf_text += f"\n{i+1}. {dtf['short']}"
                    if i >= MAX_TOP_TFS - 1:
                        break
                if i != len(dtfs) - 1 or truncated:
                    tf_text += f'\nAnd {len(dtfs) - i - 1 + truncated:,} others'

                if ev['type'] == 'dex':
                    ratio = ev['buy_usd'] / (ev['buy_usd'] + ev['sell_usd'])
                    em.color = Color.from_hsv(h=ratio / 3, s=0.69, v=1)
                    em.add_field("Avg Price", f"${ev['avg_price']:.3f}", inline=True)
                    em.add_field("Buy/Sell Volume", f"${ev.get('buy_usd', 0):,.2f} / ${ev.get('sell_usd', 0):,.2f}", inline=True)
                else:
                    em.color = average_color(map(lambda tf: tf['color'], dtfs))

                if desc:
                    em.description = f"{desc}\n\n**Largest transactions**{tf_text}"
                else:
                    em.description = f"**Largest transactions**{tf_text}"
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
        if ev['type'] == 'interesting_transfer':
            em.color = Color.from_rgb(79, 36, 203)
            em.title = f"Interesting transfer: {em.title}"
        print(em.to_dict())
        output = {'embed': em}
        return output

    def build_kill_message(self, ev: dict) -> dict:
        color = Color.from_rgb(32, 32, 32)
        dt = ev['time']
        desc = f"Identity: {self.get_addr_text(ev['addr'], 'identity')}"
        e = Embed(color=color, title=f"Termination", description=desc,
                  url=f"https://scan.idena.io/transaction/{ev['hash']}", timestamp=dt)
        e.set_thumbnail(f'https://robohash.idena.io/{ev["addr"]}')
        if ev.get('age'):
            e.add_field("Age", ev['age'], inline=True)
        if ev.get('stake'):
            e.add_field("Stake", f"{abs(int(ev['stake'])):,} iDNA", inline=True)
        if ev.get('pool'):
            e.add_field("Pool", self.get_addr_text(ev['pool'], 'pool'), inline=True)
        return {'embed': e}

    def build_mass_pool_message(self, ev: dict) -> dict:
        if ev['subtype'] == 'kill':
            color = Color.dark_red()
            name = 'termination'
        elif ev['subtype'] == 'delegate':
            color = Color.dark_green()
            name = 'delegation'
        elif ev['subtype'] == 'undelegate':
            color = Color.dark_red()
            name = 'undelegation'
        else:
            self.log.warning(f"Unknown mass pool subtype: {ev['subtype']}")
            color = Color.dark_blue()

        dt = max([e['time'] for e in ev['tfs']])
        tfs = sorted(ev['tfs'], key=lambda ev: ev['stake'], reverse=True)
        desc = f"Pool: {self.get_addr_text(ev['pool'], 'pool')}"
        if ev['stake'] or ev.get('age'):
            desc = f"\nTotal stake: **{int(ev['stake']):,}** iDNA, Total age: **{ev['age']:,}**"
        idents_text = ''
        LINES = 5
        for i, k in enumerate(tfs):
            idents_text += f"\n{i+1}. {self.get_addr_text(k['addr'], 'identity', short=False)}"
            if k.get('stake') or k.get('age'):
                idents_text += f" ({int(k.get('stake', 0)):,} iDNA, age: {k.get('age', 0)})"
            if i >= (LINES - 1) and len(tfs) > LINES:
                idents_text += f'\nAnd {len(tfs) - LINES:,} others'
                break
        desc += f'\n\n**Identities**{idents_text}'
        e = Embed(title=f"Mass pool {name}", description=desc, color=color,
                  url=f"https://scan.idena.io/pool/{ev['pool']}", timestamp=dt)
        e.set_thumbnail(f'https://robohash.idena.io/{ev["pool"]}')
        return {'embed': e}

    def build_pool_stats_message(self, ev: dict, long: bool = False) -> dict:
        color = Color.dark_blue()
        fields = [{'title': "Terminated identities", 'field': 'killed'},
                  {'title': "Delegated identities", 'field': 'delegated'},
                  {'title': "Undelegated identities", 'field': 'undelegated'},]
        max_lines = 5 if not long else 10
        pools = ev['stats']
        hours = int(round(ev['period'] / (60 * 60), 0))
        e = Embed(color=color, title=f"Pool stats for the past {hours} hours")
        for field in fields:
            field_text = ''
            field_pools = map(lambda i: (i[0], i[1][field['field']]), pools.items())
            field_pools = sorted(filter(lambda p: p[1] > 0, field_pools), key=lambda t: t[1], reverse=True)
            for i, pool in enumerate(field_pools):
                if pool[1] == 0:
                    break
                field_text += '\n' if i != 0 else ''
                field_text += f"{i+1}. {self.get_addr_text(pool[0], 'pool', no_link=False, short_len=4, short=True)}: {pool[1]:,}"
                if i == max_lines - 1 and len(field_pools) > max_lines:
                    rest_sum = sum([p[1] for p in field_pools[i+1:]])
                    field_text += f'\nAnd {len(field_pools) - max_lines} others with {rest_sum:,} identitites'
                    break
            if len(field_text) > 0:
                e.add_field(field['title'], field_text, inline=True)
        return {'embed': e}

    def build_cex_trade_message(self, ev: dict) -> dict:
        ratio = ev['total_buy_val'] / (ev['total_buy_val'] + ev['total_sell_val'])
        self.log.debug(f"{ratio=}")
        color = Color.from_hsv(h=ratio / 3, s=0.69, v=1)

        desc = f'Total buy/sell volume: **${ev["total_buy_val"]:,.0f}** / **${ev["total_sell_val"]:,.0f}**'
        em = Embed(color=color, title=f"Large CEX volume", description=desc)
        for m_name, m in ev['markets'].items():
            em.add_field('Market', MARKETS[m_name]['link'], inline=True)
            em.add_field('Buy/Sell Volume', f"${m['buy_usd_value']:,.0f} / ${m['sell_usd_value']:,.0f}", inline=True)
            if m.get('avg_price'):
                em.add_field('Avg Price', f"${m['avg_price']:.3f}", inline=True)
            else:
                em.add_field('​', '​', inline=True)
        return {'embed': em}

    def build_top_message(self, ev: dict) -> dict:
        DESC_LIMIT = 4096
        desc = f'Total value: **${ev["total_usd_value"]:,.0f}**'
        # @TODO: clean this up
        if ev['top_type'] in ['kill', 'stake']:
            desc += f' (**{ev["total_idna"]:,.0f}** iDNA)'
        if ev['top_type'] == 'kill':
            desc += f'. Total age: **{ev["total_age"]:,}**'
        tf_texts = []
        total_len = 0
        tfs: list[Transfer] = ev['tfs']
        didnt_fit = 0
        for i, tf in enumerate(tfs):
            if ev['top_type'] == 'kill':
                killed = self.get_addr_text(tf.meta['killedIdentity'], type_='identity', short_len=4)
                stake = abs(list(tf.changes.values())[0])
                tf_texts.append(f"{i+1}. {killed} ({stake:,.0f} iDNA, age: {tf.meta['age']})")
            elif ev['top_type'] == 'stake':
                addr = self.get_addr_text(tf.signer, type_='identity')
                tf_texts.append(f"{i+1}. {addr} ({tf.value():,.0f} iDNA)")
            elif ev['top_type'] == 'dex':
                dtf = self.describe_tf(tf)
                tf_texts.append(f"{i+1}. {dtf['short']}")
            elif ev['top_type'] == 'transfer':
                try:
                    dtf = self.describe_tf(tf)
                except Exception as e:
                    self.log.error(f"{tf.hash}")
                    continue
                tf_texts.append(f"{i+1}. {dtf['short']}")
            total_len += len(tf_texts[-1])
            if total_len >= DESC_LIMIT - 500:  # some margin
                didnt_fit = len(tfs) - i + 1
                break
        if didnt_fit or ev["truncated"]:
            tf_texts.append(f'And {didnt_fit + ev["truncated"]:,} others')
        tf_text = '\n'.join(tf_texts)
        past = f'for the past {int(ev["period"] / 60 / 60)} hours'
        desc = f"{desc}\n{tf_text}"
        if ev['top_type'] == 'kill':
            em = Embed(title=f'Top kills {past}', description=desc, color=Color.dark_red())
        elif ev['top_type'] == 'stake':
            em = Embed(title=f'Top stake replenishments {past}', description=desc, color=Color.dark_green())
        elif ev['top_type'] == 'dex':
            em = Embed(title=f'Top DEX transactions {past}', description=desc, color=Color.gold())
        elif ev['top_type'] == 'transfer':
            em = Embed(title=f'Top transfers {past}', description=desc, color=Color.from_rgb(129, 190, 238))

        return {'embed': em}

    def describe_tf(self, tf: Transfer) -> dict:
        "Turns a Transfer into a dict that can [almost] be displayed as an Embed."
        self.log.debug(f"Describing: {tf}")
        d = {'title': 'Transfer', 'desc': '', 'short': self.get_tx_text(tf, 'Unknown transfer'),
             'value': tf.meta.get('usd_value', 0), 'by': tf.signer, 'tf': tf,
             'chain': tf.chain, 'color': Color.from_rgb(129, 190, 238), 'url': self.get_tf_url(tf)}
        if 'usd_price' in tf.meta:
            d['price'] = tf.meta['usd_price']
        if IDENA_TAG_SEND in tf.tags or len(tf.tags) == 0:
            d.update({'title': 'Transfer',
                      'desc': f'Sent **{int(tf.value()):,}** iDNA to {self.get_addr_text(tf.to(single=True), chain=tf.chain)}',
                      'short': self.get_tx_text(tf, f'Sent to {shorten(tf.to(single=True))}')})
        elif IDENA_TAG_BRIDGE_BURN in tf.tags:
            d.update({'title': 'Bridge transfer to BSC',
                      'desc': f'Bridged **{int(tf.value()):,}** iDNA to BSC address {self.get_addr_text(tf.meta["bridge_to"], chain="bsc")}',
                      'short': self.get_tx_text(tf, f'Bridged to BSC: {shorten(tf.meta["bridge_to"])}'),
                      'color': Color.light_grey()})
        elif IDENA_TAG_STAKE in tf.tags:
            d.update({'title': 'Stake replenishment',
                      'desc': f'Replenished stake with **{int(tf.value()):,}** iDNA',
                      'short': self.get_tx_text(tf, f'Replenished stake'),
                      'thumbnail': f'https://robohash.idena.io/{tf.signer}',
                      'color': Color.dark_green()})
        elif IDENA_TAG_BURN in tf.tags:
            d.update({'title': 'Coin burn',
                      'desc': f'Burned **{int(tf.value()):,}** iDNA',
                      'short': self.get_tx_text(tf, f'Burned coins'),
                      'color': Color.orange()})
        elif any_in(tf.tags, [IDENA_TAG_KILL, IDENA_TAG_KILL_DELEGATOR]):
            d.update({'title': 'Identity killed',
                      'desc': f'Killed at age {tf.meta["age"]} and unlocked **{int(tf.value(recv=True)):,}** iDNA',
                      'short': self.get_tx_text(tf, f'Identity killed'),
                      'color': Color.from_rgb(32, 32, 32)})
        elif IDENA_TAG_DEPLOY in tf.tags:
            d.update({'title': 'Contract deployment',
                      'desc': f'Deployed contract {self.get_addr_text(tf.meta["call"]["contract"], type_="contract")}' + f' with **{int(tf.value()):,}** iDNA' if int(tf.value()) > 0 else '',
                      'short': self.get_tx_text(tf, f'Deployed contract'),
                      'color': Color.blue()})
        elif IDENA_TAG_CALL in tf.tags:
            d.update({'title': 'Contract call',
                      'desc': f'Called contract {self.get_addr_text(tf.meta["call"]["contract"], type_="contract")}' + f' with **{int(tf.value()):,}** iDNA' if int(tf.value()) > 0 else '',
                      'short': self.get_tx_text(tf, f'Called contract'),
                      'color': Color.blue()})
        elif BSC_TAG_BRIDGE_BURN in tf.tags:
            d.update({'title': 'Bridge transfer to Idena',
                      'desc': f'Bridged **{int(tf.value()):,}** iDNA to Idena',
                      'short': self.get_tx_text(tf, f'Bridged to Idena'),
                      'color': Color.light_grey()})
        elif BSC_TAG_BRIDGE_MINT in tf.tags:
            d.update({'title': 'Bridge transfer from Idena',
                      'desc': f'Bridged **{int(tf.value()):,}** iDNA to BSC address {self.get_addr_text(tf.to(True))}',
                      'short': self.get_tx_text(tf, f'Bridged from Idena: {shorten(tf.to(single=True))}'),
                      'color': Color.light_grey()})
        elif DEX_TAG not in tf.tags:
            self.log.warning(f"Can't describe transfer: {tf}")
        if 'usd_value' in tf.meta:
            d['short'] += f" (${tf.meta['usd_value']:,.2f})"
        else:
            d['short'] += f" ({int(tf.value()):,} iDNA)"

        if DEX_TAG in tf.tags:
            liq_str = ''
            if any_in(tf.tags, DEX_LP_TAGS):
                for pool_addr, pool_ch in tf.meta['lp'].items():
                    liq_str += ', ' if liq_str else ''
                    ticker = self.db.known[self.db.known[pool_addr]['token1']]['name']
                    liq_str += f"**{abs(float(pool_ch['token'])):,.2f}** {ticker} + **{abs(int(pool_ch['idna'])):,}** iDNA"
                excess = tf.meta.get('lp_excess', 0)
                if DEX_TAG_PROVIDE_LP in tf.tags and excess != 0:
                    liq_str = f"{'Bought' if excess > 0 else 'Sold'} **{int((abs(excess))):,}** iDNA and deposited {liq_str}"
                elif DEX_TAG_WITHDRAW_LP in tf.tags and excess != 0:
                    liq_str = f"Withdrawn {liq_str} and {'bought' if excess > 0 else 'sold'} **{int((abs(excess))):,}** iDNA"
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
                # d['short'] += f" ({int(tf.value())} iDNA)"
            elif DEX_TAG_BUY in tf.tags:
                d.update({'title': 'DEX buy',
                          'short': self.get_tx_text(tf, f'Bought iDNA'),
                          'color': Color.green()})
                # d['short'] += f" ({int(tf.value())} iDNA)"
            elif DEX_TAG_SELL in tf.tags:
                d.update({'title': 'DEX sell',
                          'short': self.get_tx_text(tf, f'Sold iDNA'),
                          'color': Color.red()})
            if 'usd_value' in tf.meta:
                d['short'] += f" (${tf.meta['usd_value']:,.2f})"
            else:
                d['short'] += f" ({int(tf.value()):,} iDNA)"

            if any_in(tf.tags, DEX_TRADE_TAGS) and not any_in(tf.tags, DEX_LP_TAGS):
                for_tokens = ''
                # Sorting the change by name for consistent order
                tokens = map(lambda i: (self.db.known[i[0]]['name'], abs(float(i[1]))), tf.meta.get('token', {}).items())
                tokens = sorted(list(tokens), key=lambda t: t[0])
                for ticker, change in tokens:
                    for_tokens += ' + ' if for_tokens else ''
                    for_tokens += f'**{change:,.2f}** {ticker}'
                d['desc'] = f'{"Bought" if DEX_TAG_BUY in tf.tags else "Sold"} **{int(tf.value()):,}** iDNA for {for_tokens}'

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
            return f"[{name}]({url}/{type_}/{addr} \'{addr}\')"
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

    async def update_activity(self, ev: dict):
        self.activity[ev['type']].update({ev['chain']: ev['value']})
        if time.time() - self.last_activity_update < 10:
            return
        act_str = f"IDNA: {self.activity['block'][CHAIN_IDENA]} BSC: {self.activity['block'][CHAIN_BSC]}"
        self.last_activity_update = time.time()
        await self.disbot.change_presence(status=disnake.Status.online, activity=disnake.Activity(type=disnake.ActivityType.watching, name=act_str))

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

        ev_msg = bot.build_top_message(ev)
        apply_ephemeral(msg, ev_msg)
        if long:
            ev_msg['ephemeral'] = True
        await bot.send_response(msg, ev_msg)

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
        await bot.send_response(msg, ev_msg)

    @disbot.slash_command(options=[disnake.Option("hours", description="Collect pool stats from this far back", required=False, type=disnake.OptionType.integer, min_value=1, max_value=720), disnake.Option("long", description="Show as many lines as possible", required=False, type=disnake.OptionType.boolean)])
    @protect(roles=command_roles, public=True)
    async def pools(msg: disnake.CommandInteraction, hours: int = 24, long: bool = False):
        "Show recent pool stats"
        if not await bot.ratelimit(msg):
            await bot.send_response(msg, {'content': 'Command execution not allowed', 'ephemeral': True})
            return
        ev = await bot.tracker.generate_pool_stats(period=hours * 60 * 60)
        if not ev:
            await bot.send_response(msg, {'content': 'No pool events in the given time range', 'ephemeral': True})
        ev_msg = bot.build_pool_stats_message(ev, long=long)
        apply_ephemeral(msg, ev_msg)
        if long:
            ev_msg['ephemeral'] = True
        # ev_msg['view'] = EventView(log)
        await bot.send_response(msg, ev_msg)

    @disnake.ui.button(label="Show more", style=disnake.ButtonStyle.secondary, custom_id="show_more")
    async def show_more(self, button: disnake.ui.Button, msg: disnake.MessageInteraction):
        log.info(f"Button: {button}")
        log.info(f"Msg: {msg}")
        await bot.send_response(msg, {'content': 'Ok', 'ephemeral': False})

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

    @xxdev.sub_command()
    @protect(roles=[], users=[DEV_USER])
    async def reset_tracker(msg: disnake.CommandInteraction):
        "Reset tracker state and check for notifications"
        bot.tracker._reset_state()
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

    @xxdev.sub_command(options=[disnake.Option("tx_hashes", description="TX hashes, comma separated", required=True, type=disnake.OptionType.string), disnake.Option("ev_type", description="Event type", required=False, type=disnake.OptionType.string)])
    @protect(roles=[], users=[DEV_USER])
    async def fetch_txs(msg: disnake.CommandInteraction, tx_hashes: str, ev_type: str = 'transfer'):
        "Fetch TXes and show notifications for them, if possible"
        tx_hashes = tx_hashes.split(',')
        await bot.send_response(msg, {'content': 'Fetching...', 'ephemeral': True})
        tfs = await bot.bsc_listener._fetch_txs(tx_hashes)
        await db.insert_transfers(tfs)
        for tf in tfs:
            e = {'type': ev_type, 'by': tf.signer, 'value': tf.meta.get('usd_value', 0),
                 'tfs': [tf], 'time': tf.timeStamp}
            bot.tracker.tracker_event_chan.put_nowait(e)

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
        async for del_msg in msg.channel.history(limit=200):
            if del_msg.author != disbot.user:
                continue
            if datetime.now(timezone.utc) - del_msg.created_at.replace(tzinfo=timezone.utc) < timedelta(minutes=minutes):
                try:
                    await del_msg.delete()
                except Exception as e:
                    log.error(f"Failed to delete message {del_msg.id}: {e}", exc_info=True)

    def apply_ephemeral(in_msg: disnake.CommandInteraction, out_msg: dict):
        if in_msg.guild is None:
            # No need to hide responses in DMs
            out_msg['ephemeral'] = False
        else:
            if bot.conf.user_ratelimit_command_number <= 0:
                out_msg['ephemeral'] = True

    return bot
