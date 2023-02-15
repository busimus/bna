from decimal import Decimal
from disnake import Color
from bna.tags import *

def shorten(addr, length=5):
    addr = addr.strip().replace('0x', '')
    return f"0x{addr[:length]}..{addr[-length:]}"

def widen(data, length=64):
    return f"0x{{:0{length}x}}".format(int(data, 16))

# Great naming convention, many are saying this
def any_in(check_these: list, for_these: list):
    "Check that any tag in `for_these` is present in `check_these`"
    return any([tag in for_these for tag in check_these])

def calculate_usd_value(tf, prices: dict, known: dict) -> float:
    value = 0
    if DEX_TAG in tf.tags:
        if any_in(tf.tags, DEX_LP_TAGS):
            for pool_addr, pool_ch in tf.meta.get('lp', {}).items():
                token_price = prices[known[known[pool_addr]['token1']]['price_id']]
                value += abs(float(pool_ch['token'])) * token_price + abs(float(pool_ch['idna'])) * prices['cg:idena']
        elif DEX_TAG_ARB in tf.tags:
            # This will usually be incorrect, but I think it should be done anyway
            for token_addr, change in tf.meta.get('token', {}).items():
                token_price = prices[known[token_addr]['price_id']]
                value += float(change) * token_price
        elif any_in(tf.tags, DEX_TRADE_TAGS):
            for token_addr, change in tf.meta.get('token', {}).items():
                token_price = prices[known[token_addr]['price_id']]
                value += abs(float(change)) * token_price
    else:
        value = abs(float(tf.value())) * prices['cg:idena']
    return value

def aggregate_dex_trades(tfs, known_addr: dict) -> dict:
    m = {'buy': Decimal(0), 'sell': Decimal(0), 'buy_usd': 0, 'sell_usd': 0, 'quote_amount': Decimal(0), 'lp_usd': 0}
    for tf in tfs:
        if any_in(tf.tags, DEX_LP_TAGS):
            lp_excess = tf.meta.get('lp_excess', Decimal(0))
            if lp_excess:
                combined_usd = tf.meta.get('usd_value', 0)
                traded_usd = combined_usd - float(abs(tf.meta.get('lp_excess', 0))) * tf.meta.get('usd_price', 0)
                lp = combined_usd - traded_usd
                m['quote_amount'] += Decimal(traded_usd)
                if lp_excess > 0:
                    m['buy'] += lp_excess
                    m['buy_usd'] += traded_usd
                else:
                    m['sell'] += lp_excess
                    m['sell_usd'] += traded_usd
                m['lp_usd'] += lp * (-1 if DEX_TAG_WITHDRAW_LP in tf.tags else 1)
            else:
                m['lp_usd'] += tf.meta.get('usd_value', 0) * (-1 if DEX_TAG_WITHDRAW_LP in tf.tags else 1)
        elif any_in(tf.tags, DEX_TRADE_TAGS):
            is_buy = DEX_TAG_BUY in tf.tags
            m['quote_amount'] += Decimal(tf.meta.get('usd_value', 0))
            m['buy_usd' if is_buy else 'sell_usd'] += tf.meta.get('usd_value', 0)
            for addr, amount in tf.changes.items():
                if known_addr.get(addr, {'type': None})['type'] == 'pool':
                    m['buy' if is_buy else 'sell'] += amount * (-1 if is_buy else 1)
    bsc_vol = m['buy'] + m['sell']
    m['avg_price'] = (float(m['quote_amount'] / bsc_vol)) if bsc_vol != 0 else 0
    return m

def average_color(colors: list[Color]) -> Color:
    avg_color = [0, 0, 0]
    c = 0
    for tf_color in colors:
        avg_color[0] += tf_color.r ** 2
        avg_color[1] += tf_color.g ** 2
        avg_color[2] += tf_color.b ** 2
        c += 1
    avg_color = (round((avg_color[0]/c)**(1/2)), round((avg_color[1]/c)**(1/2)), round((avg_color[2]/c)**(1/2)))
    return Color.from_rgb(*avg_color)

def trade_color(buy_vol, sell_vol) -> Color:
    r = buy_vol / (buy_vol + sell_vol)
    adjusted = (r*r) / (2 * (r*r - r) + 1)
    return Color.from_hsv(h=adjusted / 3, s=0.69, v=1)

def get_identity_color(address: str):
    import hashlib
    cs = [Color.from_rgb(40, 170, 225), Color.from_rgb(140, 95, 60), Color.from_rgb(55, 180, 75),
          Color.from_rgb(165, 170, 170), Color.from_rgb(245, 145, 30), Color.from_rgb(255, 70, 155),
          Color.from_rgb(145, 40, 140), Color.from_rgb(235, 30, 35), Color.from_rgb(245, 245, 240),
          Color.from_rgb(250, 235, 50)]
    hash = hashlib.sha512(address.lower().encode('utf-8')).hexdigest()
    return cs[int(hash[:11], 16) % len(cs)]
