from bna.tags import *
from disnake import Color

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

rpc_api_type_map = {'SendTx': 'send',
 'ActivationTx': 'activation',
 'InviteTx': 'invite',
 'KillTx': 'kill',
 'SubmitFlipTx': 'killInvitee',
 'SubmitAnswersHashTx': 'submitFlip',
 'SubmitShortAnswersTx': 'submitAnswersHash',
 'SubmitLongAnswersTx': 'submitShortAnswers',
 'EvidenceTx': 'submitLongAnswers',
 'OnlineStatusTx': 'evidence',
 'KillInviteeTx': 'online',
 'ChangeGodAddressTx': 'changeGodAddress',
 'BurnTx': 'burn',
 'ChangeProfileTx': 'changeProfile',
 'DeleteFlipTx': 'deleteFlip',
 'DeployContract': 'deployContract',
 'CallContract': 'callContract',
 'TerminateContract': 'terminateContract',
 'DelegateTx': 'delegate',
 'UndelegateTx': 'undelegate',
 'KillDelegatorTx': 'killDelegator',
 'StoreToIpfsTx': 'storeToIpfs',
 'ReplenishStakeTx': 'replenishStake'}

def average_color(colors: list[Color]) -> Color:
    avg_color = [0, 0, 0]
    c = 0
    for tf_color in colors:
        avg_color[0] += tf_color.r ** 2
        avg_color[1] += tf_color.g ** 2
        avg_color[2] += tf_color.b ** 2
        c += 1
    avg_color = (int((avg_color[0]/c)**(1/2)), int((avg_color[1]/c)**(1/2)), int((avg_color[2]/c)**(1/2)))
    return Color.from_rgb(*avg_color)
