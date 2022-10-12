import asyncio
import datetime as dt
import json
import os
import pprint
import sys

import pandas as pd
import websockets

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import local_directory as loc_dir

# Import API Key from Deribit
"""module <proc_config>"""
sys.path.append(loc_dir.config_main)
from api_key import deribit_key_1, deribit_key_2



LOCAL_TIMEZONE = dt.datetime.now(dt.timezone.utc).astimezone().tzinfo

async def call_api(msg):
    # wss://test.deribit.com/ws/api/v2
    # wss://www.deribit.com/ws/api/v2
    async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
        await websocket.send(msg)
        while websocket.open:
            response = await websocket.recv()
            return response

async def call_priv_api(msg, acc_num=1):

    #access_token = call_auth(deribit_key_1.client_id, deribit_key_1.client_secret)['access_token']
    if acc_num == 1:
        auth_msg = json.dumps(get_auth_creds(deribit_key_1.client_id, deribit_key_1.client_secret))
    elif acc_num == 2:
        auth_msg = json.dumps(get_auth_creds(deribit_key_2.client_id, deribit_key_2.client_secret))
    else:
        return "Invalid account number"

    async with websockets.connect(
            'wss://www.deribit.com/ws/api/v2'
            # extra_headers={"Authorization": f"Bearer {access_token}"}
    ) as websocket:
        await websocket.send(auth_msg)
        while websocket.open:
            response = await websocket.recv()
            await websocket.send(msg)
            response = await websocket.recv()
            return response

""" Public Authentication """
def get_auth_creds(cli_id, cli_secret):
    msg = {
        "id": 9929,
        "method": "public/auth",
        "params": {
            "grant_type": "client_credentials",
            "scope": "query",
            "client_id": cli_id,
            "client_secret": cli_secret
        },
        "jsonrpc": "2.0"
    }
    return msg

def call_auth(cli_id, cli_secret):

    """More like a status verification"""
    msg = get_auth_creds(cli_id, cli_secret)
    json_resp = get_json_resp(msg)
    pprint.pprint(json_resp)

    return json_resp['result'] # dict

def get_json_resp(msg):
    resp = asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))
    return json.loads(resp)

def get_priv_json_resp(msg, acc_num):
    resp = asyncio.get_event_loop().run_until_complete(call_priv_api(json.dumps(msg), acc_num))
    return json.loads(resp)

"""------------------------------------  Helper Functions  --------------------------------"""
def get_datetime_from_timestamp(unit='ms', tstz='utc', dttz='local'):
    """tstz/dttz: timezone of the timestamp and converted timezone.
        Accept values 'utc' or 'local' """
    pass

def get_timestamp_from_datetime(d, unit='ms', dttz='local', tstz='utc'):

    if dttz == 'local' and tstz=='utc':
        ts = int(d.timestamp() - 28800)
    elif dttz == 'utc' and tstz =='local':
        ts = int(d.timestamp() + 28800)
    else:
        ts = d.timestamp()

    if unit =='ms':
        ts = ts * 1000

    return ts


"""---------------------------  Websockets Public/Private Messages  ----------------------"""
def _get_msg_last_settlement(inst, count):

    msg = {
        "jsonrpc" : "2.0",
        "id" : 5482,
        "method" : "public/get_last_settlements_by_instrument",
        "params" : {
            "instrument_name" : inst,
            "type" : "settlement",
            "count" : count
        }
    }
    return msg

def _get_msg_mark_price_history(inst, start_ts, end_ts):

    msg = {
        "method" : "public/get_mark_price_history",
        "params" : {
            "instrument_name" : inst,
            "start_timestamp" : start_ts,
            "end_timestamp" : end_ts
        },
        "jsonrpc" : "2.0"
    }
    return msg

def _get_msg_index_price(inst):
    """
    Only perpetuals [?] strings accepted in format "eth_usdc"
    """
    msg = {"jsonrpc": "2.0",
           "method": "public/get_index_price",
           "id": 42,
           "params": {
               "index_name": inst.lower()}
           }
    return msg

def _get_msg_book_summary_by_instrument(inst):

    msg = {
        "jsonrpc" : "2.0",
        "id" : 3659,
        "method" : "public/get_book_summary_by_instrument",
        "params" : {
            "instrument_name" : inst # "ETH-22FEB19-140-P"
        }
    }
    return msg


def _get_msg_instruments(type, currency, expired=False):

    params = dict(currency=currency, kind=type, expired=expired)
    msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "public/get_instruments",
        "params": params
    }
    return msg


def _get_private_msg_transaction_logs(currency, start_ts, end_ts, count=100):

    msg = {
        "method": "private/get_transaction_log",
        "params": {
            "currency" : currency,
            "start_timestamp" : start_ts, #ms
            "end_timestamp" : end_ts, #ms
            "count" : count,
        },
        "jsonrpc": "2.0",
        "id" : 4
    }
    return msg


"""------------------------------------  Get DataFrame & Data Dict & Data Values ------------------------------------"""
def get_last_settlement(inst, count):

    msg = _get_msg_last_settlement(inst, count)
    json_resp = get_json_resp(msg)

    settlement_records = json_resp['result']['settlements']

    ret = pd.DataFrame.from_records(settlement_records)
    ret['timestamp'] = ret.timestamp // 1000 # ms to s
    ret['utc_datetime'] = ret.timestamp.apply(lambda x: dt.datetime.utcfromtimestamp(x))

    ret['loc_date'] = ret.timestamp.apply(lambda x:  dt.date.fromtimestamp(x).date()) # auto conversion
    ret['loc_time'] = ret.timestamp.apply(lambda x:  dt.datetime.fromtimestamp(x).time())

    # timestamp object with timezone info not supported by Excel
    # Conversion to: dtype: datetime64[ns]
    # ret['utc_time'] = [dt.datetime.fromtimestamp(date, tz=LOCAL_TIMEZONE) for date in ret.timestamp]
    # ret['timestamp'] = ret.timestamp.apply(lambda x: x.to_datetime64())

    ret = ret[['instrument_name', 'type', 'timestamp', 'utc_datetime', 'loc_date', 'loc_time',
               'mark_price', 'session_profit_loss', 'profit_loss', 'position', 'index_price']]

    return ret


def get_book_summary_by_instrument(inst):

    msg = _get_msg_book_summary_by_instrument(inst)
    json_resp = get_json_resp(msg)

    inst_summary = pd.DataFrame.from_records(json_resp['result'])

    return inst_summary


def get_index_price(inst, quote="USD"):

    """
    Parameters
    ----------
    :param: quote can take value ``USD``, ``USDC``

    """
    msg = _get_msg_index_price("_".join([inst.lower(), quote.lower()]))
    json_resp = get_json_resp(msg)

    price = json_resp['result']['index_price']
    ts = json_resp['usIn']
    time = dt.datetime.fromtimestamp(ts/1000000, tz=LOCAL_TIMEZONE)             # 1/1000 ms to s
    return pd.DataFrame(data=dict(zip(['ts', 'time', 'price'], [ts, time, price])), index=[inst])
    # return pd.DataFrame(data=[time, price], index=[inst.lower()], columns=['time', 'index_price'])


def get_option_instruments(currency, expired=False):

    msg = _get_msg_instruments("option", currency, expired)
    json_resp = get_json_resp(msg)
    instruments = json_resp["result"]
    data = pd.DataFrame.from_records(instruments)

    return data.instrument_name.unique()


def get_futures_instruments(fut_type="reversed", currency="ETH", expired=False):

    msg = _get_msg_instruments("future",currency, expired)
    json_resp = get_json_resp(msg)

    instruments = json_resp["result"]

    data = pd.DataFrame.from_records(instruments)
    if fut_type is not None:
        data = data[data.future_type=="reversed"]

    return data.instrument_name.unique()

def get_private_transaction_logs(currency, start_dt, end_dt, acc_num=1):

    start_ts = get_timestamp_from_datetime(start_dt)
    end_ts = get_timestamp_from_datetime(end_dt)

    msg = _get_private_msg_transaction_logs(currency, start_ts, end_ts)
    json_resp = get_priv_json_resp(msg, acc_num)

    # pprint.pprint(json_resp)

    if 'result' in json_resp.keys():
        data = pd.DataFrame.from_records(json_resp['result']['logs'])
    else:
        return None

    return data

def get_processed_trade_logs(raw_logs, type=None):

    raw_logs['timestamp'] = raw_logs.timestamp // 1000 # ms to s
    raw_logs['utc_datetime'] = raw_logs.timestamp.apply(lambda x: dt.datetime.utcfromtimestamp(x))
    raw_logs['loc_date'] = raw_logs.timestamp.apply(lambda x:  dt.date.fromtimestamp(x)) # auto conversion
    raw_logs['loc_time'] = raw_logs.timestamp.apply(lambda x:  dt.datetime.fromtimestamp(x).time())

    keep_cols = ['username', 'type', 'trade_id', 'instrument_name',
                 'timestamp', 'utc_datetime', 'loc_date', 'loc_time',
                 'side', 'price_currency', 'price', 'position',
                 'fee_balance', 'equity', 'currency', 'commission',
                 'change', 'cashflow', 'profit_as_cashflow', 'balance', 'amount', 'mark_price']
    processed_logs = raw_logs[keep_cols]

    if type is not None:
        processed_logs = processed_logs[processed_logs.type==type]

    return processed_logs

#/public/ticker
def get_option_mark_ul_price(inst):
    inst_summary = get_book_summary_by_instrument(inst)
    return inst_summary['underlying_price']


def get_option_px(currency, expiry: dt.date, strike, opt_type: str):
    pass


"""------------------------------------  pricing functions  ------------------------------------"""
def get_pricer(mark_spot, mark_iv=None, valDate=None, rf=0.01):
    valDate = valDate if valDate is not None else dt.date.today()
    return bsm.BSMPriceEngineCrypto(valDate, mark_spot, mark_iv, rf)



def get_option_implied_volatility(inst):

    ul_index_px = get_option_mark_ul_price(inst)




"""------------------------------------  Databases  --------------------------------------------"""

def write_excel(df, dir, filename):
    fp = os.path.join(dir, filename)
    with pd.ExcelWriter(fp, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)



#     # check if time is UTC+8 + 10 minutes
#     update_futures_settlement_price_database()
#     # send email report to me

print("Status: Ready")
