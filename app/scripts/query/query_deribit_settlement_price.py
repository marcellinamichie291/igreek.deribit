import sys
import os
import json
import asyncio
import websockets
import datetime
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import local_directory as loc_dir

sys.path.append(loc_dir.config_main)
from db_config import get_db_connection


def get_msg(index_name):
    msg = {
        "jsonrpc": "2.0",
        "id": 3601,
        "method": "public/get_delivery_prices",
        "params": {"index_name": index_name, "offset": 0, "count": 99}
    }
    return msg


async def call_api(msg):
    async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
        await websocket.send(msg)
        while websocket.open:
            response = await websocket.recv()
            return response


def get_json_resp(msg):
    resp = asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))
    return json.loads(resp)


try:
    index_names = ['btc_usd', 'eth_usd']
    for index in index_names:
        cur_time = datetime.datetime.utcnow()

        data1 = get_json_resp(get_msg(index))
        data2 = data1['result']['data']

        if len(data2) > 0:
            df1 = pd.DataFrame.from_records(data2)
            df1 = df1.rename(columns={'delivery_price': 'settlement_price', 'date': 'settlement_date'})
            df1['settlement_price'] = df1['settlement_price'].fillna(0.0)
            df1['settlement_date'] = df1['settlement_date'].fillna('1970-01-01')
            df1['settlement_date'] = df1['settlement_date']+' 08:00:00'
            df1['currency_base'] = index.split('_')[0]
            df1['currency_quote'] = index.split('_')[1]
            df1['modify_time'] = cur_time
            df1['modify_time'] = pd.to_datetime(df1['modify_time'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
            df1 = df1.fillna('')

            keys = df1.columns.to_list()
            stmt_key = ", ".join(keys)
            stmt_val = ", ".join(["%s" for i in keys])
            stmt = f"REPLACE INTO settlement_price_deribit ({stmt_key}) VALUES ({stmt_val})"

            conn = get_db_connection()
            cur = conn.cursor()
            cur.executemany(stmt, df1.values.tolist())
            conn.commit()
            conn.close()

            print(f'{cur_time.strftime("%Y-%m-%d %H:%M:%S")}: {index} success, rows {len(data2)}')
        else:
            print(f'{cur_time.strftime("%Y-%m-%d %H:%M:%S")}: {index} fail, rows 0')
except Exception as e:
    print(e)
