import asyncio
import datetime as dt
import json
import os
import sys

import numpy as np
import pandas as pd
import websockets
from dateutil import parser

sys.path.append(r"C:\Users\admin\Desktop\ardgo-am-python\scripts")
import local_directory as loc_dir

sys.path.append(loc_dir.config_main)
from query_deribit import get_index_price

class DeribitOptionsData:

    def __init__(self, instrument):
        instrument = instrument.upper()
        if instrument not in ['BTC', 'ETH']:
            raise ValueError('instrument must be either BTC or ETH')
        self._instrument = instrument
        self.options = None
        self.utc_query_ts = 0
        self.loc_query_ts = 0
        self._get_options_chain()
        self.call_time = int(dt.datetime.utcnow().timestamp()) # in s


    @staticmethod
    async def call_api(msg):
        async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
            await websocket.send(msg)
            while websocket.open:
                response = await websocket.recv()
                return response

    @staticmethod
    def json_to_dataframe(response):
        response = json.loads(response)
        results = response['result']
        df = pd.DataFrame(results)
        return df

    def update(self):
        self._get_options_chain()

    @property
    def instrument(self):
        return self._instrument

    @instrument.setter
    def instrument(self, new_instrument):
        if isinstance(new_instrument, str):
            self._instrument = new_instrument
        else:
            raise ValueError('New instrument must be a string')

    @staticmethod
    def date_parser(str_date):
        date = str_date.split('-')[-1]
        try:
            res = parser.parse(date)
            return res
        except ParserError:
            print(f"str_date: {str_date}, date_str_extracted: {date}")
            return None

    @staticmethod
    def strike_parser(inst_name):
        strike = inst_name.split('-')[-2]
        return int(strike)

    @staticmethod
    def side_parser(inst_name):
        side = inst_name.split('-')[-1]
        if side == 'P':
            return 'Put'
        if side == 'C':
            return 'Call'
        else:
            return 'N/A'

    @staticmethod
    def async_loop(message):
        return asyncio.get_event_loop().run_until_complete(DeribitOptionsData.call_api(message))


    def process_df(self, df):

        # add spot column (deribit index price)
        spot_info = get_index_price(self.instrument, quote="USD").loc[self.instrument]
        df['spot_usd_price'] = spot_info.price
        df['spot_usd_time'] = spot_info.time.time()

        spot_info = get_index_price(self.instrument, quote="USDC").loc[self.instrument]
        df['spot_usdc_price'] = spot_info.price
        df['spot_usdc_time'] = spot_info.time.time()

        # add expiry column
        df['expiry'] = [DeribitOptionsData.date_parser(date) for date in df.underlying_index]
        df['expiry'] = df.expiry.apply(lambda x: x + dt.timedelta(hours=8) if x is not None else None)

        #add strike column
        df['strike'] = [DeribitOptionsData.strike_parser(i) for i in df.instrument_name]

        # add side, i.e. put or call
        df['type'] = [DeribitOptionsData.side_parser(j) for j in df.instrument_name]

        # get example option with closest expiry in order to calc dollar price of options_trading

        # spot = DeribitOptionsData.get_quote(self._instrument)

        df['dollar_bid'] = df.underlying_price * df.bid_price
        df['dollar_ask'] = df.underlying_price * df.ask_price
        df['dollar_mid'] = df.underlying_price * df.mid_price

        # utctimestamp in s
        df['utc_ts'] = np.full(shape=(df.shape[0],1), fill_value=self.utc_query_ts)

        # return utc time = local time - 8h
        df['utc_dt'] = df.utc_ts.apply(lambda x: dt.datetime.fromtimestamp(x))

        df['loc_date'] = df.utc_ts.apply(lambda x:  dt.date.fromtimestamp(x + 28800)) # auto conversion
        df['loc_time'] = df.utc_ts.apply(lambda x:  dt.datetime.fromtimestamp(x + 28800).time())

        #create time to expiry (toe) as float
        df['toe'] = (df['expiry'] - dt.datetime.fromtimestamp(self.utc_query_ts)).dt.days / 365

        return df


    @staticmethod
    def get_quote(instrument):
        msg1 = \
            {
                "jsonrpc": "2.0",
                "id": 9344,
                "method": "public/ticker",
                "params": {
                    "instrument_name": instrument +'-PERPETUAL',
                    "kind": "future"
                }
            }
        quote = json.loads(DeribitOptionsData.async_loop(json.dumps(msg1)))
        return float(quote['result']['last_price'])

    @property
    def chain(self):
        return self.options

    def _get_options_chain(self):
        msg1 = \
            {
                "jsonrpc": "2.0",
                "id": 9344,
                "method": "public/get_book_summary_by_currency",
                "params": {
                    "currency": self._instrument,
                    "kind": "option"
                }
            }

        self.utc_query_ts = int(dt.datetime.utcnow().timestamp())
        self.loc_query_ts = self.utc_query_ts + 28800

        response = self.async_loop(json.dumps(msg1))

        time_elapsed = dt.datetime.utcnow().timestamp() - self.utc_query_ts # timedelta.seconds

        print(f"Time elapsed in seconds between query and response returns... {time_elapsed:.0f}")

        df = self.json_to_dataframe(response)

        df = self.process_df(df) # modified

        self.options = df.copy(deep=True)

    def available_instruments(self, currency, expired=False):
        msg = \
            {
                "jsonrpc": "2.0",
                "id": 9344,
                "method": "public/get_instruments",
                "params": {
                    "currency": currency,
                    "kind": "option",
                    "expired": expired
                }
            }
        resp = self.async_loop(json.dumps(msg))
        resp = json.loads(resp)
        instruments = [d["instrument_name"] for d in resp['result']]
        return instruments

    @classmethod
    def option_info(cls, option_label):
        msg = \
            {
                "jsonrpc": "2.0",
                "id": 8106,
                "method": "public/ticker",
                "params": {
                    "instrument_name": option_label
                }
            }

        response = DeribitOptionsData.async_loop(json.dumps(msg))
        return json.loads(response)

    def expiries(self):
        return sorted(self.options.expiry.unique())

    def get_side_exp(self, side, exp='all'):
        if side.capitalize() not in ['Call', 'Put']:
            raise ValueError("Side must be 'Call' or 'Put'")
        if exp == 'all':
            return self.options[self.options['type'] == side]
        else:
            return self.options[(self.options.expiry == exp) & (self.options['type'] == side)]

    def save_csv(self, dir=loc_dir.db_crypto):

        filename = "dbt_option_chain_raw_" + self._instrument + "_"
        filename = filename + dt.datetime.fromtimestamp(self.loc_query_ts).strftime("%Y%m%d_%H%M%S")
        filename = filename + ".csv"
        self.options.to_csv(os.path.join(dir,
                                         "options",
                                         "chain",
                                         "raw",
                                         self._instrument,
                                         filename),
                            index=False)

