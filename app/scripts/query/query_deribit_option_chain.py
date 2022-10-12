import asyncio
import datetime as dt
import json
import os
import sys

import numpy as np
import pandas as pd
import websockets
from dateutil import parser

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import local_directory as loc_dir

# sys.path.append(loc_dir.config_main)
from query_deribit import get_index_price
from implied_volatility import find_iv

"""module <proc_config>"""
sys.path.append(loc_dir.config_main)
from db_config import get_db_connection


class DeribitOptionsData:

    def __init__(self, instrument, log_file):

        self._log = log_file

        self.call_time = int(dt.datetime.now().timestamp()) # in s
        self._log.write(f"\nProc Run Start Time: {dt.datetime.fromtimestamp(self.call_time)}")

        instrument = instrument.upper()

        try:
            assert instrument in ['BTC', 'ETH']
        except AssertionError:
            self._log.write(ValueError('instrument must be either BTC or ETH'))

        self._instrument = instrument
        self.options = None
        self.utc_query_ts = 0
        self.loc_query_ts = 0
        self._get_option_chain()


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
        self._get_option_chain()

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
        except parser.ParserError:
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
            return 'p'
        if side == 'C':
            return 'c'
        else:
            return 'N/A'

    @staticmethod
    def async_loop(message):
        return asyncio.get_event_loop().run_until_complete(DeribitOptionsData.call_api(message))


    def _process_df(self, df):

        self._log.write(f"\n{__class__.__name__} _process_df() [status]: Processing data ...")

        # add spot column (deribit index price)
        self._log.write(f"\n\t>>> API Query of USD/{self.instrument} spot ... ")
        spot_info = get_index_price(self.instrument, quote="USD").loc[self.instrument]
        df['spot_usd_price'] = spot_info.price
        df['spot_usd_time'] = spot_info.time.time()
        self._log.write(f" [Completed]. Value added to data: {spot_info.price:.2f}")
        
        # add usdc spot price
        '''
        self._log.write(f"\n\t>>> API Query of USDC/{self.instrument} spot ... ")
        spot_info = get_index_price(self.instrument, quote="USDC").loc[self.instrument]
        df['spot_usdc_price'] = spot_info.price
        df['spot_usdc_time'] = spot_info.time.time()
        self._log.write(f" [Completed]. Value added to data: {spot_info.price:.2f}")
        '''

        # add expiry column
        df['expiry'] = [DeribitOptionsData.date_parser(date) for date in df.underlying_index]
        df['expiry'] = df.expiry.apply(lambda x: x + dt.timedelta(hours=8) if x is not None else None)

        #add strike column
        df['strike'] = [DeribitOptionsData.strike_parser(i) for i in df.instrument_name]

        # add side, i.e. put or call
        df['type'] = [DeribitOptionsData.side_parser(j) for j in df.instrument_name]

        # get example option with closest expiry in order to calc dollar price of options_trading

        # spot = DeribitOptionsData.get_quote(self._instrument)
        
        # Calculate bid, ask, mid mark in USD
        df['usd_bid'] = df.spot_usd_price * df.bid_price
        df['usd_ask'] = df.spot_usd_price * df.ask_price
        df['usd_mid'] = df.spot_usd_price * df.mid_price
        df['usd_mark'] = df.spot_usd_price * df.mark_price

        # utctimestamp in s
        df['utc_ts'] = np.full(shape=(df.shape[0],1), fill_value=self.utc_query_ts)

        # return utc time = local time - 8h
        df['utc_dt'] = df.utc_ts.apply(lambda x: dt.datetime.fromtimestamp(x))
        
        # add local time (hong kong)
        '''
        df['loc_date'] = df.utc_ts.apply(lambda x:  dt.date.fromtimestamp(x + 28800)) # auto conversion
        df['loc_time'] = df.utc_ts.apply(lambda x:  dt.datetime.fromtimestamp(x + 28800).time())
        '''

        # create time to expiry (toe) as float
        df['time_to_expiry'] = ((df.expiry - df.utc_dt) / np.timedelta64(1, "s")) / 3.154e7

        # Calculate implied volatility
        df['iv_bid'] = find_iv(df.usd_bid, df.underlying_price, df.strike, df.time_to_expiry, df.interest_rate, df.type, 0).IV.to_numpy()
        df['iv_ask'] = find_iv(df.usd_ask, df.underlying_price, df.strike, df.time_to_expiry, df.interest_rate, df.type, 0).IV.to_numpy()
        df['iv_mid'] = find_iv(df.usd_mid, df.underlying_price, df.strike, df.time_to_expiry, df.interest_rate, df.type, 0).IV.to_numpy()
        df['iv_mark'] = find_iv(df.usd_mark, df.underlying_price, df.strike, df.time_to_expiry, df.interest_rate, df.type, 0).IV.to_numpy()

        # Calculate APY
        df['apy_bid'] = df['usd_bid'] / (df['spot_usd_price'] * df['time_to_expiry'])
        df['apy_ask'] = df['usd_ask'] / (df['spot_usd_price'] * df['time_to_expiry'])
        df['apy_mid'] = df['usd_mid'] / (df['spot_usd_price'] * df['time_to_expiry'])
        df['apy_mark'] = df['usd_mark'] / (df['spot_usd_price'] * df['time_to_expiry'])

        # Calculate premium
        df['premium'] = df.bid_price * df.spot_usd_price

        # Remove multiple columns
        removed_cols = ['underlying_index', 'price_change', 'interest_rate', 'estimated_delivery_price', 'spot_usd_time', 'creation_timestamp']
        df = df.drop(removed_cols, axis=1)

        # Reorder columns
        column_names = ['utc_ts', 'utc_dt', 'spot_usd_price', 'instrument_name', 'quote_currency', 'base_currency', 'volume', 'open_interest', 'type', 'strike', 'underlying_price', 'expiry', 'time_to_expiry', 'low', 'high', 'last', 'bid_price', 'ask_price', 'mid_price', 'mark_price', 'usd_bid', 'usd_ask', 'usd_mid', 'usd_mark', 'iv_bid', 'iv_ask', 'iv_mid', 'iv_mark', 'apy_bid', 'apy_ask', 'apy_mid', 'apy_mark', 'premium']
        df = df.reindex(columns = column_names)

        # Sort by type, expiry, strike

        df = df.sort_values(by=['type', 'expiry', 'strike'])

        self._log.write(f"\n{__class__.__name__} _process_df() [status]: Task completed.")
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

    def _get_option_chain(self):

        self.utc_query_ts = int(dt.datetime.utcnow().timestamp())
        self.loc_query_ts = self.utc_query_ts + 28800

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

        self._log.write(f"\n[{dt.datetime.now()}] Querying option chain for {self.instrument}")
        response = self.async_loop(json.dumps(msg1))
        time_elapsed = dt.datetime.utcnow().timestamp() - self.utc_query_ts # timedelta.seconds
        self._log.write(f"\nTime elapsed in seconds between query and response returns... {time_elapsed:.0f}")

        df = self.json_to_dataframe(response)
        self._log.write(f"\nJSON converted to DataFrame. Raw data has {df.shape[0]} rows and {df.shape[1]} columns.")

        df = self._process_df(df) # modified
        self.options = df.copy(deep=True)

        self._log.write(f"Data returned with {self.options.shape[0]} rows and {self.options.shape[1]} columns.")

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
        path = os.path.join(dir,"options", "chain", "raw", self._instrument)
        self.options.to_csv(os.path.join(path,filename), index=False)
        self._log.write(f"\n{self.instrument} option chain saved to ... " + \
                        f"\n[Path]: {path}" + f"\n[Filename]: {filename}")

    def save_db(self):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            df1 = self.options.copy()
            keys = df1.columns.to_list()
            stmt_key = ", ".join(keys)
            stmt_val = ", ".join(["%s" for i in keys])
            stmt = f"REPLACE INTO dual_investment_data_deribit ({stmt_key}) VALUES ({stmt_val})"
            df1['utc_ts'] = pd.to_datetime(df1['utc_ts'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
            df1['utc_dt'] = pd.to_datetime(df1['utc_dt'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
            df1['expiry'] = pd.to_datetime(df1['expiry'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
            df1['expiry'] = df1['expiry'].fillna('1970-01-01 00:00:00')
            df1 = df1.fillna(0)
            cur.executemany(stmt, df1.values.tolist())
            conn.commit()
            conn.close()
        except Exception as e:
            print(e)
            self._log.write(f"\n{e}")
