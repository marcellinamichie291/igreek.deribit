import datetime as dt
import os
import shutil
import sys

import pandas as pd

sys.path.append(r"C:\Users\cheng\deribit\scripts")
import local_directory as loc_dir

"""module <proc_config>"""
sys.path.append(loc_dir.config_main)

"""module <query>"""
import query_deribit as deriq
from query_deribit_option_chain import DeribitOptionsData as dbt_option


DB_DIR = loc_dir.db_main
TDY = dt.date.today()
TDY_SETTLE = dt.datetime(TDY.year, TDY.month, TDY.day, 16, 0, 0)

def update_option_chain(instruments: list, log: str):

    with open(log, 'r') as f:
        lines = f.readlines()

    with open(log, 'w') as f:
        hdr = "Log of Task Scheduler \"Query Deribit Option Chain\""
        hdr2 = f"\n[Last Update] Time: {dt.datetime.now()}"

        if len(lines) > 0:
            lines[0] = hdr
            lines[1] = hdr2
            f.writelines(lines)
        else:
            f.write(hdr)
            f.write(hdr2)

    status = ""
    with open(log, 'a') as log_file:
        try:
            for inst in instruments:
                log_file.write(f"\n\n============================================ {dt.datetime.now()} {inst} ============================================")
                option_obj = dbt_option(inst, log_file=log_file)
                option_obj.save_csv()
                status = "Success"
        except:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            log_file.write(f"\nProcess exited with exceptions.\nException Type: {ex_type}.\nException Value: {ex_value}.")
            status = "Failure"

    print(f"Status: Proc Run {status}.")


# if __name__ == '__main__':

    # start_tst
    # auth_dict_1 = deriq.auth(deribit_key_1.client_id, deribit_key_1.client_secret)
    # auth_dict_2 = deriq.auth(deribit_key_2.client_id, deribit_key_2.client_secret)


    # eth_futures = deriq.get_futures_instruments(currency="ETH")
    # btc_futures = deriq.get_futures_instruments(currency="BTC")
    # eth_options = deriq.get_option_instruments(currency="ETH")
    # btc_options = deriq.get_option_instruments(currency="BTC")

    # update_futures_settlement_price_database(eth_futures,count=1)
    # update_option_database(eth_options)

    # get_all_currencies_traded
    # update_trade_records(["ETH"],
    #                      dbt_cfg.INCEPTION_DATETIME,
    #                      dbt_cfg.CURRENT_DATETIME)

