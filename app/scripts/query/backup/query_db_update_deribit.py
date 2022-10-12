import datetime as dt
import os
import shutil
import sys

import pandas as pd

sys.path.append(r"C:\Users\admin\Desktop\ardgo-am-python\scripts")
import local_directory as loc_dir

"""module <proc_config>"""
sys.path.append(loc_dir.config_main)

"""module <query>"""
import query_deribit as deriq
from query_deribit_option_chain import DeribitOptionsData as dbt_option


DB_DIR = loc_dir.db_main
TDY = dt.date.today()
TDY_SETTLE = dt.datetime(TDY.year, TDY.month, TDY.day, 16, 0, 0)

def update_futures_settlement_price_database(instruments=None, count=1):

    dir = os.path.join(DB_DIR, "futures", "settlements")
    lsdir = os.listdir(dir)

    filename = "reversed_futures_settlements_".upper() + dt.datetime.strftime(TDY, "%Y%b").upper() + ".xlsx"
    filepath = os.path.join(dir, filename)

    run_doc = open(os.path.join(dir, "proc_run_history", dt.datetime.strftime(TDY, "%Y%m%d")+".txt"), "a")
    print(f"[Main Dir] {lsdir}", file=run_doc)
    print(f"[Writing to ...: {filepath}", file=run_doc)

    if filename in lsdir:

        # create backup file path
        print("Making backup directory ...", file=run_doc)
        backup_dir = os.path.join(dir, "backup")
        backup_filename = filename.rstrip(".xlsx") + "_" + dt.datetime.strftime(TDY, "%Y%m%d").upper() + ".xlsx"
        backup_filepath = os.path.join(backup_dir, backup_filename)

        # back up to fldr: output/futures/settlements/backup
        shutil.copy2(filepath, backup_filepath)
        print(f"[Backup completed - Backup Dir] {backup_filepath}", file=run_doc)

        # read data from the backed up file
        with open(backup_filepath, mode="rb") as last_file:
            db_last = pd.read_excel(last_file)
        print(f"Previous database contains {db_last.shape[0]} records", file=run_doc)

    else:
        db_last = pd.DataFrame()

    # query new records
    records = []
    if instruments is None:
        instruments = deriq.get_futures_instruments()

    for inst in instruments:
        try:
            records.append(deriq.get_last_settlement(inst, count))
        except:
            print(f"Error on retrieving data for {inst}", file=run_doc)
            continue

    # compile dataframe
    df = pd.concat(records)

    # combine all records of current month with the last settlement prices of the day
    db_new = pd.concat([db_last, df])

    # sort by instrument and descending order of timestamp
    db_new.sort_values(by=['instrument_name', 'timestamp'], ascending=[1, 0], inplace=True)

    # remove duplicates
    """Warning need to debug here"""
    print(f"Before drop number of rows: {db_new.shape[0]}", file=run_doc)
    db_new.drop_duplicates(inplace=True, ignore_index=True)

    # write to excel
    with pd.ExcelWriter(filepath) as writer:
        db_new.to_excel(writer, index=False)

    """Debug"""
    temp_df = pd.read_excel(filepath)
    print(f"Before 2nd drop number of rows: {temp_df.shape[0]}", file=run_doc)

    temp_df.drop_duplicates(inplace=True, ignore_index=True)
    print(f"After 2nd drop number of rows: {temp_df.shape[0]}", file=run_doc)

    if temp_df.shape[0] <= db_new.shape[0]:
        with pd.ExcelWriter(filepath) as writer:
            temp_df.to_excel(writer, index=False)
        print(f"Second write completed.", file=run_doc)
    del temp_df

    print(f"[Successful Update on {tdy}] - Reversed Futures Settlement Price Database", file=run_doc)
    run_doc.close()

def update_trade_records(currencies=['ETH'], start_dt=None, end_dt=None, acc_nums=[1,2], type=None):

    if end_dt is None:

        end_dt = TDY_SETTLE

    if start_dt is None:
        start_dt = end_dt + dt.timedelta(days=-1)

    trade_logs = []
    for c in currencies:
        for i in acc_nums:
            df = deriq.get_private_transaction_logs(c, start_dt, end_dt, i)
            if df is not None:
                trade_logs.append(df)

    logs_df = pd.concat(trade_logs)

    dir = os.path.join(DB_DIR, "account", "logs")
    start_dt_str = dt.datetime.strftime(start_dt, "%Y%m%d")
    end_dt_str = dt.datetime.strftime(end_dt, "%Y%m%d")

    # export raw
    filename = '_'.join(["deribit_trade_logs_raw_", start_dt_str, end_dt_str]) + ".csv"
    filepath = os.path.join(dir, filename)
    logs_df.to_csv(filepath, index=False)

    # export processed
    logs_df = deriq.get_processed_trade_logs(logs_df)
    filename = '_'.join(["deribit_trade_logs_processed_", start_dt_str, end_dt_str]) + ".csv"
    filepath = os.path.join(dir, filename)
    logs_df.to_csv(filepath, index=False)
    print(f"Please find your file under --> {dir}")

    # make transfer logs

def update_option_chain(instruments: list):

    for inst in instruments:
        option_obj = dbt_option(inst)
        option_obj.save_csv()


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

