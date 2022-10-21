import sys
import time
import os
import datetime

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import local_directory as loc_dir

sys.path.append(loc_dir.query_main)
import query_db_update_deribit

if __name__ == '__main__':
    cur_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    while True:
        try:
            filepath = os.path.join(loc_dir.drive, "database", f"dbt_option_chain_query_log_{cur_time}.txt")
            query_db_update_deribit.update_option_chain(instruments=["ETH", "BTC"], log=filepath)
            time.sleep(10)
        except Exception as e:
            print(e)
