import sys
from os import path
sys.path.append(r"C:\Users\cheng\deribit\scripts")
import local_directory as loc_dir

sys.path.append(loc_dir.query_main)
from query_db_update_deribit import update_option_chain

if __name__ == '__main__':
    filepath = path.join(loc_dir.drive, "database", "dbt_option_chain_query_log.txt")
    update_option_chain(instruments=["ETH", "BTC"], log=filepath)

