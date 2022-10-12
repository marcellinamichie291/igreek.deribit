import sys

sys.path.append(r"C:\Users\admin\Desktop\ardgo-am-python\scripts")
import local_directory as loc_dir

sys.path.append(loc_dir.query_main)
from query_db_update_deribit import update_option_chain

if __name__ == '__main__':
    update_option_chain(instruments=["ETH", "BTC"])

