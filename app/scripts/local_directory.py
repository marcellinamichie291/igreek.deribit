import os

homedrive = os.environ['HOMEDRIVE']
homepath = os.environ['HOMEPATH']
drive = r"C:\Users\cheng\deribit"

desktop = os.path.join(homedrive, homepath, "Desktop")
# onedrive = os.path.join(onedrive, "backup", "Desktop")

scripts_main = os.path.join(homedrive, homepath, "deribit", "scripts")
auto_main = os.path.join(scripts_main,"automations")
config_main = os.path.join(scripts_main, "proc_config")
data_proc_main = os.path.join(scripts_main, "data_process")
query_main = os.path.join(scripts_main, "query")
research_main = os.path.join(scripts_main, "research")
runtime_main = os.path.join(scripts_main, "runtime")

db_main = os.path.join(drive, "database")
db_crypto = os.path.join(db_main, "crypto")
