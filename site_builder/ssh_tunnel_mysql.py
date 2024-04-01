# this script takes all the pickled dataframes and writes them to the remote (pythonanywhere) mysql database using SSH tunneling
# the dataframe pkl location is defined by the config file at 

from sqlalchemy import create_engine, text as sql_text
import yaml
import os
from pathlib import Path
import pandas as pd
import sys
import argparse
import sshtunnel
import time
import glob

# parser = argparse.ArgumentParser() # TODO
# parser.add_argument("path")
# parser.add_argument('--to_live_website', action=argparse.BooleanOptionalAction)
# args = parser.parse_args()

with open(f'../hockey_site/config.yaml', 'r') as f:
    config = yaml.safe_load(f)
root_dir = str(Path.cwd())
config['root_dir'] = root_dir + "/.."
config['filename_date'] = config['current_date'].replace("-", "")
with open(f'ssh_config.yml', 'r') as f:
    pwd_config = yaml.safe_load(f)

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0

# if args.to_live_website:
if len(sys.argv) == 2 and sys.argv[1] == "to_real_site":
    ssh_username=pwd_config['real_username'] 
    ssh_password=pwd_config['real_password']
else:
    ssh_username=pwd_config['test_username'] 
    ssh_password=pwd_config['test_password']
remote_bind_address=f'{ssh_username}.mysql.pythonanywhere-services.com'

with sshtunnel.SSHTunnelForwarder(
    ('ssh.pythonanywhere.com'),
    ssh_username=ssh_username, ssh_password=ssh_password,
    remote_bind_address=(remote_bind_address, 3306), allow_agent=False
) as tunnel:

    # connect to mysql database
    if len(sys.argv) == 2 and sys.argv[1] == "to_real_site":
        mysql_username = pwd_config['real_username'] 
        mysql_password = pwd_config['real_mysql_password']
        mysql_databasename = pwd_config['real_databasename']
    else:
        mysql_username = pwd_config['test_username']
        mysql_password = pwd_config['test_mysql_password']
        mysql_databasename = pwd_config['test_databasename']

    SQLALCHEMY_DATABASE_URI = "mysql://{username}:{password}@{host}:{port}/{databasename}".format(
        username=mysql_username,
        password=mysql_password,
        host='127.0.0.1', 
        port=tunnel.local_bind_port,
        databasename=mysql_databasename,
    )
    print(SQLALCHEMY_DATABASE_URI)
    engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_recycle=280)
    input_dir = os.path.join(config['data_dir'], config['filename_date'])
    print(input_dir)
    # read tables to pandas and pickle them
    start = time.time()
    for pklname in glob.glob(f"{input_dir}/*.pkl"):
        tmp = pklname.split("/")[-1]
        table_name = tmp.split(".pkl")[0]
        print(f"unpickling {table_name}")
        df = pd.read_pickle(pklname)
        print(f"writing {table_name} to mysql")
        df.to_sql(name=table_name, con=engine, index=False, chunksize=1000) # TODO handle this if_exists='replace')
        end = time.time()
        print(f"elapsed time: {end - start}")
        
