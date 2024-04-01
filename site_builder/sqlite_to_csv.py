from sqlalchemy import create_engine, text as sql_text
import yaml
import os
from pathlib import Path
import pandas as pd
import sys

# input arguments
# config_file = sys.argv[1] 

# connect to sqlite database
with open(f'../hockey_site/config.yaml', 'r') as f:
    config = yaml.safe_load(f)
config['filename_date'] = config['current_date'].replace("-", "")
root_dir = str(Path.cwd())
config['root_dir'] = root_dir + "/.."
db_file = os.path.join(config['data_dir'], f"{config['filename_date']}.db")
engine = create_engine(f"sqlite:///{db_file}")

output_dir = os.path.join(config['data_dir'], config['filename_date'])
print(output_dir)
if not os.path.exists(output_dir):
   # Create a new directory because it does not exist
   os.makedirs(output_dir)

# read tables to pandas and pickle them
table_names = ['ep_raw_asg', 'ep_raw_intl', 'ep_raw_postseasons', 'ep_raw_transfers', 'game_player', 'games', 'links',
               'norm_names', 'player_names', 'player_playoffs', 'players_nhl', 'scratches', 'skaters', 'teammates']
# table_names = ['teammates']
for table_name in table_names:
    print(f"reading {table_name}")
    q = f"select * from {table_name}"
    df = pd.read_sql_query(sql=sql_text(q), con=engine.connect())
    print(f"pickling {table_name}")
    df.to_pickle(os.path.join(output_dir, f"{table_name}.pkl"))

