import sqlite3 as sql
import pandas as pd
import sys
import csv
import yaml
pd.set_option('display.max_columns', 500)
sys.path.insert(1, '../hockey_db')
from sqlalchemy import create_engine, text as sql_text

# data_dir = ""
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)
config['filename_date'] = config['current_date'].replace("-", "")
db_filename = f"../hockey_db_data/{config['filename_date']}.db"
print(db_filename)
engine = create_engine(f"sqlite:///{db_filename}")

# check skaters
player_rows = pd.read_sql_query(sql=sql_text(f"select * from skaters join links using(playerId)"), con=engine.connect(), parse_dates=['start_date', 'end_date']) 

with open('data/skaters_test.txt','r') as in_file:
    reader = csv.DictReader(in_file) 
    num_passed = 0
    num_failed = 0
    for idx, row in enumerate(reader):
        match = player_rows.loc[(player_rows.playerName==row['player']) & (player_rows.team==row['team']) & (player_rows.league==row['league']) & (pd.to_datetime(player_rows.start_date)==pd.to_datetime(row['start_date'])) & (pd.to_datetime(player_rows.end_date)==pd.to_datetime(row['end_date']))]
        partial_match = player_rows.loc[(player_rows.playerName==row['player']) & (player_rows.team==row['team'])]
        if len(match) != 1:
            print(f"TEST FAILED: line {idx}, found {len(match)} matching rows")
            if row['notes']:
                print(f"WARNING: {row['notes']}")
            print()
            print(f"{row['player']}, {row['team']} ({row['league']}) {row['start_date']} to {row['end_date']}")
            if len(match) > 0:
                print(match)
            if len (partial_match) > 0:
                print("partial match")
                for row in partial_match.itertuples():
                    print(f"{row.playerName}, {row.team} ({row.league}) {row.start_date.strftime('%Y-%m-%d')} to {row.end_date.strftime('%Y-%m-%d')}")
            print("------------------\n\n")
            num_failed += 1
        else:
            num_passed += 1
    print(f"skaters tests: {num_passed} passed out of {num_passed+num_failed} tests = {num_passed/(num_passed+num_failed)*100:.2f}% accuracy")
        


