import sqlite3 as sql
import pandas as pd
import sys
import csv
from update_data import DBBuilder

pd.set_option('display.max_columns', 500)

db_filename = sys.argv[1] # formatted db (not raw)
db_builder = DBBuilder()
db_builder.read_from_db(db_filename)

# check skaters
with open('data/skaters_test.txt','r') as in_file:
    reader = csv.DictReader(in_file) 
    num_passed = 0
    num_failed = 0
    for idx, row in enumerate(reader):
        player_rows = db_builder.process_player(row['player'])
        match = player_rows.loc[(player_rows.player==row['player']) & (player_rows.team==row['team']) & (player_rows.league==row['league']) & (pd.to_datetime(player_rows.start_date)==pd.to_datetime(row['start_date'])) & (pd.to_datetime(player_rows.end_date)==pd.to_datetime(row['end_date']))]
        if len(match) != 1:
            print(f"TEST FAILED: line {idx}, found {len(match)} matching rows")
            print(row)
            print(match)
            num_failed += 1
        else:
            num_passed += 1
    print(f"skaters tests: {num_passed} passed out of {num_passed+num_failed} tests = {num_passed/(num_passed+num_failed)*100:.2f}% accuracy")
        


