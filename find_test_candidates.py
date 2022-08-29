import sqlite3 as sql
import pandas as pd
import sys
import csv
pd.set_option('display.max_columns', 500)

db_filename = sys.argv[1] # formatted db (not raw)
conn = sql.connect(db_filename)

# check skaters
skaters = pd.read_sql_query('select * from skaters', conn)
player = input("Specify a player (q to quit): ")
while player != "q":
    tmp = skaters.loc[skaters.player==player]
    print(tmp)
    player = input("Specify a player (q to quit): ")

