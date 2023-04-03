import csv

from normalize_name import *
import pandas as pd
import sqlite3 as sql
import unicodedata
import re
import numpy as np
#pd.set_option('max_columns', None)
pd.options.display.max_columns = None

class name_db():

    def __init__(self):
        db_name = 'names_20230403.db'
        conn = sql.connect(db_name)
        norm_names = pd.read_sql_query('select * from norm_names', conn)
        links = pd.read_sql_query('select * from links', conn)
        self.name_links = norm_names.merge(links, how='outer', on='canon_name')
        self.correct_links()

    def correct_links(self):
        with open('ep_link_corrections.txt') as in_file:
            reader = csv.DictReader(in_file)
            for row in reader:
                self.name_links.loc[self.name_links['ep_link'] == row['incorrect_link'], 'ep_link'] = row['correct_link']

    def get_possible_links(self, input_name, db_type):
        if db_type != "ep" and db_type != "nhl":
            print("ERROR: specify valid link type for name_db ('ep' or 'nhl')")
            return None
        link_name = f"{db_type}_link"
        tgt_name = normalize_name(input_name)
        #tgt_name = self.strip_accents(input_name.lower())
        name_rows = self.name_links.loc[(self.name_links.norm_name == tgt_name) & (self.name_links[link_name] != "")]
        output = []
        for _, row in name_rows.iterrows():

            output.append({'player': row['canon_name'], 'link': row[link_name]})
        return output

    def correct_period_in_link(self, link):
        # link = row[link_name]
        name_link_idx = link.rfind("/")
        name_link = link[name_link_idx:]
        if "." in name_link:
            # print(link)
            name_link = name_link.replace(".", "")
            link = link[:name_link_idx] + name_link
            # print(link)
        return link
            # anme_ = link.replace(".", "")

    def get_name(self, player_id, db_type):
        if db_type != "ep" and db_type != "nhl":
            print("ERROR: specify valid link type for name_db ('ep' or 'nhl')")
            return None
        name_rows = self.name_links.loc[self.name_links[f"{db_type}_link"] == player_id]
        if len(name_rows) == 0:
            print(f"ERROR: no player with {db_type} id {player_id}")
            return None
        return name_rows.iloc[0].canon_name

    def strip_accents(self, text):
        try:
            text = unicode(text, 'utf-8')
        except NameError: # unicode is a default on python 3
            pass
        text = unicodedata.normalize('NFD', text)\
               .encode('ascii', 'ignore')\
               .decode("utf-8")
        text = text.replace("-"," ") # handle hyphenated names
        text = re.sub(r'[^\w\s]', '', text) # remove punctuation (e.g. "J.T. Brown")
        return str(text)

    # check if the normalization function changes anyone's name length -- hoping to catch the Mads Sogaard error faster
    def check_name_normalization(self):
        for row in self.name_links.itertuples(index=False):
            if normalize_name(row.canon_name) == row.norm_name and len(row.canon_name) != len(row.norm_name):
                print(row)

if __name__ == "__main__":
    name_db = name_db()
    name_db.check_name_normalization()
    #print(name_db.get_possible_links("connor mcDavid", "ep"))
    #print(name_db.get_possible_links("Martin St-Louis", "ep"))
