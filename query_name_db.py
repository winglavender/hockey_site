import pandas as pd
import sqlite3 as sql
import unicodedata
import re
pd.set_option('max_columns', None)

class name_db():

    def __init__(self):
        db_name = 'names_20221006.db'
        conn = sql.connect(db_name)
        norm_names = pd.read_sql_query('select * from norm_names', conn)
        links = pd.read_sql_query('select * from links', conn)
        self.name_links = norm_names.merge(links, on='canon_name')

    def get_possible_links(self, input_name, db_type):
        if db_type != "ep" and db_type != "nhl":
            print("ERROR: specify valid link type for name_db ('ep' or 'nhl')")
            return None
        link_name = f"{db_type}_link"
        tgt_name = self.strip_accents(input_name.lower())
        name_rows = self.name_links.loc[(self.name_links.norm_name == tgt_name) & (self.name_links[link_name] != "")]
        output = []
        for _, row in name_rows.iterrows():
            output.append({'player': row['canon_name'], 'link': row[link_name]})
        return output

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

if __name__ == "__main__":
    name_db = name_db()
    print(name_db.get_possible_links("connor mcDavid", "nhl"))
    print(name_db.get_possible_links("connor mcDavid", "ep"))
    print(name_db.get_possible_links("sebastian aho", "nhl"))
    print(name_db.get_possible_links("sebastian aho", "ep"))
    print(name_db.get_possible_links("nate mackinnon", "nhl"))
    print(name_db.get_possible_links("sid crosby", "nhl"))
    print(name_db.get_possible_links("fdsakjl;", "nhl"))