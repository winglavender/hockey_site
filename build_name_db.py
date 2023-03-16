from query_db import hockey_db
from query_game_roster_db import game_roster_db

from normalize_name import *
import sys
#import unicodedata
#import re
import sqlite3 as sql
import pandas as pd
from os.path import exists
import numpy as np


#def normalize_name(orig_name):
#    name = orig_name.lower()
#    name = strip_accents(name)
#    #if orig_name == "":
#    #    print("help")
#    return name


#def strip_accents(text):
#    try:
#        text = unicode(text, 'utf-8')
#    except NameError: # unicode is a default on python 3
#        pass
#    text = unicodedata.normalize('NFD', text)\
#           .encode('ascii', 'ignore')\
#           .decode("utf-8")
#    text = text.replace("-", " ") # handle hyphenated names
#    text = re.sub(r'[^\w\s]', '', text) # remove punctuation (e.g. "J.T. Brown")
#    return str(text)


def check_norm_name(db, name):
    print(db.names.loc[db.names.norm_name==name])


def check_orig_name(db, name):
    print(db.names.loc[db.names.orig_name==name])


def get_num_games_played(scratches, game_player, player_id):
        scratch_games_ids = scratches.loc[scratches.playerId==player_id].gameId.values.tolist()
        all_games = game_player.loc[game_player.playerId == player_id]
        games = all_games.loc[~all_games.gameId.isin(scratch_games_ids)]
        return len(games)


def find_duplicates(ep_db, nhl_db, links_filename, out_filename):
    # read previous links file
    disambiguated_links = set()
    with open(links_filename) as in_file:
        for line in in_file:
            tmp = line.strip().split(",")
            ep_link = tmp[0].strip()
            if ep_link != "":
                disambiguated_links.add(ep_link)
            nhl_link = tmp[1].strip()
            if nhl_link != "":
                disambiguated_links.add(nhl_link)
    nhl_db_players = pd.read_csv(f"{nhl_db}_players.zip", compression='zip')
    ep_conn = sql.connect(ep_db)
    ep_db_players = pd.read_sql_query('select * from skaters', ep_conn)
    unique_people_links_nhl = nhl_db_players.playerId.unique()
    unique_people_links_ep = ep_db_players.link.unique()
    unique_people_names_nhl = []
    unique_people_names_ep = []
    for link in unique_people_links_nhl:
        unique_people_names_nhl = pd.concat([unique_people_names_nhl,nhl_db_players.loc[nhl_db_players.playerId == link].iloc[0].playerName])
    for link in unique_people_links_ep:
        unique_people_names_ep = pd.concat([unique_people_names_ep,ep_db_players.loc[ep_db_players.link == link].iloc[0].player])
    # NHL DB
    norm_names = [strip_accents(name.lower()) for name in unique_people_names_nhl]
    norm_names_seen = {}
    output_duplicates = []
    for idx, name in enumerate(norm_names):
        if name not in norm_names_seen:
            norm_names_seen[name] = []
        norm_names_seen[name].append(idx)
    for name in norm_names_seen:
        if len(norm_names_seen[name]) > 1:
            for idx in norm_names_seen[name]:
                output_duplicates.append((name, unique_people_names_nhl[idx], unique_people_links_nhl[idx]))
    # EP DB
    norm_names = [strip_accents(name.lower()) for name in unique_people_names_ep]
    norm_names_seen = {}
    for idx, name in enumerate(norm_names):
        if name not in norm_names_seen:
            norm_names_seen[name] = []
        norm_names_seen[name].append(idx)
    for name in norm_names_seen:
        if len(norm_names_seen[name]) > 1:
            for idx in norm_names_seen[name]:
                output_duplicates.append((name, unique_people_names_ep[idx], unique_people_links_ep[idx]))
    with open(out_filename, 'w') as out_file:
        for norm_name, name, link in output_duplicates:
            if str(link) not in disambiguated_links:
                out_file.write(f"{name},{norm_name},{link}\n")

def process_names(ep_db, nhl_db, out_db, link_file):
    normalized_names = pd.DataFrame(columns=['norm_name', 'canon_name'])
    # read already disambiguated names
    canon_name_links = []
    with open(link_file, 'r') as in_file:
        for line in in_file:
            line_list = line.strip().split(",")
            if len(line_list) == 2:
                line_list.append("") # no specified canon name (will default to NHL name)
            canon_name_links.append(line_list) # might be (nhl_link, ep_link) or (nhl_link, ep_link, canon_name)
    name_links = pd.DataFrame(canon_name_links, columns=['nhl_link', 'ep_link', 'canon_name'])
    name_links['nhl_link'] = pd.to_numeric(name_links['nhl_link'])
    # get canon names if not previously specified in link_file
    nhl_db_players = pd.read_csv(f"{nhl_db}_players.zip", compression='zip')
    for _, player_row in nhl_db_players.iterrows(): # TODO why are alexandre picard, colin white getting output here?
        player_nhl_link = player_row['playerId']
        player_nhl_name = player_row['playerName']
        norm_nhl_name = normalize_name(player_nhl_name)
        existing_link = name_links.loc[name_links.nhl_link == player_nhl_link]
        if len(existing_link) == 0:
            # need to add this link to the links table
            name_links = pd.concat([name_links,pd.DataFrame([{'nhl_link': player_nhl_link, 'canon_name': player_nhl_name, 'ep_link': ""}])])
            normalized_names = pd.concat([normalized_names,pd.DataFrame([{'norm_name': norm_nhl_name, 'canon_name': player_nhl_name}])])
        elif len(existing_link) == 1: # TODO it can't be more than 1 right?
            existing_canon_name = existing_link.iloc[0].canon_name
            if existing_canon_name == "":
                # if no specified other canon name, use the NHL name
                name_links.loc[name_links.nhl_link==player_nhl_link, 'canon_name'] = player_nhl_name
                # update normalized name df
                # TODO make this more efficient later, when I've figured out what data I need when
                normalized_names = pd.concat([normalized_names,pd.DataFrame([{'norm_name': norm_nhl_name, 'canon_name': player_nhl_name}])])
            else:
                # update normalized name df to map to the existing canon name
                normalized_names = pd.concat([normalized_names,pd.DataFrame([{'norm_name': norm_nhl_name, 'canon_name': existing_canon_name}])])
    # attempt to match EP names to canon names
    ep_conn = sql.connect(ep_db)
    ep_db_players = pd.read_sql_query('select * from skaters', ep_conn)
    for _, player_row in ep_db_players.iterrows():
        player_ep_link = player_row['link']
        player_ep_name = player_row['player']
        norm_ep_name = normalize_name(player_ep_name)
        # check for an existing canon name
        existing_link = name_links.loc[name_links.ep_link == player_ep_link]
        if len(existing_link) == 0:
            # no defined link, we have to use the name form to look for a link
            norm_name_row = normalized_names.loc[normalized_names.norm_name==norm_ep_name]
            if len(norm_name_row) == 1:
                # found an existing canon name, update that name with EP link
                existing_canon_name = norm_name_row.iloc[0].canon_name
                name_links.loc[name_links.canon_name==existing_canon_name, 'ep_link'] = player_ep_link
            else:
                # can't use the name form to find a link, this EP name will be used as the canon name
                # TODO is this assumption correct?
                name_links = pd.concat([name_links,pd.DataFrame([{'canon_name': player_ep_name, 'nhl_link': "", 'ep_link': player_ep_link}])])
        elif len(existing_link) == 1: # TODO it can't be more than 1 right?
            # we found a specified link
            existing_canon_name = existing_link.iloc[0].canon_name
            # check whether the canon name is already linked to this norm name (if not, update)
            existing_norm_row = normalized_names.loc[(normalized_names.canon_name==existing_canon_name) & (normalized_names.norm_name==norm_ep_name)]
            if len(existing_norm_row) == 0:
                # update normalized_names
                normalized_names = pd.concat([normalized_names,pd.DataFrame([{'norm_name': norm_ep_name, 'canon_name': existing_canon_name}])])
    # add first/last names to link table
    new_names = []
    for _, row in normalized_names.iterrows():
        name_parts = get_name_parts(row['norm_name'])
        #if name_parts[0] == "":
        #    print(row)
        for part in name_parts:
            new_names.append([part, row['canon_name']])
    normalized_names = pd.concat([normalized_names,pd.DataFrame(new_names, columns=['norm_name', 'canon_name'])])
    # output missing links
    nhl_scratches = pd.read_csv(f"{nhl_db}_scratches.zip", compression='zip')
    nhl_game_player = pd.read_csv(f"{nhl_db}_game_player.zip", compression='zip',
                                   dtype={'assists': 'str', 'goals': 'str', 'powerPlayAssists': 'str'})

    with open('nhl_link_only_10.txt', 'w') as nhl_out_file_10, open('nhl_link_only.txt', 'w') as nhl_out_file, open('ep_link_only.txt', 'w') as ep_out_file:
        nhl_out_tuples = []
        ep_out_tuples = []
        for _, row in name_links.iterrows():
            if row.ep_link == "":
                games_played = get_num_games_played(nhl_scratches, nhl_game_player, row.nhl_link) # get nhl games played
                nhl_out_tuples.append((row.nhl_link, row.canon_name, games_played))
            elif row.nhl_link == "":
                ep_out_tuples.append((row.ep_link, row.canon_name))
        # sort output by canon name, probably makes matching easier
        nhl_out_tuples = sorted(nhl_out_tuples, key=lambda x: x[1])
        for nhl_link, canon_name, games_played in nhl_out_tuples:
            if games_played >= 10:
                nhl_out_file_10.write(f"{nhl_link},{canon_name},{games_played}\n")
            nhl_out_file.write(f"{nhl_link},{canon_name},{games_played}\n")
        ep_out_tuples = sorted(ep_out_tuples, key=lambda x: x[1])
        for ep_link, canon_name in ep_out_tuples:
            ep_out_file.write(f"{ep_link},{canon_name}\n")
    # save to db
    out_conn = sql.connect(out_db)
    normalized_names.drop_duplicates(inplace=True, ignore_index=True)
    normalized_names.to_sql('norm_names', out_conn)
    name_links.drop_duplicates(inplace=True, ignore_index=True)
    name_links.to_sql('links', out_conn)

def get_name_parts(name):
    print("NAME")
    print(name)
    name_parts = name.split()
    #last_word_idx = len(name_parts)-1
    all_name_parts = []
    for i in range(0, len(name_parts)):
        name_part = name_parts[i]
        if name_part != name:
            all_name_parts.append(name_part)
        for j in range(i+1, len(name_parts)):
            name_part += " " + name_parts[j]
            if name_part != name:
                all_name_parts.append(name_part)
    print(all_name_parts)
    return all_name_parts
    #if len(name_parts) == 2:
    #    return name_parts
    #if len(name_parts) > 0:
    #    return name_parts[0]
    #return [""]


if __name__ == "__main__":
    if sys.argv[1] == "find_duplicates":
        ep_db = sys.argv[2]
        nhl_db = sys.argv[3] # nhl data root filename eg 'game_records_20002023_20221024'
        links_filename = sys.argv[4] # the links file from the last time we build the name DB so we know which duplicates we've already dealt with
        duplicates_filename = sys.argv[5]
        find_duplicates(ep_db, nhl_db, links_filename, duplicates_filename)
    elif sys.argv[1] == "build_name_db": # repeat this step and manually fill out the link file until satisfied, then use resulting out_db file as website input
        ep_db = sys.argv[2] # processed, not raw
        nhl_db = sys.argv[3]
        out_db = sys.argv[4]
        if exists(out_db):
            print(f"Error: file {out_db} already exists.")
            sys.exit(0)
        link_file = sys.argv[5] # contains deduplication info and manually joined nhl-ep links
        process_names(ep_db, nhl_db, out_db, link_file)
    # if sys.argv[1] == "compare_names":
    #     find_mismatched_names()
    # if sys.argv[1] == "check_norm_name":
    #     if sys.argv[2] == "ep":
    #         check_norm_name(ep_db, sys.argv[3])
    #     elif sys.argv[2] == "nhl":
    #         check_norm_name(nhl_db, sys.argv[3])
    # elif sys.argv[1] == "check_orig_name":
    #     if sys.argv[2] == "ep":
    #         check_orig_name(ep_db, sys.argv[3])
    #     elif sys.argv[2] == "nhl":
    #         check_orig_name(nhl_db, sys.argv[3])
