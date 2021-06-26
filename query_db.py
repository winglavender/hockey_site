import pandas as pd
import sqlite3 as sql
import unicodedata

db_name = 'hockey_rosters_v8.db'

def league_key_to_display(league_str):
    leagues = {'nhl': 'NHL', 'og': 'Olympics', 'khl': 'KHL', 'ahl': 'AHL', 'wc': 'Worlds', 'ohl': 'OHL', 'whl': 'WHL', 'qmjhl': 'QMJHL', 'ushl': 'USHL', 'usdp': 'USDP', 'ncaa': 'NCAA', 'wjc-20': 'World Juniors', 'wjc-18': 'WC-U18', 'whc-17': 'WHC-17', 'wcup': 'World Cup'}
    return leagues[league_str]

def team_key_to_display(league_str, team_str):
    if league_str == 'og':
        return f"{team_str} Olympics"
    elif league_str == 'wc':
        return f"{team_str} Worlds"
    elif league_str == 'wcup':
        return f"{team_str} World Cup"
    else:
        return team_str

def compute_overlap_interval(start_date1, end_date1, start_date2, end_date2):
    start_dates = pd.DataFrame([pd.to_datetime(start_date1), pd.to_datetime(start_date2)])
    end_dates = pd.DataFrame([pd.to_datetime(end_date1), pd.to_datetime(end_date2)])
    start_interval = start_dates.max().iloc[0]
    end_interval = end_dates.min().iloc[0]
    num_seasons = max(1, end_interval.year-start_interval.year)
    if start_interval <= end_interval:
        return (start_interval, end_interval), num_seasons
    else:
        return False, 0

def query_career_teammates(target):
    # target is a player link (guaranteed unique for each name)
    conn = sql.connect(db_name)
    terms = pd.read_sql_query(f'select * from skaters where link=="{target}"',conn)
    output = []
    for index, term in terms.iterrows():
        # get following teammates
        teammates_a = pd.read_sql_query(f'select * from skaters where league="{term.league}" and team=="{term.team}" and start_date >= "{term.start_date}" and start_date < "{term.end_date}" and link != "{term.link}"',conn)
        # get preceding teammates
        teammates_b = pd.read_sql_query(f'select * from skaters where league="{term.league}" and team=="{term.team}" and "{term.start_date}" > start_date and "{term.start_date}" < end_date and link != "{term.link}"',conn)
        teammates = pd.concat([teammates_a, teammates_b])
        for teammate_id in teammates.link.unique():
            # all the overlapping time periods our target has with this teammate
            teammate_rows = teammates.loc[teammates.link==teammate_id]
            #overlap_years_list = []
            for index, teammate_term in teammate_rows.iterrows():
                overlap_term, seasons_count = compute_overlap_interval(term.start_date, term.end_date, teammate_term.start_date, teammate_term.end_date)
                output.append((overlap_term[0].year, [teammate_rows.iloc[0].player, team_key_to_display(term.league, term.team), overlap_term[0].year, overlap_term[0].month-1, overlap_term[0].day, overlap_term[1].year, overlap_term[1].month-1, overlap_term[1].day]))
    # sort all teammates by first overlap year
    output.sort()
    sorted_output = []
    for year, data in output:
        sorted_output.append(data)
    return sorted_output

def query_pair_teammates(player1_id, player2_id):
    conn = sql.connect(db_name)
    terms = pd.read_sql_query(f'select * from skaters where link=="{player1_id}"', conn)
    output = [] 
    for index, term in terms.iterrows():
        potential_overlaps_a = pd.read_sql_query(f'select * from skaters where league="{term.league}" and team=="{term.team}" and link=="{player2_id}" and start_date >= "{term.start_date}" and start_date < "{term.end_date}"',conn)
        potential_overlaps_b = pd.read_sql_query(f'select * from skaters where league="{term.league}" and team=="{term.team}" and link=="{player2_id}" and "{term.start_date}" > start_date and "{term.start_date}" < end_date',conn)
        potential_overlaps = pd.concat([potential_overlaps_a, potential_overlaps_b])
        for index2, candidate in potential_overlaps.iterrows():
            # check for overlap
            overlap_interval, num_seasons = compute_overlap_interval(term.start_date, term.end_date, candidate.start_date, candidate.end_date)
            if overlap_interval:
                output.append((overlap_interval[0].year, [term.team, term.league, overlap_interval[0].year, overlap_interval[1].year]))
    # sort terms by first year of overlap
    output.sort()
    sorted_output = []
    for year, data in output:
        sorted_output.append({"team": data[0], "league": league_key_to_display(data[1]), "year1": data[2], "year2": data[3]})
    return sorted_output

def player_to_description(player_row):
    year1 = pd.to_datetime(player_row.start_date).year
    year2 = pd.to_datetime(player_row.end_date).year
    return f"most recently {player_row.team} ({year1}-{year2})"

# for all multiple possible players for a given name input, 
# list out all options, each with an attached description of the most recent NHL team they played for 
def format_multiple_options(links):
    unique_players = {}
    for index, row in links.iterrows():
        if row.link not in unique_players:
            unique_players[row.link] = {'player': row.player, 'link': row.link, 'start_date': row.start_date, 'end_date': row.end_date, 'description': player_to_description(row), 'team': row.team}
        else:
            other_start_date = unique_players[row.link]['start_date']
            if row.start_date > other_start_date and row.league == 'nhl':
                # new latest season (restrict to NHL only), update player info
                unique_players[row.link] = {'player': row.player, 'link': row.link, 'start_date': row.start_date, 'end_date': row.end_date, 'description': player_to_description(row), 'team': row.team}
    return unique_players.values()

def strip_accents(text):
    try:
        text = unicode(text, 'utf-8')
    except NameError: # unicode is a default on python 3
        pass
    text = unicodedata.normalize('NFD', text)\
           .encode('ascii', 'ignore')\
           .decode("utf-8")
    return str(text)


# given an input name, return the player id (EliteProspects url)
# if there are multiple possible players, return all possibilities, each with a description
def retrieve_player_link(name):
    conn = sql.connect(db_name)
    tgt_name = strip_accents(name.lower())
    names = pd.read_sql_query(f'select * from names where norm_name=="{tgt_name}"', conn)
    unique_links = set()
    for index, name_row in names.iterrows():
        links = pd.read_sql_query(f'select * from skaters where player=="{name_row.orig_name}"', conn)
        unique_links.update(list(links.link.unique()))
    if len(unique_links) == 1:
        return 1, (names.iloc[0].orig_name, list(unique_links)[0])
    elif len(unique_links) == 0:
        return 0, name
    else:
        # format output
        output = format_multiple_options(links)
        return len(output), output
