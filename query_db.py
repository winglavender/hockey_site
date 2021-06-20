import pandas as pd
import sqlite3 as sql

def compute_overlap_years_str(start_date1, end_date1, start_date2, end_date2):
    start_year1 = pd.to_datetime(start_date1).year
    start_year2 = pd.to_datetime(start_date2).year
    end_year1 = pd.to_datetime(end_date1).year
    end_year2 = pd.to_datetime(end_date2).year
    start_year = max(start_year1, start_year2)
    end_year = min(end_year1, end_year2)
    overlap_str = f"({start_year}-{end_year})"
    return overlap_str

def query_db(target):
    conn = sql.connect('hockey_rosters_v3.db')
    # TODO change this to link, deal with multiple name possibilities
    terms = pd.read_sql_query(f'select * from skaters where link=="{target}"',conn)
    output = []
    for index, term in terms.iterrows():
        tenure = f"({pd.to_datetime(term.start_date).year}-{pd.to_datetime(term.end_date).year})"
        team_output = {'team': term.team, 'tenure': tenure, 'players': []}
        if term.team == "multiple":
            continue # unresolved trade period
        # get following teammates
        teammates_a = pd.read_sql_query(f'select * from skaters where team=="{term.team}" and start_date >= "{term.start_date}" and start_date < "{term.end_date}" and link != "{term.link}"',conn)
        # get preceding teammates
        teammates_b = pd.read_sql_query(f'select * from skaters where team=="{term.team}" and "{term.start_date}" > start_date and "{term.start_date}" < end_date and link != "{term.link}"',conn)
        teammates = pd.concat([teammates_a, teammates_b])
        # TODO sort by frequency/importance here
        for teammate in teammates.link.unique():
            teammate_rows = teammates.loc[teammates.link==teammate]
            output_strs = []
            for index, teammate_term in teammate_rows.iterrows():
                overlap_years_str = compute_overlap_years_str(term.start_date, term.end_date, teammate_term.start_date, teammate_term.end_date)
                output_strs.append(overlap_years_str)
            team_output['players'].append({'player': teammate_rows.iloc[0].player, 'seasons': ",".join(output_strs), 'link': teammate_rows.iloc[0].link})
        output.append(team_output)
    return output

def player_to_description(player_row):
    year1 = pd.to_datetime(player_row.start_date).year
    year2 = pd.to_datetime(player_row.end_date).year
    return f"most recently {player_row.team} ({year1}-{year2})"

def format_multiple_options(links):
    unique_players = {}
    for index, row in links.iterrows():
        if row.link not in unique_players:
            unique_players[row.link] = {'player': row.player, 'link': row.link, 'start_date': row.start_date, 'end_date': row.end_date, 'description': player_to_description(row), 'team': row.team}
        else:
            other_start_date = unique_players[row.link]['start_date']
            if row.start_date > other_start_date:
                # new latest season, update player info
                unique_players[row.link] = {'player': row.player, 'link': row.link, 'start_date': row.start_date, 'end_date': row.end_date, 'description': player_to_description(row), 'team': row.team}
    return unique_players.values()

def retrieve_link_db(name):
    conn = sql.connect('hockey_rosters_v3.db')
    links = pd.read_sql_query(f'select * from skaters where player=="{name}"', conn)
    if len(links) == 1:
        return links.iloc[0].link, 1
    elif len(links) == 0:
        return name, 0
    else:
        # format output
        output = format_multiple_options(links)
        return output, len(output)
