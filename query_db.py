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
    num_seasons = max(1, end_year-start_year)
    return (start_year, end_year), num_seasons

def compute_timeline_str(year_ranges):
    year_ranges.sort()
    year_ranges_condensed = []
    for year_range in year_ranges:
        if len(year_ranges_condensed) == 0:
            year_ranges_condensed.append(year_range)
        else:
            last_range = year_ranges_condensed[-1]
            if year_range[0] == last_range[1]:
                new_range = (last_range[0], year_range[1])
                year_ranges_condensed[-1] = new_range
            else:
                year_ranges_condensed.append(year_range)
    # convert to string
    year_ranges_list = []
    for year_range in year_ranges_condensed:
        year_ranges_list.append(f"{year_range[0]}-{year_range[1]}")
    return ", ".join(year_ranges_list)


def query_db(target):
    # target is a player link (guaranteed unique for each name)
    conn = sql.connect('hockey_rosters_v3.db')
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
        # sort names alphabetically
        teammates_sorted = []
        for teammate_id in teammates.link.unique():
            teammate_rows = teammates.loc[teammates.link==teammate_id]
            full_name = teammate_rows.iloc[0].player
            last_name = full_name.split()[-1]
            teammates_sorted.append((last_name, teammate_id))
        teammates_sorted.sort()
        for teammate_lastname, teammate_id in teammates_sorted:
            teammate_rows = teammates.loc[teammates.link==teammate_id]
            total_seasons = 0
            overlap_years_list = []
            for index, teammate_term in teammate_rows.iterrows():
                overlap_years, seasons_count = compute_overlap_years_str(term.start_date, term.end_date, teammate_term.start_date, teammate_term.end_date)
                total_seasons += seasons_count
                overlap_years_list.append(overlap_years)
            overlap_years_str = compute_timeline_str(overlap_years_list)
            team_output['players'].append({'player': teammate_rows.iloc[0].player, 'seasons': overlap_years_str, 'link': teammate_rows.iloc[0].link, 'num_seasons': total_seasons})
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
    unique_links = links.link.unique()
    if len(unique_links) == 1:
        return unique_links[0], 1
    elif len(links) == 0:
        return name, 0
    else:
        # format output
        output = format_multiple_options(links)
        return output, len(output)
