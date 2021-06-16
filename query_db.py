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
#     if start_year == end_year:
#         overlap_str = f"({start_year})"
    return overlap_str

def query_db(target):
    conn = sql.connect('hockey_rosters_v3.db')
    # TODO change this to link, deal with multiple name possibilities
    terms = pd.read_sql_query(f'select * from skaters where player=="{target}"',conn)
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
