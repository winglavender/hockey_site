import pandas as pd
import sqlite3 as sql

def merge_seasons(season_list):
    seasons_unsorted = []
    for season in season_list:
        tmp = season.split("-")
        seasons_unsorted.append((int(tmp[0]), int(tmp[1])))
    seasons_sorted = sorted(seasons_unsorted)
    seasons_merged = []
    num_seasons = 0
    for season in seasons_sorted:
        if len(seasons_merged) == 0:
            seasons_merged.append(season)
            num_seasons += 1
        else:
            last_season = seasons_merged[-1]
            if last_season[1] > season[0]: # duplicate season because of AHL-NHL stuff probably
                continue
            elif last_season[1] == season[0]:
                new_last_season = (last_season[0], season[1])
                seasons_merged[-1] = new_last_season
                num_seasons += 1
            else:
                seasons_merged.append(season)
                num_seasons += 1
    for idx, season in enumerate(seasons_merged):
        seasons_merged[idx] = str(season[0]) + "-" + str(season[1])
    seasons_str = ", ".join(seasons_merged)
    return seasons_str, num_seasons

def query_db(target):
    conn = sql.connect('hockey_rosters.db')
    teammates = pd.read_sql_query(f'select b.playername, b.season, b.team, b.link from skaters a inner join skaters b on a.season = b.season and a.team = b.team and a.link != b.link where a.playername="{target}"',conn)
    #print(teammates)
    teams = teammates.team.unique()
    output = []
    for team in teams:
        if team == "totals":
            continue # todo fix this
        print(team)
        team_output = {'team': team, 'players': []}
        #output_str += team + "<br>"
        # todo sort by frequency/importance here
        team_teammates = teammates.loc[teammates.team==team].link.unique()
        for player_link in team_teammates:
            player_name = teammates.loc[teammates.link==player_link].playername.iloc[0]
            seasons = teammates.loc[teammates.link==player_link]
            combined_seasons, num_seasons = merge_seasons(seasons['season'])
            team_output['players'].append({'player': player_name, 'seasons': combined_seasons, 'num_seasons': num_seasons, 'link': player_link})
            #print(f"\t{player_name}\t{combined_seasons}\t{num_seasons}\t{player_link}") 
            #output_str += f"\t{player_name}\t{combined_seasons}\t{num_seasons}\t{player_link}<br>"
        output.append(team_output)
    print(output)
    return output
