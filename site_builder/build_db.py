# inputs
import yaml
import os
import sys
import numpy as np
from pathlib import Path
root_dir = str(Path.cwd())#.parents[0])


if os.getenv('PYANYWHERE'):
    root_dir = os.path.join(root_dir, 'hockey_site')
    print(root_dir)
    # config_location = '/home/hockeyteammates/hockey_site/config.yaml'
     # "/home/hockeyteammates/hockey_site/data/"
else:
    sys.path.insert(1, '../hockey_scraper')
    import scraper
    # config_location = '../hockey_site/config.yaml'
    # data_dir = "/Users/alice/Dropbox/Projects/hockey_site/data/"
    db_dir = os.path.join(Path(root_dir).parents[0], 'hockey_db_data')
print(root_dir)
data_dir = os.path.join(root_dir, 'data')
config_location = os.path.join(root_dir, 'config.yaml')
with open(config_location, 'r') as f:
    config = yaml.safe_load(f)
if 'prev_file_date' not in config:
    config['prev_file_date'] = None

earliest_transfer_date = "2000/08/01" # this is the earliest transfer date that exists in EliteProspects
scrape_start_year = 1990 # TODO 1917 someday 

# imports
import time
import csv
import json
import concurrent.futures
import pandas as pd
from datetime import date
from datetime import timedelta
from datetime import datetime
from os.path import exists
MAX_THREADS = 30
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import create_engine, text as sql_text
from unidecode import unidecode
# for font-accurate string comparisons
from PIL import ImageFont
font = ImageFont.load_default()

def scrape_page(url):
    """
    Scrape a given url

    :param url: url for page
    :return: response object
    """
    response = requests.Session()
    retries = Retry(total=10, backoff_factor=.1)
    response.mount('http://', HTTPAdapter(max_retries=retries))

    page = requests.get(url, timeout=500)
    page_string = str(page)
    if page_string != '<Response [200]>':
        print(url, page_string)
    while page_string == '<Response [403]>':
        print("Just got a 403 Error within the page. Time to sleep, then try again.")
        time.sleep(10)
        page = requests.get(url, timeout = 500) 
        page_string = str(page)
        print("Let's try again")

    if page_string != '<Response [200]>':
        print(url, page_string)
    if (str(page_string) == '<Response [404]>'):
        print(f"ERROR: {page_string} on url {url}")
    
    try:
        page = page.json()
    except:
        page = None
    return url, page

from site_builder.season_calculator import SeasonCalculator
from site_builder.normalize_name import normalize_name

# AHL affiliates 
with open(os.path.join(data_dir, "ahl_affiliates.json")) as f:
    ahl_affiliates = json.load(f)

with open(os.path.join(data_dir, "verified_trades.csv")) as f:
    verified_trades = pd.read_csv(f)
verified_trades['date'] = pd.to_datetime(verified_trades['date'])

# establish list of seasons to scrape (relevant for both scraping operations)
today = datetime.strptime(config['current_date'], '%Y-%m-%d').date()
today_str = config['current_date'].replace("-","")
season_calc = SeasonCalculator(today, os.path.join(data_dir, 'nhl_season_dates.txt')) 
today_season, _ = season_calc.get_season_from_date(today)
seasons_to_scrape = []
nhl_seasons_to_scrape = []
end_year_final = int(today_season.split('-')[1])

# set up nhl player scrape seasons (all seasons from 1990-91 to present)
for start_year in range(scrape_start_year, end_year_final): 
    end_year = start_year+1
    nhl_seasons_to_scrape.append(f"{start_year}-{end_year}")
if not config['prev_file_date']: # we want to scrape all seasons from 1990-91 to present
    seasons_to_scrape = nhl_seasons_to_scrape.copy()
    # for start_year in range(scrape_start_year, end_year_final): 
        # end_year = start_year+1
        # seasons_to_scrape.append(f"{start_year}-{end_year}")
else: # we only want to scrape starting in the season containing prev_file_date
    last_update_season, _ = season_calc.get_season_from_date(pd.to_datetime(config['prev_file_date']))
    for start_year in range(int(last_update_season.split("-")[0]), end_year_final):
        end_year = start_year+1
        seasons_to_scrape.append(f"{start_year}-{end_year}")

def get_tournament_leagues(): # this is a function so we can call it from the side code teammates_db.py
    # first value is month and second value is 0 if the first year in a season should be used, 1 if the second year in the season should be used
    tournament_leagues = {'Hlinka Gretzky Cup': (8, 0), 'WJAC-19': (12, 0), 'Oly-Q': (9, 0), 'Olympics': (2,1), 'wjc-20': (1,1), 'wc': (6,1), 'wjc-18': (4,1), 'WJC-18': (4,1), 'WJC-18 D1A': (4,1), 'W-Cup': (9,0), 'WCup': (9,0), 'nhl-asg': (2,1), 'whc-17': (11,0), 'U17-Dev': (11,0), 'U18-Dev': (11,0)} 
    return tournament_leagues
    
tournament_leagues = get_tournament_leagues()
league_strings = {'Oly-Q': 'Olympics Qualifiers', 'wc': 'Worlds', 'wjc-20': 'World Juniors', 'WCup': 'World Cup', 'nhl-asg': 'NHL ASG', 'wjc-20': 'World Juniors', 'wjc-18': 'WC-U18', 'whc-17': 'WHC-17'}
nhl_leagues_to_drop = {'WC', 'WC-A', 'WJC-A', 'WJ18-A', 'WJC-20', 'M-Cup', 'Champions HL', 'Other', 'WJC-18', 'International', 'WHC-17'}

# FUNCTIONS
def get_team_display_string(league_str, team_str):
    if league_str == 'Olympics':
        return f"{team_str} Olympics"
    elif league_str == 'OGQ':
        return f"{team_str} Olympics Qualifiers"
    elif league_str == 'wc':
        return f"{team_str} Worlds"
    elif league_str == 'WCup':
        return f"{team_str} World Cup"
    elif league_str == 'wjc-20' or league_str == 'wjc-18' or league_str == 'whc-17':
        return f"{team_str}"
    elif league_str == 'nhl-asg':
        return f"{team_str} (NHL)"
    elif league_str != 'NHL':
        return f"{team_str} ({league_str})"
    else:
        return team_str
    
# get all game urls for all seasons 
def get_game_urls(seasons_to_scrape): 
    # create list of season ids (season name without the hyphen because this is the format used for the url)
    season_ids = []
    for season_name in seasons_to_scrape:
        season_id = season_name.replace("-", "")
        season_ids.append((season_id, season_name))
    game_urls = []
    teams_info, teams_by_season = get_all_teams_info()
    for season_id, season_name in season_ids:
        season_game_urls = get_all_game_urls_for_season(season_id, teams_info)
        game_urls.extend(season_game_urls)
    return game_urls, teams_info, teams_by_season

def get_all_game_urls_for_season(season_id, team_info):
    urls = set()
    for _, row in team_info.iterrows():
        _, games_json = scrape_page(f"https://api-web.nhle.com/v1/club-schedule-season/{row['triCode']}/{season_id}")
        if not games_json:
            print(f"No game data found for {row['triCode']} {season_id}")
            continue
        for game in games_json["games"]:
            urls.add(f"https://api-web.nhle.com/v1/gamecenter/{game['id']}/boxscore")
    return urls

def get_all_teams_info():
    url="https://api.nhle.com/stats/rest/en/team"
    _, teams_json = scrape_page(url)
    teams_info = pd.DataFrame.from_records(teams_json["data"])
    teams_info.drop(teams_info[teams_info['rawTricode'] == "NHL"].index, inplace = True)
    # make teams_by_season dict
    teams_by_season = {} # season -> list of teams that were active
    for _, team in teams_info.iterrows():
        _, season_json = scrape_page(f"https://api-web.nhle.com/v1/roster-season/{team['rawTricode']}")
        if not season_json:
            print(f"No season data found for {team['triCode']}")
            continue
        for season in season_json:
            if season not in teams_by_season:
                teams_by_season[season] = []
            teams_by_season[season].append(team['rawTricode'])
    return teams_info, teams_by_season
    
# This function copies the old data to the new database and removes potentially redundant data from the seasons we're about to scrape
def copy_db(seasons_to_scrape, prev_engine, engine):
    # NHL site data
    games_df = pd.read_sql(sql=sql_text("select * from games"), con=prev_engine.connect())
    games_to_drop_df = games_df[games_df['seasonName'].isin(seasons_to_scrape)]
    scratches_df = pd.read_sql(sql=sql_text("select * from scratches"), con=prev_engine.connect())
    game_player_df = pd.read_sql(sql=sql_text("select * from game_player"), con=prev_engine.connect())
    # players_df = pd.read_sql(sql_text("select * from players_nhl"), con=prev_engine.connect()) # just going to drop player data based on duplicates because there's no other way to filter it
    # EP site data -- we fully scrape transfers and ASG data even for a partial season scrape, so the international league data is all that we need to copy/filter
    ep_raw_intl_df = pd.read_sql(sql=sql_text("select * from ep_raw_intl"), con=prev_engine.connect()) # this is raw rather than clean because this is the scraping stage, we'll process later regardless
    ep_raw_intl_df = ep_raw_intl_df.loc[~ep_raw_intl_df['season'].isin(seasons_to_scrape)]
    # drop games from the seasons-to-scrape 
    games_df = games_df[~games_df['gameId'].isin(games_to_drop_df['gameId'])] 
    scratches_df = scratches_df[~scratches_df['gameId'].isin(games_to_drop_df['gameId'])]
    game_player_df = game_player_df[~game_player_df['gameId'].isin(games_to_drop_df['gameId'])]
    # copy to new db
    games_df.to_sql(name="games", con=engine, index=False)
    scratches_df.to_sql(name="scratches", con=engine, index=False)
    game_player_df.to_sql(name="game_player", con=engine, index=False)
    return ep_raw_intl_df    

def get_team_name_from_id(team_json, teams_info):
    team_lookup = teams_info.loc[teams_info['id']==team_json['id']]
    if len(team_lookup) == 1:
        return team_lookup.iloc[0]["fullName"]
    else:
        return team_json["name"]["default"]
    
def scrape_game_pages(game_urls, teams_info, engine):
    all_stats_list = ["toi", "assists", "points", "goals", "shots", "hits", "powerPlayGoals", "powerPlayPoints",
                    "pim", "faceOffWinningPctg", "faceoffs", "shortHandedGoals",
                    "shPoints", "blockedShots", "plusMinus", "powerPlayToi", "shorthandedToi",
                    "evenStrengthShotsAgainst", "powerPlayShotsAgainst", "shorthandedShotsAgainst", "saveShotsAgainst",
                    "evenStrengthGoalsAgainst", "powerPlayGoalsAgainst", "shorthandedGoalsAgainst", "goalsAgainst"]
    # scrape games
    print(f"num game ids: {len(game_urls)}")
    games_info = []
    game_player_data = []
    scratch_data = []
    player_ids = set()
    player_urls = set()
    threads = min(MAX_THREADS, len(game_urls))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(scrape_page, game_urls)
        for game_url, game_json in results:
            if not game_json:
                print(f"No game data found for {game_url}")
                continue
            game_id = int(game_url.split("/")[5])
            if not game_json:
                continue  # check if scrape of this page failed
            # check if game has been played yet
            if "score" not in game_json["homeTeam"]:
                continue
            if game_json["awayTeam"]["score"] > game_json["homeTeam"]["score"]:
                winningTeam = "awayTeam"
            else:
                winningTeam = "homeTeam"
            games_info.append([game_id, game_url, game_json['gameDate'], game_json['gameType'], game_json['season'], season_id_to_name(game_json['season']), get_team_name_from_id(game_json['awayTeam'], teams_info), get_team_name_from_id(game_json['homeTeam'], teams_info), game_json['awayTeam']['score'], game_json['homeTeam']['score'], game_json['venue']['default'], winningTeam])
            # process game roster
            for team in ["homeTeam", "awayTeam"]:
                if "gameInfo" in game_json["summary"] and "scratches" in game_json["summary"]["gameInfo"][team]:
                    for scratch in game_json["summary"]["gameInfo"][team]["scratches"]: # updated
                        scratch_data.append([game_id, scratch["id"]])
                for player_type in ["forwards", "defense", "goalies"]:
                    # for player in game_json["boxscore"]["playerByGameStats"][team][player_type]:
                    for player in game_json["playerByGameStats"][team][player_type]:
                        if player["playerId"] >= 9000000: # these seem to be players who played against NHL team (e.g. preseason game vs European team) who are not NHL players and therefore don't have a real profile page or id
                            continue # assume this is a dummy id, we're just going to ignore this player
                        game_player_data_list = [game_id, player["playerId"], team]
                        player_ids.add(player["playerId"])
                        player_urls.add(f"https://api-web.nhle.com/v1/player/{player['playerId']}/landing")
                        # get player stats
                        for stat_name in all_stats_list:
                            if stat_name in player:
                                game_player_data_list.append(player[stat_name])
                            else:
                                game_player_data_list.append("-")
                        if "saveShotsAgainst" in player or "savePctg" in player:
                            game_player_data_list.extend(computeShotsSaves(player)) # compute and add shotsAgainst, saves, savePercentage 
                        else:
                            game_player_data_list.extend(["-", "-", "-"])
                        game_player_data.append(game_player_data_list) 
    # write to database    
    games_df = pd.DataFrame(games_info, columns=['gameId', 'gameUrl', 'gameDate', 'gameType', 'seasonId', 'seasonName', 'awayTeam', 'homeTeam', 'awayScore', 'homeScore', 'venueName', 'winningTeam'])
    games_df.to_sql(name="games", if_exists='append', index=False, con=engine) 
    scratches_df = pd.DataFrame(scratch_data, columns=["gameId", "playerId"])
    scratches_df.to_sql(name="scratches", if_exists='append', index=False, con=engine)
    columns_list = ["gameId", "playerId", "team"] + all_stats_list + ["shotsAgainst", "saves", "savePercentage"]# skater_stats_names + goalie_stats_names
    game_player_df = pd.DataFrame(game_player_data, columns=columns_list)
    game_player_df.to_sql(name="game_player", if_exists='append', index=False, con=engine)

def computeShotsSaves(goalieStats):
    tmp = goalieStats["saveShotsAgainst"].split("/")
    if len(tmp) != 2:
        print("goalie stats error")
        print(goalieStats["saveShotsAgainst"])
        shots_against = 0
        saves = 0
    else:
        shots_against = int(tmp[1])
        saves = int(tmp[0])
    if "savePctg" in goalieStats: # old stats have savePctg, stats starting in 23-24 don't?
        save_pct = goalieStats["savePctg"] 
    else:
        if shots_against == 0:
            save_pct = 0.0
        else:
            save_pct = float(saves) / shots_against
    return shots_against, saves, save_pct

def get_career_team_color(league):
    if league in tournament_leagues:
        return "80b4f2" # tournament color
    else:
        return "2e7cdb" # non-tournament team color

def season_id_to_name(season_id):
    season_id_str = str(season_id)
    season_name = f"{season_id_str[:4]}-{season_id_str[4:]}"
    return season_name

# get all players team-season terms from NHL site
def scrape_nhl_skaters(engine, teams_by_season):#, old_players_df=None):
    # get all players on NHL rosters for all specified seasons
    roster_player_urls = set()
    team_season_roster_urls = []
    for season_to_scrape in nhl_seasons_to_scrape:
        season_id = int(season_to_scrape.replace("-", ""))
        if season_id not in teams_by_season:
            print(f"{season_id} not found")
            continue
        teams_to_scrape = teams_by_season[season_id]
        for team_tricode in teams_to_scrape:
            team_season_roster_urls.append(f"https://api-web.nhle.com/v1/roster/{team_tricode}/{season_id}")
    threads = min(MAX_THREADS, len(team_season_roster_urls))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(scrape_page, team_season_roster_urls)
        for roster_url, roster_json in results:
            if not roster_json:
                print(f"No roster data found for {roster_url}")
                continue
            for player in roster_json['forwards']:
                roster_player_urls.add(f"https://api-web.nhle.com/v1/player/{player['id']}/landing")
            for player in roster_json['defensemen']:
                roster_player_urls.add(f"https://api-web.nhle.com/v1/player/{player['id']}/landing")
            for player in roster_json['goalies']:
                roster_player_urls.add(f"https://api-web.nhle.com/v1/player/{player['id']}/landing")
    # now get all team terms for those players
    roster_player_urls = list(roster_player_urls)
    print('https://api-web.nhle.com/v1/player/8476876/landing' in roster_player_urls)
    player_terms = []
    threads = min(MAX_THREADS, len(roster_player_urls))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(scrape_page, roster_player_urls)
        for player_url, player_json in results:
            if not player_json:
                print(f"No player data found for {player_url}")
                continue
            player_id = player_url.split("/")[5]
            for term in player_json['seasonTotals']:
                if player_url=='https://api-web.nhle.com/v1/player/8476876/landing':
                    print(term)
                if 'gamesPlayed' in term:
                    games_played = term['gamesPlayed']
                else:
                    games_played = -1
                if term['gameTypeId'] != 2:
                    continue # skip playoffs 
                player_terms.append((player_id, f"{player_json['firstName']['default']} {player_json['lastName']['default']}", term['season'], term['leagueAbbrev'], term['teamName']['default'], games_played))
    player_terms_df = pd.DataFrame(player_terms, columns=['playerId', 'playerName', 'season', 'league', 'team', 'games_played']) 
    player_terms_df['season'] = player_terms_df['season'].apply(season_id_to_name)
    # if old_players_df is not None:
    #     player_terms_df = pd.concat([player_terms_df, old_players_df], axis='rows')
    #     player_terms_df.sort_values(by=['games_played'], inplace=True)
    #     # current season will appear with varying numbers of games played as the season progresses, so we can't use games_played to drop duplicates
    #     player_terms_df.drop_duplicates(subset=['playerId', 'season', 'league', 'team'], keep='last', inplace=True) 
    #     # player_terms_df = player_terms_df.drop_duplicates() 
    player_terms_df.to_sql(name="players_nhl", index=False, con=engine)

def scrape_ep(engine, old_ep_raw_intl=None): 
    leagues = ["wc", "wjc-20", "wjc-18", "whc-17"]
    postseasons = pd.read_csv(os.path.join(data_dir, "ep_raw_postseasons.csv"))
    # get ASG data
    asg_data = scraper.get_asg()
    asg_data = asg_data.drop(['index'], axis=1, errors='ignore')
    # remove positions/whitespace from ep names
    asg_data['player'] = asg_data['player'].str.replace(r"\(.*\)", "", regex=True)
    asg_data['player'] = asg_data['player'].str.strip()    
    # get skaters
    skaters = scraper.get_skaters(leagues, seasons_to_scrape)
    # remove positions/whitespace from ep names
    skaters['player'] = skaters['player'].str.replace(r"\(.*\)", "", regex=True)
    skaters['player'] = skaters['player'].str.strip()
     # scrape transfer data and save to raw db
    transfers = scraper.get_transfers(earliest_transfer_date)#, max_page=2)
    transfers.date = pd.to_datetime(transfers.date)
    transfers['player'] = transfers['player'].str.replace(r"\(.*\)", "", regex=True)
    transfers['player'] = transfers['player'].str.strip()
    # combine previous and new skater data
    if old_ep_raw_intl is not None:
        skaters = pd.concat([skaters, old_ep_raw_intl], axis='rows')
        skaters = skaters.drop_duplicates()
    # write data
    transfers.to_sql('ep_raw_transfers', engine, index=False)
    skaters.to_sql('ep_raw_intl', engine, if_exists='append', index=False)
    asg_data.to_sql('ep_raw_asg', engine, index=False)
    postseasons.to_sql('ep_raw_postseasons', engine, index=False)

def process_player_terms(player_terms, transfers, postseasons, test_id=None):
    # process raw data into app format and save to new db
    all_rows = []
    all_playoffs = []
    all_unused_transfers = []
    # build player timelines using NHL site data
    for player_id in player_terms['playerId'].unique():
        if test_id and player_id != test_id:
            continue
        player_rows = player_terms.loc[player_terms['playerId'] == player_id]
        career_rows, playoffs_rows, unused_transfers = build_player_timeline_new(player_rows, transfers, postseasons)
        all_unused_transfers.extend(unused_transfers)
        all_rows.extend(career_rows)
        all_playoffs.extend(playoffs_rows)
        if test_id:
            print(player_rows)
            print(career_rows)
            print(playoffs_rows)
            return
    # postprocessing players
    processed_players = pd.DataFrame(all_rows, columns=['team', 'league', 'start_date', 'end_date', 'playerId'])
    processed_players[['start_year_js', 'start_month_js', 'start_day_js']] = processed_players.apply(lambda x: get_js_date_values(x.start_date), axis=1, result_type='expand')
    processed_players[['end_year_js', 'end_month_js', 'end_day_js']] = processed_players.apply(lambda x: get_js_date_values(x.end_date), axis=1, result_type='expand')
    processed_players['team_display_str'] = processed_players.apply(lambda x: get_team_display_string(x.league, x.team), axis=1)
    processed_players['color'] = processed_players.apply(lambda x: get_career_team_color(x.league), axis=1) # add colors for career teams (tournaments vs not)
    processed_players['years_str'] = processed_players.apply(lambda x: get_years_str(x.league, x.start_date, x.end_date), axis=1)
    processed_players['tooltip_str'] = processed_players.apply(lambda x: get_player_term_tooltip_str(x.team_display_str, x.years_str), axis=1)
    # postprocessing playoffs
    processed_playoffs = pd.DataFrame(all_playoffs, columns=['link', 'year', 'tooltip_str', 'color'])
    out = processed_playoffs.apply(lambda x: get_js_date_values_playoffs(x.year), axis=1, result_type='expand')   
    processed_playoffs[['start_year_js', 'start_month_js', 'start_day_js', 'end_year_js', 'end_month_js', 'end_day_js']] = out
    processed_players.to_sql('skaters', engine, if_exists='replace', index=False)
    processed_playoffs.to_sql('player_playoffs', engine, if_exists='replace', index=False)
    # create teammate intersection table
    teammates = build_teammates_df(processed_players)
    teammates.to_sql('teammates', engine, if_exists='replace', index=False)
    # create unused transfers table for fixing by hand
    all_unused_transfers = pd.DataFrame(all_unused_transfers, columns=['date', 'from_team', 'to_team', 'link', 'player', 'playerId', 'playerName'])
    all_unused_transfers.to_sql('unused_transfers', engine, index=False)

def get_player_term_tooltip_str(team_display_str, years_str):
    return f"{team_display_str}<br>{years_str}"

def get_js_date_values(date):
    return date.year, date.month-1, date.day

def get_js_date_values_playoffs(year):
    return year, 4, 1, year, 4, 30 

# returns tuple (start_date, end_date) that defines the overlapping interval,
# the number of seasons in the interval,
# and True/False whether the overlap is invalid (occurs within a single offseason)
def compute_overlap_interval(start_date, end_date, league):
    num_seasons = max(1, end_date.year-start_date.year)
    if start_date <= end_date:
        # check if overlap is valid or not: invalid overlap has start and end dates in the same offseason
        season1, in_season1 = season_calc.get_season_from_date(start_date)
        season2, in_season2 = season_calc.get_season_from_date(end_date)
        valid = True
        if not in_season1 and not in_season2 and season1 == season2:
            valid = False
        if league in tournament_leagues:
            valid = True
        return num_seasons, valid
    else:
        return 0, False

# TODO need to fix this for tournament dates (which dates, which tournaments, all of them?)
def get_years_str(league, start_date, end_date):
    years_str = f"{start_date.year}-{end_date.year}"
    if league in tournament_leagues:
        _, tourney_year = tournament_leagues[league]
        if tourney_year == 0: # start date occurs in the first year of the season
            years_str = f"{start_date.year}-{start_date.year+1}"
        else: # start date occurs in the second year of the season
            years_str = f"{start_date.year-1}-{start_date.year}"
    else: 
        # catch delayed season start exceptions
        if start_date == pd.to_datetime("2013-01-19"): # adjust 2012-2013 lockout date season start
            years_str = "2012" + years_str[4:]
        elif start_date == pd.to_datetime("2021-01-13"):
            years_str = "2020" + years_str[4:] 
    return years_str

def get_league_display_string(league_str):
    if league_str not in league_strings:
        return league_str
    return league_strings[league_str]
    
def build_teammates_df(players_df):
    teammates = pd.merge(players_df, players_df, on=['league','team'])
    teammates = teammates[(teammates['start_date_x'] <= teammates['end_date_y']) & (teammates['start_date_y'] <= teammates['end_date_x'])]
    teammates = teammates[teammates['playerId_x']!=teammates['playerId_y']] # player can't overlap with himself
    # compute overlap
    teammates['overlap_start_date'] = teammates[['start_date_x','start_date_y']].max(axis=1)
    teammates['overlap_end_date'] = teammates[['end_date_x','end_date_y']].min(axis=1)
    teammates['overlap_len'] = teammates['overlap_end_date'] - teammates['overlap_start_date']
    teammates['overlap_start_year_js'] = pd.DatetimeIndex(teammates['overlap_start_date']).year
    teammates['overlap_start_month_js'] = pd.DatetimeIndex(teammates['overlap_start_date']).month-1
    teammates['overlap_start_day_js'] = pd.DatetimeIndex(teammates['overlap_start_date']).day
    teammates['overlap_end_year_js'] = pd.DatetimeIndex(teammates['overlap_end_date']).year
    teammates['overlap_end_month_js'] = pd.DatetimeIndex(teammates['overlap_end_date']).month-1
    teammates['overlap_end_day_js'] = pd.DatetimeIndex(teammates['overlap_end_date']).day
    # get interval length, check if valid (doesn't occur solely in an offseason)
    teammates[['num_seasons','is_valid_interval']] = teammates.apply(lambda x: compute_overlap_interval(x.overlap_start_date, x.overlap_end_date, x.league), axis=1, result_type='expand')
    # get display strings
    teammates['years_str'] = teammates.apply(lambda x: get_years_str(x.league, x.overlap_start_date, x.overlap_end_date), axis=1) # TODO is this wrong for tournaments? just ASG?
    teammates['league_display_str'] = teammates.apply(lambda x: get_league_display_string(x.league), axis=1)
    teammates['team_display_str'] = teammates.apply(lambda x: get_team_display_string(x.league, x.team), axis=1)
    # drop invalid intervals (contained within an offseason)
    teammates = teammates.loc[teammates['is_valid_interval']]
    # TODO drop player names (because we only want to use canon_name field from name db)
    return teammates

# TODO this feels really convoluted? not sure how necessary this code is
def convert_tournament_dates(tournament, start_date, end_date):
    season_years = (start_date.year, end_date.year)
    season_years_str = f"{start_date.year}-{end_date.year}" # TODO move
    # catch delayed season start exceptions
    if season_years_str == "2013-2013":
        season_years = (2012,2013)
    elif season_years_str == "2021-2021":
        season_years = (2020, 2021)
    if tournament in tournament_leagues:
        tourney_month, tourney_year = tournament_leagues[tournament]
    else:
        tourney_month, tourney_year = (4,1)
        print(f"Tournament not found: {tournament}")
    return (pd.to_datetime(f"{season_years[tourney_year]}/{tourney_month}/1"), pd.to_datetime(f"{season_years[tourney_year]}/{tourney_month}/28"))

def id_adjacent_team_rows(team_rows):
    # identify lists of adjacent seasons
    adj_seasons = []
    seasons = team_rows['season'].tolist()
    season_sort_ids = []
    season_map = {}
    for season in seasons:
        season_id = int(season.split("-")[0])
        season_map[season_id] = season
        season_sort_ids.append(season_id)
    season_sort_ids.sort() # seasons in sorted order by value of first year
    prev_season_id = 0
    for idx, season_id in enumerate(season_sort_ids):
        season_name = season_map[season_id]
        if prev_season_id + 1 == season_id: # this season is immediately adjacent to the last one
            adj_seasons[len(adj_seasons)-1].append(season_name)
        else:
            adj_seasons.append([season_name])
        prev_season_id = season_id
    # now get the corresponding roster row for these seasons
    adj_team_rows = []
    for season_list in adj_seasons:
        rows_tmp = pd.DataFrame()
        for season in season_list:
            row = team_rows.loc[team_rows.season==season]
            rows_tmp = pd.concat([rows_tmp, row])
        rows_tmp.drop(['index'], axis=1, errors='ignore')
        adj_team_rows.append(rows_tmp)
    return adj_team_rows

def get_team_subwords(team):
    return team.split() + [team]

def build_player_timeline_new(rows, transfers, postseasons):
    rows['start_date'] = 0
    rows['end_date'] = 0
    player_id = rows.iloc[0]['playerId']
    player_timeline = []
    player_trade_candidates = transfers.loc[transfers['playerId']==player_id] 
    player_trade_candidates['orig_transfer'] = True
    # expand trade candidates with AHL teams
    ahl_rows = []
    for _, row in player_trade_candidates.iterrows():
        from_team_alts = [row['from_team']]
        to_team_alts = [row['to_team']]
        if row['from_team'] in ahl_affiliates:
            for ahl_team in ahl_affiliates[row['from_team']]:
                from_team_alts.append(ahl_team)
        if row['to_team'] in ahl_affiliates:
            for ahl_team in ahl_affiliates[row['to_team']]:
                to_team_alts.append(ahl_team)
        for from_team in from_team_alts:
            for to_team in to_team_alts:
                if from_team == row['from_team'] and to_team == row['to_team']:
                    continue # don't add a duplicate trade
                ahl_rows.append([row['date'], from_team, to_team, row['link'], row['player'], row['playerId'], row['playerName'], False]) 
    player_trade_candidates = pd.concat([player_trade_candidates, pd.DataFrame(ahl_rows, columns=['date', 'from_team', 'to_team', 'link', 'player', 'playerId', 'playerName', 'orig_transfer'])])
    # condense adjacent seasons for the same team -- we're now ignoring league name differences (bc of NHL inconsistent data)
    for team in rows['team'].unique():
        # TODO check usntdp here
        team_rows = rows.loc[(rows['team'] == team)] # & (rows['league'] == league)]
        # unify league name
        team_leagues = team_rows['league'].unique()
        team_leagues = sorted(team_leagues)
        league = team_leagues[0]
        # check for tournament special cases: no merging, each row becomes its own row in the timeline
        if league in tournament_leagues:
            # use all available leagues
            for league_option in team_leagues:
                team_rows = rows.loc[(rows['team'] == team) & (rows['league'] == league_option)]
                seasons = team_rows['season'].tolist()
                for _, season in enumerate(seasons):
                    season_start, season_end, _ = season_calc.get_season_dates(season)
                    # adjust for tournaments
                    tournament_start_date, tournament_end_date = convert_tournament_dates(league_option, season_start, season_end)
                    player_timeline.append([team, league_option, tournament_start_date, tournament_end_date, player_id])
            continue
        adjacent_team_rows = id_adjacent_team_rows(team_rows)
        # for a list of adjacent seasons on a roster, we will add at least 1 tenure row to the player timeline
        for adj_season_rows in adjacent_team_rows:
            # select all trades that occurred in this time period
            term_start_date = season_calc.get_season_dates(adj_season_rows.iloc[0]['season'])[0]
            term_end_date_soft = season_calc.get_season_dates(adj_season_rows.iloc[-1]['season'])[1]
            # create a new tenure row (without checking trades)
            player_timeline.append([team, league, term_start_date, term_end_date_soft, player_id, adj_season_rows.iloc[0].season, adj_season_rows.iloc[-1].season])
    # now consider trades TODO this will need the most refinement maybe
    unused_transfers = []
    player_timeline = pd.DataFrame(player_timeline, columns=['team', 'league', 'term_start_date', 'term_end_date', 'player_id', 'first_season', 'last_season'])
    for _, row in player_trade_candidates.iterrows():
        trade_season, trade_in_season = season_calc.get_season_from_date(row.date)
        if not trade_in_season: # find offseason trade
            trade_season_next = season_calc.get_next_season(trade_season)
            from_row = player_timeline.loc[(player_timeline['team']==row.from_team) & (player_timeline['last_season']==trade_season)]
            to_row = player_timeline.loc[(player_timeline['team']==row.to_team) & (player_timeline['first_season']==trade_season_next)]
            if len(from_row) == 1 and len(to_row) == 1:
                player_timeline.loc[(player_timeline['team']==row.from_team) & (player_timeline['last_season']==trade_season), 'term_end_date'] = row['date'] - pd.to_timedelta(1, unit='d')
                player_timeline.loc[(player_timeline['team']==row.to_team) & (player_timeline['first_season']==trade_season_next), 'term_start_date'] = row['date'] 
                continue 
            elif len(from_row) == 1 and (row.to_team.lower() in ('retired', 'no team', 'unknown') or is_verified_trade(row)):
                player_timeline.loc[(player_timeline['team']==row.from_team) & (player_timeline['last_season']==trade_season), 'term_end_date'] = row['date'] - pd.to_timedelta(1, unit='d')
                continue
            elif len(to_row) == 1 and (row.from_team.lower() in ('retired', 'no team', 'unknown') or is_verified_trade(row)):
                player_timeline.loc[(player_timeline['team']==row.to_team) & (player_timeline['first_season']==trade_season_next), 'term_start_date'] = row['date'] 
                continue
        # TODO make trade date adjustment into a function
        if trade_in_season: # find in-season trade
            from_row = player_timeline.loc[(player_timeline['team']==row.from_team) & (player_timeline['last_season']==trade_season)]
            to_row = player_timeline.loc[(player_timeline['team']==row.to_team) & (player_timeline['first_season']==trade_season)]
            if len(from_row) == 1 and len(to_row) == 1:
                player_timeline.loc[(player_timeline['team']==row.from_team) & (player_timeline['last_season']==trade_season), 'term_end_date'] = row['date'] - pd.to_timedelta(1, unit='d')
                player_timeline.loc[(player_timeline['team']==row.to_team) & (player_timeline['first_season']==trade_season), 'term_start_date'] = row['date'] 
                continue
            elif len(from_row) == 1 and (row.to_team.lower() in ('retired', 'no team', 'unknown') or is_verified_trade(row)):
                player_timeline.loc[(player_timeline['team']==row.from_team) & (player_timeline['last_season']==trade_season), 'term_end_date'] = row['date'] - pd.to_timedelta(1, unit='d')
                continue
            elif len(to_row) == 1 and (row.from_team.lower() in ('retired', 'no team', 'unknown') or is_verified_trade(row)):
                player_timeline.loc[(player_timeline['team']==row.to_team) & (player_timeline['first_season']==trade_season), 'term_start_date'] = row['date'] 
                continue
        # this trade didn't match a pair of teams, only add if this was an original transfer (not one from AHL team expansion options)
        if row['orig_transfer']:
            unused_transfers.append(row)
    player_timeline.drop(['first_season', 'last_season'], axis=1, inplace=True) # TODO maybe keep these columns around? could be useful
    # playoffs 
    player_timeline = player_timeline.values.tolist()
    playoffs = get_player_playoffs(postseasons, player_timeline, player_id)
    return player_timeline, playoffs, unused_transfers

def is_verified_trade(row):
    if len(verified_trades.loc[(verified_trades['date']==row['date']) & (verified_trades['playerName']==row['player'])]) == 1:
        return True
    return False    

def build_player_timeline(rows, transfers, postseasons):
    rows['start_date'] = 0
    rows['end_date'] = 0
    player_id = rows.iloc[0]['playerId']
    player_timeline = []
    # expand trade candidates with AHL teams
    player_trade_candidates = transfers.loc[transfers['playerId']==player_id] 
    ahl_rows = []
    for _, row in player_trade_candidates.iterrows():
        if row.from_team in ahl_affiliates:
            # new row: we remove the to_team because we don't want to increase the number of trades for that team
            ahl_rows.append([row['date'], ahl_affiliates[row['from_team']], "", row['playerId'], row['league'], row['player']]) # TODO this will be a list of possible AHL affiliates
        if row.to_team in ahl_affiliates:
            # new row: we remove the from_team because we don't want to increase the number of trades for that team
            ahl_rows.append([row['date'], "", ahl_affiliates[row['to_team']], row['playerId'], row['league'],
                                row['player']])  # TODO this will be a list of possible AHL affiliates
    ahl_df = pd.DataFrame(ahl_rows, columns=['date', 'from_team', 'to_team', 'playerId', 'league', 'player'])
    player_trade_candidates = pd.concat([player_trade_candidates, ahl_df])
    # condense adjacent seasons for the same team
    for league in rows['league'].unique():
        league_rows = rows.loc[rows['league'] == league]
        for team in league_rows['team'].unique():
            if team == 'USNTDP Juniors': # TODO is this still true for NHL data?
                continue # USHL is weird and this team encompasses both U17 and U18 teams so it's redundant/too coarse
            team_rows = rows.loc[(rows['team'] == team) & (rows['league'] == league)]
            # check for tournament special cases: no merging, each row becomes its own row in the timeline
            if league in tournament_leagues:
                seasons = team_rows['season'].tolist()
                for _, season in enumerate(seasons):
                    season_start, season_end, _ = season_calc.get_season_dates(season)
                    # adjust for tournaments
                    tournament_start_date, tournament_end_date = convert_tournament_dates(league, season_start, season_end)
                    player_timeline.append([team, league, tournament_start_date, tournament_end_date, player_id])
                continue
            adjacent_team_rows = id_adjacent_team_rows(team_rows)
            # for a list of adjacent seasons on a roster, we will add at least 1 tenure row to the player timeline
            for adj_season_rows in adjacent_team_rows:
                # select all trades that occurred in this time period
                term_start_date = season_calc.get_season_dates(adj_season_rows.iloc[0]['season'])[0]
                term_start_season = adj_season_rows.iloc[0].season
                term_end_date_soft = season_calc.get_season_dates(adj_season_rows.iloc[-1]['season'])[1]
                term_end_date = season_calc.get_season_dates(adj_season_rows.iloc[-1]['season'])[2]
                prev_term_offseason_start = season_calc.get_season_dates(season_calc.get_prev_season(adj_season_rows.iloc[0]['season']))[1]
                # removed league equality check on the following query because it restricted trades to NHL teams only (even when the trade involved another team in AHL, NCAA etc)
                trade_candidates = player_trade_candidates.loc[(player_trade_candidates['date'] >= prev_term_offseason_start) & (player_trade_candidates['date'] <= term_end_date) & ((player_trade_candidates['to_team']==team) | (player_trade_candidates['from_team']==team))]
                # if no trades, create a new tenure row
                if len(trade_candidates) == 0:
                    player_timeline.append([team, league, term_start_date, term_end_date_soft, player_id])
                else: # iterate over trade candidates to build tenure rows
                    chronological_trades = trade_candidates.sort_values(by='date')
                    currently_on_team = True
                    current_start_date = term_start_date
                    left_team_season = 0
                    trade_idx = 0
                    for _, trade in chronological_trades.iterrows():
                        trade_season = season_calc.get_season_from_date(trade['date'])[0]
                        if trade['from_team'] == trade['to_team']:
                            # ignore extensions
                            continue
                        if trade_idx == 0 and trade_season == term_start_season:
                            # they were traded to the team in this season, so they started the season not on the team
                            currently_on_team = False
                        if team == trade['to_team'] and trade_season == season_calc.get_prev_season(term_start_season):
                            # they were traded to this team in the offseason before this season (see Dylan Strome, Johnny Gaudreau)
                            current_start_date = trade['date']
                            currently_on_team = True
                        elif team == trade['to_team'] and not currently_on_team:
                            if left_team_season != 0 and left_team_season != season_calc.get_prev_season(trade_season) and left_team_season != trade_season:
                                # if the last trade away was NOT last season or this season, then there's a missing transfer back (see lockouts)
                                # we need to add a new tenure row for that span of time up to end of last season (assume they're traded BACK to this team in this season)
                                last_season_end = season_calc.get_season_dates(
                                    season_calc.get_prev_season(
                                        season_calc.get_season_from_date(trade['date'])[0]))[1]
                                if current_start_date < last_season_end:
                                    # this is a hack
                                    player_timeline.append([team, league, current_start_date, last_season_end, player_id])
                            # beginning of a new tenure with this team
                            current_start_date = trade['date']
                            currently_on_team = True
                        elif team == trade['to_team'] and currently_on_team:
                            # This catches Vinny because there's a weird Bakersfield->Oilers transaction in a year where he didn't actually end up on the Oilers? 
                            # Tuukka Rask case: was UFA, not signed, rejoined this team in the middle of the season
                            # we need to add the last tenure (we know it exists and hasn't already been added because currently_on_team is True)
                            # TODO actually Tuukka Rask doesn't fall into this category because of the **** lockout, who does?
                            last_season_end = season_calc.get_season_dates(season_calc.get_prev_season(season_calc.get_season_from_date(trade['date'])[0]))[1]
                            if current_start_date < last_season_end:
                                # this is a hack
                                player_timeline.append([team, league, current_start_date, last_season_end, player_id])
                        elif team == trade['from_team'] and currently_on_team:
                            # a normal trade from this team: add a tenure row for the tenure up to this point
                            if current_start_date < trade['date'] - pd.to_timedelta(1, unit='d'):
                                # this is a hack :(
                                player_timeline.append([team, league, current_start_date, trade['date'] - pd.to_timedelta(1, unit='d'), player_id])
                            # set start date to beginning of next season in the case that there is no trade back (everyone e.g. Claude Giroux after the lockout in 2012)
                            # if there's a following trade back, this date won't be used
                            current_start_date = season_calc.get_season_dates(season_calc.get_next_season(season_calc.get_season_from_date(trade['date'])[0]))[0]
                            left_team_season = season_calc.get_season_from_date(trade['date'])[0]
                            currently_on_team = False
                        elif team == trade['from_team'] and not currently_on_team:
                            # this happens when the trade back to this team wasn't properly recorded (Claude Giroux and lockout)
                            # current_start_date should have already been set to the next season following the previous trade away
                            if current_start_date < trade['date'] - pd.to_timedelta(1, unit='d'):
                                # this is a hack :(
                                player_timeline.append([team, league, current_start_date,
                                                    trade.date - pd.to_timedelta(1, unit='d'), player_id])
                            current_start_date = season_calc.get_season_dates(season_calc.get_next_season(
                                season_calc.get_season_from_date(trade['date'])[0]))[0]
                            left_team_season = season_calc.get_season_from_date(trade['date'])[0]
                            currently_on_team = False
                        trade_idx += 1
                    # if this tenure ended without a trade, add it
                    # OR we ended with a trade away but the player clearly came back in the following season because they remained on the roster (lockouts etc, see Claude Giroux)
                    if currently_on_team or left_team_season != adj_season_rows.iloc[-1]['season']:
                        if current_start_date > term_end_date_soft:
                            # off-season trade (back?) to this team for players that aren't listed on next season's roster yet
                            term_end = term_end_date
                        else:
                            term_end = term_end_date_soft
                        player_timeline.append([team, league, current_start_date, term_end, player_id])
    playoffs = get_player_playoffs(postseasons, player_timeline, player_id)
    return player_timeline, playoffs, None

def get_player_playoffs(postseasons, player_timeline, player_id):
    # playoffs
    playoff_queries = []
    for term in player_timeline:
        if term[1] != "NHL":
            continue
        possible_first_season = f"{term[2].year-1}-{term[2].year}"
        first_season_dates = season_calc.get_season_dates(possible_first_season)
        playoffs_end_date = first_season_dates[1]
        if term[2] < playoffs_end_date and term[3] >= playoffs_end_date: # check whether the player was on the team during playoffs in the first possible season of their tenure (approximation)
            playoff_queries.append((term[0], possible_first_season))
        if term[2].year != term[3].year: # check following seasons 
            for i in range(term[2].year, term[3].year-1): # for all seasons in the middle of this tenure, player was definition on the roster during playoffs
                playoff_queries.append((term[0], f"{i}-{i+1}"))
            possible_last_season = f"{term[3].year-1}-{term[3].year}"
            last_season_dates = season_calc.get_season_dates(possible_last_season)
            playoffs_end_date = last_season_dates[1]
            if term[2] < playoffs_end_date and term[3] >= playoffs_end_date: # check whether the player was on the team during playoffs in the last possible season
                playoff_queries.append((term[0], f"{term[3].year-1}-{term[3].year}"))
    playoffs = []
    for team, playoff_year in playoff_queries:
        team_str = unidecode(team)
        year = playoff_year.split("-")[1]
        result = postseasons.loc[
            (postseasons['team'] == team_str)]
        result = postseasons.loc[(postseasons['team']==team_str) & (postseasons['season']==playoff_year)]['postseason']
        if len(result) == 0:
            continue
        result = result.item()
        tooltip_str = f"{playoff_year}<br>{result}"
        if result == "Did not make playoffs":
            color = "#cfcecc" # missed playoffs color
        elif result == "Champion":
            color = "fae525" # won Cup color
        else:
            color = "ebb11e" # made playoffs but didn't win Cup color # TODO change the color to something else?
        playoffs.append([player_id, year, tooltip_str, color])
    return playoffs

def find_duplicates(nhl_db_players, ep_transfers, ep_asg, ep_intl): #data_dir, ep_db, nhl_db, links_filename, out_filename):
    # read previous links file
    disambiguated_links = set()
    with open(os.path.join(data_dir, "name_id_links.csv")) as in_file:
        for line in in_file:
            tmp = line.strip().split(",")
            if len(tmp) != 3:
                print(f"skipped line: {tmp}")
                continue
            ep_link = tmp[0].strip()
            if ep_link != "":
                disambiguated_links.add(ep_link)
            player_id = tmp[1].strip()
            if player_id != "":
                disambiguated_links.add(player_id)
    ep_players = pd.concat([ep_transfers[['link', 'player']], ep_asg[['link', 'player']], ep_intl[['link', 'player']]])
    unique_people_links_nhl = nhl_db_players['playerId'].unique()
    unique_people_links_ep = ep_players['link'].unique()
    unique_people_names_nhl = []
    unique_people_names_ep = []
    for player_id in unique_people_links_nhl:
        unique_people_names_nhl.append(nhl_db_players.loc[nhl_db_players['playerId'] == player_id].iloc[0]['playerName'])
    for link in unique_people_links_ep:
        unique_people_names_ep.append(ep_players.loc[ep_players['link'] == link].iloc[0]['player'])
    # NHL DB
    norm_names = [normalize_name(name) for name in unique_people_names_nhl]
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
    norm_names = [normalize_name(name) for name in unique_people_names_ep]
    norm_names_seen = {}
    for idx, name in enumerate(norm_names):
        if name not in norm_names_seen:
            norm_names_seen[name] = []
        norm_names_seen[name].append(idx)
    for name in norm_names_seen:
        if len(norm_names_seen[name]) > 1:
            for idx in norm_names_seen[name]:
                output_duplicates.append((name, unique_people_names_ep[idx], unique_people_links_ep[idx]))
    # other duplicate scenario: links or playerIds that are mapped to multiple alternatives in links table
    links_df = pd.read_sql_query(sql=sql_text("select * from links"), con=engine.connect()) 
    pId_counts = links_df.playerId.value_counts()
    dupe_pId_rows = links_df[links_df.playerId.isin(pId_counts.index[pId_counts.gt(1)])]
    dupe_pId_rows.replace('', np.nan, inplace=True)
    dupe_pId_rows.dropna(subset=['playerId'], inplace=True)
    link_counts = links_df.link.value_counts()
    dupe_link_rows = links_df[links_df.playerId.isin(link_counts.index[link_counts.gt(1)])]
    dupe_link_rows.replace('', np.nan, inplace=True)
    dupe_link_rows.dropna(subset=['link'], inplace=True)
    # other duplicate scenario: EP player links with multiple URL alternatives
    ep_players['link_id'] = ep_players['link'].apply(get_id_from_ep_url)
    ep_player_links = ep_players[['link', 'link_id']].drop_duplicates() 
    linkId_counts = ep_player_links.link_id.value_counts() # if a link_id has multiple distinct mapped links, then the player has multiple urls that have not been unified
    dupe_linkId_rows = ep_player_links[ep_player_links.link_id.isin(linkId_counts.index[linkId_counts.gt(1)])]
    dupe_linkId_rows.sort_values(by='link_id',inplace=True)
    with open(os.path.join(data_dir, f"duplicates_{today_str}.txt"), 'w') as out_file:
        out_file.write("GROUP 1\n")
        for norm_name, name, link in output_duplicates:
            if str(link) not in disambiguated_links:
                out_file.write(f"{name},{norm_name},{link}\n")
        out_file.write("GROUP 2\n")
        for row in dupe_pId_rows.itertuples():
            out_file.write(f"{row.playerId},{row.link},{row.playerName}\n")
        out_file.write("GROUP 3\n")
        for row in dupe_link_rows.itertuples():
            out_file.write(f"{row.playerId},{row.link},{row.playerName}\n")
        out_file.write("GROUP 4\n")
        for row in dupe_linkId_rows.itertuples():
            out_file.write(f"{row.link},{row.link_id}\n")

def get_id_from_ep_url(ep_url):
    # remove trailing slash
    if ep_url[-1]=='/':
        ep_url = ep_url[:-1]
    # find id
    tmp = ep_url.split('/')
    id = tmp[-2]
    return id

def build_name_db(engine): 
    normalized_names = pd.DataFrame(columns=['norm_name', 'canon_name'])
    nhl_db_players = pd.read_sql_query(sql=sql_text("select * from players_nhl"), con=engine.connect()) # TODO eventually this should be an operation in the database?
    nhl_db_players['playerId'] = nhl_db_players['playerId'].astype(int)
    # initialize canon names with NHL names
    name_links = nhl_db_players[['playerId', 'playerName']]
    name_links['playerId'] = name_links['playerId'].astype(int)
    name_links = name_links.drop_duplicates() # because of the multiple terms per player
    # read disambiguated names from file, replace playerName with canon_name in this case
    disamb_names = pd.read_csv(os.path.join(data_dir, "name_id_links.csv"))
    name_links = name_links.merge(disamb_names, how='left', on='playerId')
    name_links["canon_name"].fillna(inplace=True, value=name_links["playerName"])
    name_links = name_links.drop(columns=['playerName'])
    name_links = name_links.rename(columns={"canon_name": "playerName"})
    # normalize names (NHL data)
    normalized_names = name_links[['playerName']].copy()
    normalized_names['norm_name'] = normalized_names['playerName'].apply(normalize_name)
    # now get names from EP 
    ep_transfers = pd.read_sql(sql=sql_text('select * from ep_clean_transfers'), con=engine.connect())
    ep_asg = pd.read_sql(sql=sql_text('select * from ep_clean_asg'), con=engine.connect())
    ep_worlds = pd.read_sql(sql=sql_text('select * from ep_clean_intl'), con=engine.connect())
    ep_players = pd.concat([ep_transfers[['link', 'player']], ep_asg[['link', 'player']], ep_worlds[['link', 'player']]])
    ep_names_to_add = []
    for _, player_row in ep_players.iterrows(): # TODO can I avoid iterating here?
        player_ep_link = player_row['link']
        player_ep_name = player_row['player']
        # check if link already accounted for
        if len(name_links.loc[name_links['link'] == player_ep_link]) > 0:
            continue
        # check for match with normalized name form
        norm_ep_name = normalize_name(player_ep_name)
        norm_name_row = normalized_names.loc[normalized_names['norm_name']==norm_ep_name]
        if len(norm_name_row) == 1:
            player_name_tgt = norm_name_row.iloc[0]['playerName']
            name_links.loc[name_links['playerName']==player_name_tgt, 'link']=player_ep_link
        else: # otherwise, append this unmatched EP link and name to name_links
            ep_names_to_add.append([player_ep_link, player_ep_name, ''])
    # add ep names
    name_links = pd.concat([name_links, pd.DataFrame(ep_names_to_add, columns=['link', 'playerName', 'playerId'])])
    # normalize names (NHL + EP data)
    normalized_names = name_links[['playerName']].copy()
    normalized_names['norm_name'] = normalized_names['playerName'].apply(normalize_name)
    # add first/last names to link table (split normalized names on whitespace)
    new_names = []
    for _, row in normalized_names.iterrows():
        name_parts = get_name_parts(row['norm_name'])
        for part in name_parts:
            new_names.append([part, row['playerName']])
    normalized_names = pd.concat([normalized_names,pd.DataFrame(new_names, columns=['norm_name', 'playerName'])])
    with open(os.path.join(data_dir, f"ep_link_only.txt"), 'w') as ep_out_file:
        ep_out_tuples = []
        for _, row in name_links.loc[name_links['playerId']==""].iterrows():
            ep_out_tuples.append((row['link'], row['playerName']))
        ep_out_tuples = sorted(ep_out_tuples, key=lambda x: x[1])
        for ep_link, canon_name in ep_out_tuples:
            ep_out_file.write(f"{ep_link},{canon_name}\n")
    # get canon name lengths
    canon_names = pd.DataFrame(normalized_names['playerName'].unique(), columns=['playerName'])
    canon_names['name_length'] = canon_names.apply(lambda x: get_string_width(x['playerName']), axis=1)
    # save to db
    normalized_names.drop_duplicates(inplace=True, ignore_index=True)
    normalized_names.to_sql('norm_names', engine, if_exists='replace', index=False)
    canon_names.to_sql('player_names', engine, if_exists='replace', index=False)
    name_links.drop_duplicates(inplace=True, ignore_index=True)
    name_links.to_sql('links', engine, if_exists='replace', index=False)
    return name_links

def remove_scratches(engine):
    scratches_df = pd.read_sql(sql=sql_text("select * from scratches"), con=engine.connect())
    game_player_df = pd.read_sql(sql=sql_text("select * from game_player"), con=engine.connect())
    game_player_scratch = game_player_df.merge(scratches_df, how='left', on=['gameId', 'playerId'], indicator=True)
    mask = game_player_scratch['_merge'] == 'both'
    game_player_no_scratch = game_player_scratch[~mask]
    game_player_no_scratch = game_player_no_scratch.drop(['_merge'], axis=1)
    # write amended game-player table to database
    game_player_no_scratch.to_sql(name="game_player", if_exists='replace', con=engine, index=False)

def get_name_parts(name):
    name_parts = name.split()
    all_name_parts = []
    for i in range(0, len(name_parts)):
        name_part = name_parts[i]
        if name_part != name:
            all_name_parts.append(name_part)
        for j in range(i+1, len(name_parts)):
            name_part += " " + name_parts[j]
            if name_part != name:
                all_name_parts.append(name_part)
    return all_name_parts

def get_string_width(input_string):
    string_no_accents = unidecode(input_string)
    width = font.getlength(string_no_accents)
    return width

def preprocessing(engine):
    # TODO temp read data
    nhl_players_df = pd.read_sql_query(sql=sql_text("select * from players_nhl"), con=engine.connect())
    # fix NHL data errors
    with open(os.path.join(data_dir, "nhl_errors.txt")) as in_file: 
        reader = csv.DictReader(in_file)
        for row in reader:
            nhl_players_df.loc[(nhl_players_df['playerId'] == row['playerId']) & (nhl_players_df[row['field']]== row['incorrect_value']), row['field']] = row['value']
    # fix EP link errors
    with open(os.path.join(data_dir, "ep_link_corrections.txt")) as in_file: 
        worlds_df = pd.read_sql(sql=sql_text('select * from ep_raw_intl'), con=engine.connect())
        transfers_df = pd.read_sql_query(sql=sql_text("select * from ep_raw_transfers"), con=engine.connect(), parse_dates=['date'])
        asg_df = pd.read_sql(sql=sql_text('select * from ep_raw_asg'), con=engine.connect())
        reader = csv.DictReader(in_file)
        for row in reader:
            worlds_df.loc[worlds_df['link'] == row['incorrect_link'], 'link'] = row['correct_link']
            asg_df.loc[asg_df['link'] == row['incorrect_link'], 'link'] = row['correct_link']
            transfers_df.loc[transfers_df['link'] == row['incorrect_link'], 'link'] = row['correct_link']   
        transfers_df.to_sql('ep_clean_transfers', engine, index=False, if_exists='replace')
        worlds_df.to_sql('ep_clean_intl', engine, index=False, if_exists='replace')
        asg_df.to_sql('ep_clean_asg', engine, index=False, if_exists='replace')
    # normalize team names
    nhl_players_df['team'] = nhl_players_df['team'].str.split().str.join(' ') # need to normalize whitespace before we do dictionary-based name replacement
    team_mapping = {}
    with open(os.path.join(data_dir, "team_name_normalization.json")) as in_file:
        team_mapping_tmp = json.load(in_file)
        for target_name in team_mapping_tmp:
            for source_name_list in team_mapping_tmp[target_name]:
                if len(source_name_list) > 2:
                    continue # TODO don't handle this more complicated replacement case yet
                elif len(source_name_list) == 2: # team name and league
                    tup = (source_name_list[0], source_name_list[1])
                elif len(source_name_list) == 1:
                    tup = (source_name_list[0])
                if tup in team_mapping_tmp:
                    # TODO remove duplicates for now
                    team_mapping.pop(tup, None)
                else:
                    team_mapping[tup] = target_name
        # now replace
        for source_team_list in team_mapping:
            target_team = team_mapping[source_team_list]
            if type(source_team_list) == tuple and len(source_team_list) == 2:
                source_team, source_league = source_team_list
                nhl_players_df.loc[(nhl_players_df['league']==source_league) & (nhl_players_df['team']==source_team), 'team'] = target_team
            else:
                source_team = source_team_list
                nhl_players_df.loc[nhl_players_df['team']==source_team, 'team'] = target_team
    # specific USNTDP normalization
    # drop USHL
    nhl_players_df = nhl_players_df.loc[~((nhl_players_df['league']=='USHL') & (nhl_players_df['team']=='USNTDP'))]
    nhl_players_df = nhl_players_df.loc[~((nhl_players_df['league']=='USHL') & (nhl_players_df['team']=='USNTDP Juniors'))]
    nhl_players_df = nhl_players_df.loc[~((nhl_players_df['league']=='USHL') & (nhl_players_df['team']=='USAHNTDP'))]
    
    # fix U-17
    # u17_mask = (nhl_players_df['league']=='U-17') & (nhl_players_df['team']=='USNTDP')
    # nhl_players_df['league'] = nhl_players_df['league'].mask(u17_mask, 'USHL')
    # nhl_players_df['team'] = nhl_players_df['team'].mask(u17_mask, 'USNTDP U-17')
    
    
    # nhl_players_df['league'] = nhl_players_df['league'].mask(mask, 'USHL')
    # print(nhl_players_df)
    # mask = df['A'].isin([1, 3]) & df['B'].isin([4, 6])

    # nhl_players_df['team'] = new_team
    # nhl_players_df['league'] = new_league
    # print(nhl_players_df.loc[nhl_players_df['playerName']=='Matthew Tkachuk'])
    # fix U-18
    # u18_mask = (nhl_players_df['league']=='U-18') & (nhl_players_df['team']=='USNTDP')
    # nhl_players_df['team'] = nhl_players_df['team'].mask(u18_mask, 'USNTDP U-18')
    # nhl_players_df['league'] = nhl_players_df['league'].mask(u18_mask, 'USHL')
    # nhl_players_df['team'] = new_team
    # nhl_players_df['league'] = new_league
    # write the correction
    nhl_players_df.to_sql(name="players_nhl", if_exists='replace', index=False, con=engine)
    postseasons = pd.read_sql_query(sql=sql_text("select * from ep_raw_postseasons"), con=engine.connect())
    # TODO drop names from the EP/NHL data tables?
    # TODO should this name step happen after we drop asg players we don't have ids for?
    # STEP FOUR: unify names (will want to repeat this step based on the outputs of the missing nhl/ep links)
    # name_db depends on preprocessing of transfers, nhl_players, what else???
    build_name_db(engine)#, players_df, transfers, asg_terms)
    # all "join links" should happen after the name db is constructed
    transfers_df = pd.read_sql_query(sql=sql_text("select * from ep_clean_transfers join links using (link)"), con=engine.connect(), parse_dates=['date'])
    transfers_df = transfers_df[transfers_df['playerId'] != ''] # drop transfer terms for players we don't have matching NHL site playerIds for
    # normalization of transfer team names
    for source_team_list in team_mapping:
        target_team = team_mapping[source_team_list]
        if type(source_team_list) == str: 
            transfers_df.loc[transfers_df['from_team']==source_team_list, 'from_team'] = target_team
            transfers_df.loc[transfers_df['to_team']==source_team_list, 'to_team'] = target_team
    # normalize whitespace
    transfers_df['to_team'] = transfers_df['to_team'].str.split().str.join(" ")
    transfers_df['from_team'] = transfers_df['from_team'].str.split().str.join(" ")
    transfers_df.to_sql('ep_clean_transfers', engine, index=False, if_exists='replace')
    asg_df = pd.read_sql(sql=sql_text('select * from ep_clean_asg join links using (link)'), con=engine.connect())
    asg_df = asg_df[asg_df['playerId'] != ''] # drop ASG terms for players we don't have matching NHL site playerIds for
    asg_df = asg_df.drop(['index', 'level_0'], axis=1, errors='ignore')
    worlds_df = pd.read_sql(sql=sql_text('select * from ep_clean_intl join links using (link)'), con=engine.connect())
    worlds_df = worlds_df[worlds_df['playerId'] != ''] # drop Worlds terms for players we don't have matching NHL site playerIds for
    players_df = pd.concat([worlds_df, nhl_players_df, asg_df], axis=0, ignore_index=True) # add intl roster data to skaters table
    # drop superfluous Worlds and M-Cup rows
    players_df = players_df[~players_df['league'].isin(nhl_leagues_to_drop)]
    # now drop non-national teams that appear more than once in a season
    players_natl = players_df.copy()
    players_natl = players_natl.loc[players_natl['team'].apply(is_national_team)]
    print(players_natl)
    players_not_natl = players_df.copy()
    players_not_natl = players_not_natl.loc[~players_not_natl['team'].apply(is_national_team)]
    print(players_not_natl)
    players_not_natl = players_not_natl.drop_duplicates(subset=['playerId', 'playerName', 'season', 'team']) # to drop semi-redundant playoff rows (e.g. Connor McDavid Marlboros, but also the same row where Marlboros are in league "Other")
    players_df = pd.concat([players_natl, players_not_natl])
    # players_df = players_df.drop_duplicates(subset=['playerId', 'playerName', 'season', 'team']) # to drop semi-redundant playoff rows (e.g. Connor McDavid Marlboros, but also the same row where Marlboros are in league "Other")
    return players_df, nhl_players_df, transfers_df, asg_df, worlds_df, postseasons # TODO can't processing_players just read from db instead of passing these dfs?

def is_national_team(team_name):
    country_names = ["Canada", "Finland", "Finland", "Sweden", "Czechia", "Czech Republic", "USA", "Denmark", "Austria", "Great Britain", "Norway", "Switzerland", "France", "Germany", "Kazakhstan", "Latvia", "Poland", "Slovakia", "Czechoslovakia", "Russian", "Soviet Union", "Belarus", "Ukraine", "Italy", "Slovenia", "Hungary"]
    for country_name in country_names:
        if country_name in team_name:
            return True
    return False


def match_transfer_teams(transfers_df, nhl_players_df):
    # get all teams from EP and NHL sites
    transfer_teams = pd.DataFrame(list(set(transfers_df['from_team'].tolist() + transfers_df['to_team'].tolist())),columns=['ep_team_name'])
    print(f"{len(transfer_teams)} transfer teams")
    roster_teams = pd.DataFrame(list(set(nhl_players_df['team'].tolist())), columns=['roster_team'])
    print(f"{len(roster_teams)} roster teams")
    # replace with established matches
    team_matches = pd.read_csv(os.path.join(data_dir, "nhldotcom_norm_team_names.csv"))
    transfer_teams = pd.merge(transfer_teams, team_matches, how='left', on=['ep_team_name'])
    # 1: match on specified team name mapping from csv
    join_teams = pd.merge(transfer_teams, roster_teams, how='left', left_on=['nhl_team_name'], right_on=['roster_team'])
    # 2: match on exact string match between the two sites
    join_teams = pd.merge(join_teams, roster_teams, how='left', left_on=['ep_team_name'], right_on=['roster_team'])
    join_teams.to_csv(os.path.join(data_dir, 'transfer_matched_team_names.csv'))
    # join_teams = join_teams.loc[(join_teams['roster_team_x']=='') & (join_teams['roster_team_y']=='')]
    join_teams = join_teams.loc[(join_teams['roster_team_x'].isna()) & (join_teams['roster_team_y'].isna())]
    from_teams = pd.merge(join_teams, transfers_df, how='left', left_on=['ep_team_name'], right_on=['from_team'])
    from_teams.to_csv(os.path.join(data_dir, 'transfer_from_unmatched_team_names.csv'))
    to_teams = pd.merge(join_teams, transfers_df, how='left', left_on=['ep_team_name'], right_on=['to_team'])
    to_teams.to_csv(os.path.join(data_dir, 'transfer_to_unmatched_team_names.csv'))



if __name__ == "__main__":
    start = time.time()
    out_db = os.path.join(db_dir, f"{today_str}.db")
    engine = create_engine(f"sqlite:///{out_db}")
    print(out_db)
    if len(sys.argv) == 1:
        print("pick an option: scrape, process, names_only")
    elif sys.argv[1] == "--scrape_playoffs":
        postseasons = scraper.get_postseasons()
        postseasons.to_csv(os.path.join(data_dir, "ep_raw_postseasons.csv"))
    elif len(sys.argv) < 2 or sys.argv[1] == "--scrape":
        print(f"Scraping seasons: {seasons_to_scrape} up to {today}")
        # copy old db to new db (removing the seasons that will be scraped fresh)
        if config['prev_file_date']:
            prev_db = os.path.join(db_dir, f"{config['prev_file_date']}.db")
            print(f"Previous database: {prev_db}")
            prev_engine = create_engine(f"sqlite:///{prev_db}")
            old_ep_raw_intl = copy_db(seasons_to_scrape, prev_engine, engine) # have to keep old_players_df around to drop duplicates before I write to db again, because I can't filter using any date
        else:
            old_ep_raw_intl = None
        # scrape NHL website game data
        game_urls, teams_info, teams_by_season = get_game_urls(seasons_to_scrape)
        # scrape NHL website roster data
        scrape_nhl_skaters(engine, teams_by_season)#, seasons_to_scrape)# old_players_df)
        # scrape player stats for all games, write to db
        scrape_game_pages(game_urls, teams_info, engine)
        end = time.time()
        print(f"elapsed time: {timedelta(seconds=end - start)}")
        # scrape EP website for transfers & roster data
        scrape_ep(engine, old_ep_raw_intl)
        end = time.time()
        print(f"elapsed time: {timedelta(seconds=end - start)}")
    if sys.argv[1] == '--process':
        print("Processing db")
        players_df, nhl_players_df, transfers_df, asg_df, worlds_df, postseasons = preprocessing(engine)
        # output unmatched EP transfer teams
        # match_transfer_teams(transfers_df, nhl_players_df)
        # sys.exit(0)
        # output candidate duplicate names
        find_duplicates(nhl_players_df, transfers_df, asg_df, worlds_df) 
        
        
        # process player terms
        player_terms = process_player_terms(players_df, transfers_df, postseasons)
        end = time.time()
        print(f"elapsed time: {timedelta(seconds=end - start)}")

    elif sys.argv[1] == '--test':
        playerId=sys.argv[2]
        # players_df = pd.read_sql_query(sql=sql_text("select * from players_nhl"), con=engine.connect())
        # players_df = players_df[~players_df['league'].isin(nhl_leagues_to_drop)]
        # players_df = players_df.drop_duplicates(subset=['playerId', 'playerName', 'season', 'team']) # to drop semi-redundant playoff rows (e.g. Connor McDavid Marlboros, but also the same row where Marlboros are in league "Other")
        # transfers = pd.read_sql_query(sql=sql_text("select * from ep_raw_transfers join links using (link)"), con=engine.connect(), parse_dates=['date'])
        # postseasons = pd.read_sql_query(sql=sql_text("select * from ep_raw_postseasons"), con=engine.connect())
        players_df, nhl_players_df, transfers_df, asg_df, worlds_df, postseasons = preprocessing(engine)
        player_terms = process_player_terms(players_df, transfers_df, postseasons, test_id=playerId)