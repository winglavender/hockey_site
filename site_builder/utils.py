import re
from unidecode import unidecode

def get_player_term_tooltip_str(team_display_str, years_str):
    return f"{team_display_str}<br>{years_str}"

def get_js_date_values(date):
    return date.year, date.month-1, date.day

def get_js_date_values_playoffs(year):
    return year, 4, 1, year, 4, 30 

def get_tournament_leagues(): # this is a function so we can call it from the side code teammates_db.py
    # first value is month and second value is 0 if the first year in a season should be used, 1 if the second year in the season should be used
    tournament_leagues = {'4 Nations': (2, 1), 'Hlinka Gretzky Cup': (8, 0), 'WJAC-19': (12, 0), 'OGQ': (9, 0), 'Oly-Q': (9, 0), 'Olympics': (2,1), 'wjc-20': (1,1), 'wc': (6,1), 'wjc-18': (4,1), 'WJC-18': (4,1), 'WJC-18 D1A': (4,1), 'W-Cup': (9,0), 'WCup': (9,0), 'nhl-asg': (2,1), 'whc-17': (11,0), 'U17-Dev': (11,0), 'U18-Dev': (11,0)} 
    return tournament_leagues

# TODO need to fix this for tournament dates (which dates, which tournaments, all of them?)
def get_years_str(league, start_date, end_date):
    years_str = f"{start_date.year}-{end_date.year}"
    tournament_leagues = get_tournament_leagues()
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

def normalize_name(name):
    name = name.lower()
    name = re.sub(r'\([^()]*\)', '', name)
    name = name.strip()
    name = unidecode(name) 
    name = name.replace('"', "") # remove quotes that will break the query
    name = name.replace("-", " ") # handle hyphenated names
    name = name.replace(".", "") # remove periods from "J.T. Brown" but we don't want to remove all punctuation e.g. "O'Connor"
    return name