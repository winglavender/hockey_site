import pandas as pd
import sqlite3 as sql
import unicodedata
import time
import re
import matplotlib as mpl
from matplotlib.afm import AFM
import os.path
from season_calculator import SeasonCalculator
from datetime import date
import numpy as np
import csv


class ep_db():

    def __init__(self, name_db, config):
        self.name_db = name_db
        self.config = config
        db_name = f"{self.config['root_dir']}/hockey_db_data/hockey_rosters_{config['filename_date']}_formatted.db"
        self.latest_date = pd.to_datetime(config['timeline_end']) # set to end of current season?
        self.conn = sql.connect(db_name)
        self.skaters = pd.read_sql_query('select * from skaters', self.conn)
        self.teammates = pd.read_sql_query('select * from teammates', self.conn)
        self.postseasons = pd.read_sql_query('select * from postseasons', self.conn)
        self.skaters.start_date = pd.to_datetime(self.skaters.start_date)
        self.skaters.end_date = pd.to_datetime(self.skaters.end_date)
        # self.names = pd.read_sql_query('select * from names', conn)
        self.league_strings = {'nhl': 'NHL', 'og': 'Olympics', 'khl': 'KHL', 'ahl': 'AHL', 'wc': 'Worlds', 'ohl': 'OHL', 'whl': 'WHL', 'qmjhl': 'QMJHL', 'ushl': 'USHL', 'usdp': 'USDP', 'ncaa': 'NCAA', 'wjc-20': 'World Juniors', 'wjc-18': 'WC-U18', 'whc-17': 'WHC-17', 'wcup': 'World Cup', 'shl': 'SHL', 'elitserien': 'Elitserien', 'mhl': 'MHL', 'liiga': 'Liiga', 'u20-sm-liiga': 'U20 SM Liiga', 'u18-sm-sarja': 'U18 SM Sarja', 'j20-superelit': 'J20 SuperElit', 'j18-allsvenskan': 'J18 Allsvenskan', 'russia': 'Russia', 'russia3': 'Russia3', 'ushs-prep': 'USHS Prep', 'nhl-asg': 'NHL ASG'}
        self.tournament_leagues = {'og': (2,1), 'wjc-20': (1,1), 'wc': (6,1), 'wjc-18': (4,1), 'whc-17': (11,0), 'wcup': (9,0), 'nhl-asg': (2,1)} # first value is month and second value is 0 if the first year in a season should be used, 1 if the second year in the season should be used
        # for font-accurate string comparisons 
        afm_filename = os.path.join(mpl.get_data_path(), 'fonts', 'afm', 'ptmr8a.afm')
        self.afm = AFM(open(afm_filename, "rb"))
        self.season_calc = SeasonCalculator(date.today(), config)
        self.correct_links()

    def correct_links(self):
        with open(self.config['root_dir'] + '/hockey_db_data/ep_link_corrections.txt') as in_file:
            reader = csv.DictReader(in_file)
            for row in reader:
                self.skaters.loc[self.skaters['link'] == row['incorrect_link'], 'link'] = row['correct_link']

    def get_string_width(self, input_string):
        width, height = self.afm.string_width_height(input_string)
        return width

    def categorize_league_list(self, league_list):
        contains_non_national = False
        national_set = {'World Juniors', 'Worlds', 'Olympics', 'WC-U18', 'WHC-17', 'World Cup'}
        if len(league_list) == 1 and league_list[0] == "NHL ASG":
            return 'yellow'
        for league in league_list:
            if league == 'NHL':
                return 'blue' # NHL
            elif league not in national_set:
                contains_non_national = True
        if contains_non_national:
            return 'green' # other
        else:
            return 'red' # national teams only

    def get_league_display_string(self, league_str):
        if league_str not in self.league_strings:
            return league_str
        return self.league_strings[league_str]

    def get_team_display_string(self, league_str, team_str):
        if league_str == 'og':
            return f"{team_str} Olympics"
        elif league_str == 'wc':
            return f"{team_str} Worlds"
        elif league_str == 'wcup':
            return f"{team_str} World Cup"
        elif league_str == 'wjc-20':
            return f"{team_str} World Juniors"
        elif league_str == 'wjc-18':
            return f"{team_str} Worlds"
        elif league_str == 'whc-17':
            return f"{team_str}"
        else:
            return team_str

    def is_tournament(self, league_str):
        if league_str in self.tournament_leagues:
            return True
        return False

    def convert_tournament_dates(self, tournament, start_date, end_date):
        season_years = (start_date.year, end_date.year)
        season_years_str = f"{start_date.year}-{end_date.year}"
        # catch delayed season start exceptions
        if season_years_str == "2013-2013":
            season_years = (2012,2013)
        elif season_years_str == "2021-2021":
            season_years = (2020, 2021)
        tourney_month, tourney_year = self.tournament_leagues[tournament]
        return (pd.to_datetime(f"{season_years[tourney_year]}/{tourney_month}/1"), pd.to_datetime(f"{season_years[tourney_year]}/{tourney_month}/28"))

    # returns tuple (start_date, end_date) that defines the overlapping interval,
    # the number of seasons in the interval,
    # and True/False whether the overlap is invalid (occurs within a single offseason)
    def compute_overlap_interval(self, start_date1, end_date1, start_date2, end_date2):
        start_dates = pd.DataFrame([pd.to_datetime(start_date1), pd.to_datetime(start_date2)])
        end_dates = pd.DataFrame([pd.to_datetime(end_date1), pd.to_datetime(end_date2)])
        start_interval = start_dates.max().iloc[0]
        end_interval = end_dates.min().iloc[0]
        num_seasons = max(1, end_interval.year-start_interval.year)
        if start_interval <= end_interval:
            # check if overlap is valid or not: invalid overlap has start and end dates in the same offseason
            season1, in_season1 = self.season_calc.get_season_from_date(start_interval)
            season2, in_season2 = self.season_calc.get_season_from_date(end_interval)
            valid = True
            if not in_season1 and not in_season2 and season1 == season2:
                valid = False
            return (start_interval, end_interval), num_seasons, valid
        else:
            return False, 0, False

    def get_terms_from_player_id(self, player_id, latest_date=None):
        # use global latest date if none specified
        if not latest_date:
            latest_date = self.latest_date
        rows = self.skaters.loc[self.skaters.link==player_id]
        filtered_rows = []
        for index, row in rows.iterrows():
            if row.team == 'USNTDP Juniors':
                continue # USHL is weird and this team encompasses both U17 and U18 teams so it's redundant/too coarse
            if row.start_date > latest_date: # term starts after our latest date
                continue
            if row.end_date > latest_date:
                row.end_date = latest_date
            filtered_rows.append(row)
        return filtered_rows
            
    # build player's individual timeline (the top row) including NHL playoff info
    def get_player_career(self, player_id, latest_date=None):
        # use global latest date if none specified
        if not latest_date:
            latest_date = self.latest_date
        output = []
        player_rows = self.get_terms_from_player_id(player_id, latest_date)
        playername = self.get_player_name_from_id(player_id)
        playoff_queries = []
        #for index, term in player_rows.iterrows():
        for term in player_rows:
            is_tourney = self.is_tournament(term.league)
            team_display_str = self.get_team_display_string(term.league, term.team)
            years_str = f"{term.start_date.year}-{term.end_date.year}"
            if is_tourney and years_str in ["2013-2013","2021-2021"]:
                if years_str == "2013-2013":
                    years_str = "2012-2013"
                elif years_str == "2021-2021":
                    years_str = "2020-2021"
            tooltip_str = f"{team_display_str}<br>{years_str}"
            if is_tourney:
                term_dates = self.convert_tournament_dates(term.league, term.start_date, term.end_date)
                team_display_str = ""
                color = "80b4f2" # tournament color
            else:
                term_dates = (term.start_date, term.end_date)
                color = "2e7cdb" # non-tournament team color
            output.append({"player": playername, "league": term.league, "team_display": team_display_str, "year1": term_dates[0].year, "month1": term_dates[0].month, "day1": term_dates[0].day, "year2": term_dates[1].year, "month2": term_dates[1].month, "day2": term_dates[1].day, "tooltip_str": tooltip_str, "id": player_id, "color": color}) 
            # check for playoff query
            if term.league == "nhl":
                possible_first_season = f"{term.start_date.year-1}-{term.start_date.year}"
                first_season_dates = self.season_calc.get_season_dates(possible_first_season)
                playoffs_end_date = first_season_dates[1]
                if term.start_date < playoffs_end_date and term.end_date >= playoffs_end_date: # check whether the player was on the team during playoffs in the first possible season of their tenure (approximation)
                    playoff_queries.append((term.team, possible_first_season))
                if term.start_date.year != term.end_date.year: # check following seasons 
                    for i in range(term.start_date.year, term.end_date.year-1): # for all seasons in the middle of this tenure, player was definition on the roster during playoffs
                        playoff_queries.append((term.team, f"{i}-{i+1}"))
                    possible_last_season = f"{term.end_date.year-1}-{term.end_date.year}"
                    last_season_dates = self.season_calc.get_season_dates(possible_last_season)
                    playoffs_end_date = last_season_dates[1]
                    if term.start_date < playoffs_end_date and term.end_date >= playoffs_end_date: # check whether the player was on the team during playoffs in the last possible season
                        playoff_queries.append((term.team, f"{term.end_date.year-1}-{term.end_date.year}"))
        # get playoff runs
        for team, playoff_year in playoff_queries:
            team_str = self.strip_accents(team)
            year = playoff_year.split("-")[1]
            result = self.postseasons.loc[
                (self.postseasons.Team == team_str)]
            result = self.postseasons.loc[(self.postseasons.Team==team_str) & (self.postseasons.Season==playoff_year)].Postseason
            if len(result) == 0:
                continue
            result = result.item()
            tooltip_str = f"{playoff_year}<br>{result}"
            if result == "Did not make playoffs":
                color = "#cfcecc" # missed playoffs color
            elif result == "Champion":
                color = "fae525" # won Cup color
            else:
                color = "ebb11e" # made playoffs but didn't win Cup color
            output.append({"player": "", "league": "nhl_playoffs", "team_display": "", "year1": year, "month1": 5, "day1": 1, "year2": year, "month2": 5, "day2": 30, "tooltip_str": tooltip_str, "id": player_id, "color": color})
        return output

    def get_overlapping_player_terms_teammate(self, player1_id, player2_id=None):
        start = time.time()
        if player2_id: 
            overlaps = self.teammates.loc[(self.teammates.link_x == player1_id) & (self.teammates.link_y == player2_id)]
        else:
            overlaps = self.teammates.loc[self.teammates.link_x==player1_id]
        end = time.time()
        print(f"elapsed time: {end-start}")
        overlaps['overlap_start_date'] = overlaps[['start_date_x','start_date_y']].max(axis=1)
        overlaps['overlap_end_date'] = overlaps[['end_date_x','end_date_y']].min(axis=1)
        overlaps['overlap_start_year_js'] = pd.DatetimeIndex(overlaps['overlap_start_date']).year
        overlaps['overlap_start_month_js'] = pd.DatetimeIndex(overlaps['overlap_start_date']).month-1
        overlaps['overlap_start_day_js'] = pd.DatetimeIndex(overlaps['overlap_start_date']).day
        overlaps['overlap_end_year_js'] = pd.DatetimeIndex(overlaps['overlap_end_date']).year
        overlaps['overlap_end_month_js'] = pd.DatetimeIndex(overlaps['overlap_end_date']).month-1
        overlaps['overlap_end_day_js'] = pd.DatetimeIndex(overlaps['overlap_end_date']).day
        end = time.time()
        print(f"elapsed time: {end-start}")
        # get teammate names from name db -- TODO eventually do this as a preprocessing step too (join)
        # TODO THIS is the slow step -- when i make everything one unified db then this player name will be returned above and it won't take a whole second to get all the canon names
        overlaps['player_name_y'] = overlaps.apply(lambda x: self.name_db.get_name(x.link_y, "ep"), axis=1) # TODO this will be join eventually
        overlaps['player_name_y_len'] = overlaps.apply(lambda x: self.name_db.get_name_len(x.link_y, "ep"), axis=1) # TODO turn into join eventually
        # print(overlaps)
        end = time.time()
        print(f"elapsed time: {end-start}")
        overlaps['tooltip_str'] = overlaps.apply(lambda x: self.get_tooltip_str(x.player_name_y, x.team_display_str, x.years_str, x.league, x.num_seasons), axis=1) 
        end = time.time()
        print(f"elapsed time: {end-start}")
        # sort orders
        overlaps.sort_values(by=['overlap_start_date', 'player_name_y'], inplace=True)
        overlaps_sort_date_no_asg = overlaps.loc[overlaps['league']!='nhl-asg']
        # get max name length 
        idx = overlaps['player_name_y_len'].idxmax()
        # print(idx)
        # print(overlaps.shape)
        print(overlaps.loc[idx])
        max_name = overlaps.loc[idx]['player_name_y'] # with asg
        idx = overlaps_sort_date_no_asg['player_name_y_len'].idxmax()
        print(overlaps_sort_date_no_asg.loc[idx])
        max_name_no_asg = overlaps_sort_date_no_asg.loc[idx]['player_name_y'] # without asg
        # finish sorting
        overlaps_sort_date = overlaps.to_dict('records')
        overlaps_sort_date_no_asg = overlaps_sort_date_no_asg.to_dict('records') # no asg
        overlaps.sort_values(by=['overlap_len', 'player_name_y'], inplace=True, ascending=False)
        overlaps_sort_len = overlaps.to_dict('records')
        overlaps_sort_len_no_asg = overlaps.loc[overlaps['league']!='nhl-asg'].to_dict('records') # no asg

        end = time.time()
        print(f"elapsed time: {end-start}")
        # TODO longest name - get from name DB when I rerun name processing
        end = time.time()
        print(f"elapsed time: {end-start}")
        return overlaps_sort_date, max_name, max_name_no_asg, overlaps_sort_date_no_asg, overlaps_sort_len, overlaps_sort_len_no_asg
        # sorted_output, longest_name, output_no_asg, sorted_output_overlap_term, sorted_output_overlap_term_no_asg

    def get_tooltip_str(self, teammate_name, team_display_str, years_str, league, num_seasons):
        tooltip_str = f"{teammate_name}<br>{team_display_str}<br>{years_str}"
        if league not in self.tournament_leagues:
            if num_seasons == 1:
                tooltip_str += f" ({num_seasons} season)"
            else:
                tooltip_str += f" ({num_seasons} seasons)"
        return tooltip_str
    
    def get_overlapping_player_terms(self, player1_id, player2_id=None):
        output = []
        player_rows = self.get_terms_from_player_id(player1_id)
        teammate_length = {}
        #for index, term in player_rows.iterrows():
        for term in player_rows:
            if player2_id: # only return overlaps involving this specific second target player
                overlaps_a = self.skaters.loc[(self.skaters.league==term.league) & (self.skaters.team==term.team) & (self.skaters.link==player2_id) & (self.skaters.start_date>=term.start_date) & (self.skaters.start_date<term.end_date)]
                overlaps_b = self.skaters.loc[(self.skaters.league==term.league) & (self.skaters.team==term.team) & (self.skaters.link==player2_id) & (term.start_date>self.skaters.start_date) & (term.start_date<self.skaters.end_date)]
            else: # return overlaps involving any other player
                overlaps_a = self.skaters.loc[(self.skaters.link!=player1_id) & (self.skaters.league==term.league) & (self.skaters.team==term.team) & (self.skaters.start_date>=term.start_date) & (self.skaters.start_date<term.end_date)]
                overlaps_b = self.skaters.loc[(self.skaters.link!=player1_id) & (self.skaters.league==term.league) & (self.skaters.team==term.team) & (term.start_date>self.skaters.start_date) & (term.start_date<self.skaters.end_date)]
            overlaps = pd.concat([overlaps_a, overlaps_b])
            team_display_str = self.get_team_display_string(term.league, term.team)
            for teammate_id in overlaps.link.unique():
                teammate_name = self.get_player_name_from_id(teammate_id)
                teammate_rows = overlaps.loc[overlaps.link==teammate_id] # get all overlapping terms for this teammate (same as overlaps if player2_id was specified)
                for index, teammate_term in teammate_rows.iterrows():
                    overlap_term, season_count, is_valid_overlap = self.compute_overlap_interval(term.start_date, term.end_date, teammate_term.start_date, teammate_term.end_date)
                    is_tourney = self.is_tournament(term.league)
                    if not is_tourney and not is_valid_overlap:
                        # skip invalid overlap intervals
                        continue
                    years_str = f"{overlap_term[0].year}-{overlap_term[1].year}" # this ensures tournament still has (year1-year2) even though the term is only across a single year
                    # fix tournament dates in years where the season started late
                    if is_tourney and years_str in ["2013-2013","2021-2021"]:
                        if years_str == "2013-2013":
                            years_str = "2012-2013"
                        elif years_str == "2021-2021":
                            years_str = "2020-2021"
                    tooltip_str = f"{teammate_name}<br>{team_display_str}<br>{years_str}"#{overlap_term[0].year}-{overlap_term[1].year}"
                    if is_tourney:
                        overlap_term = self.convert_tournament_dates(term.league, overlap_term[0], overlap_term[1])
                    else:
                        if season_count == 1:
                            tooltip_str += f" ({season_count} season)"
                        else:
                            tooltip_str += f" ({season_count} seasons)"
                    output.append((overlap_term[0].year, overlap_term[0].month, overlap_term[0].day, [teammate_name, term.league, term.team, team_display_str, overlap_term[0].year, overlap_term[0].month, overlap_term[0].day, overlap_term[1].year, overlap_term[1].month, overlap_term[1].day, tooltip_str, teammate_rows.iloc[0].link, years_str]))
                    if teammate_id not in teammate_length:
                        teammate_length[teammate_id] = pd.Timedelta(0)
                    teammate_length[teammate_id] += (overlap_term[1] - overlap_term[0])
        # sort all overlaps by first overlap year
        output.sort()
        sorted_output = []
        output_no_asg = []
        longest_name = ""
        for year, month, day, data in output:
            sorted_output.append({"player": data[0], "league": self.get_league_display_string(data[1]), "team": data[2], "team_display": data[3], "year1": data[4], "month1": data[5], "day1": data[6], "year2": data[7], "month2": data[8], "day2": data[9], "tooltip_str": data[10], "id": data[11], "years_str": data[12]})
            if self.get_league_display_string(data[1]) != "NHL ASG":
                output_no_asg.append({"player": data[0], "league": self.get_league_display_string(data[1]), "team": data[2], "team_display": data[3], "year1": data[4], "month1": data[5], "day1": data[6], "year2": data[7], "month2": data[8], "day2": data[9], "tooltip_str": data[10], "id": data[11], "years_str": data[12]})
            if self.get_string_width(self.strip_accents(data[0])) > self.get_string_width(self.strip_accents(longest_name)):
                longest_name = data[0]
        # create output with overlap term sorting
        overlap_term_output = []
        for _, _, _, data in output:
            teammate_id = data[11]
            teammate_overlap_term = teammate_length[teammate_id]
            overlap_term_output.append((teammate_overlap_term, data))
        overlap_term_output.sort(reverse=True) # sort by overlap term length
        sorted_output_overlap_term = []
        sorted_output_overlap_term_no_asg = []
        # format output with overlap term
        for _, data in overlap_term_output:
            sorted_output_overlap_term.append({"player": data[0], "league": self.get_league_display_string(data[1]), "team": data[2], "team_display": data[3], "year1": data[4], "month1": data[5], "day1": data[6], "year2": data[7], "month2": data[8], "day2": data[9], "tooltip_str": data[10], "id": data[11], "years_str": data[12]})
            if self.get_league_display_string(data[1]) != "NHL ASG":
                sorted_output_overlap_term_no_asg.append({"player": data[0], "league": self.get_league_display_string(data[1]), "team": data[2], "team_display": data[3], "year1": data[4], "month1": data[5], "day1": data[6], "year2": data[7], "month2": data[8], "day2": data[9], "tooltip_str": data[10], "id": data[11], "years_str": data[12]})
        return sorted_output, longest_name, output_no_asg, sorted_output_overlap_term, sorted_output_overlap_term_no_asg

    # get all players on the roster for this team in this season
    def get_players_from_roster(self, team, season):
        # set up season start/end timestamps
        season_dates = self.season_calc.get_season_dates(season)
        season_start = season_dates[0]
        season_end = season_dates[1] # this is the season end date NOT including the offseason
        # get players
        # a: players whose tenure on this team started during the season
        teammates_a = self.skaters.loc[(self.skaters.league=="nhl") & (self.skaters.team==team) & (self.skaters.start_date >= season_start) & (self.skaters.start_date<season_end)]
        # b: players whose tenure on this team started before the season and ended anytime after the season start (could be a different season)
        teammates_b = self.skaters.loc[(self.skaters.league=="nhl") & (self.skaters.team==team) & (season_start>self.skaters.start_date) & (season_start<self.skaters.end_date)]
        potential_teammates = pd.concat([teammates_a,teammates_b])
        return potential_teammates

    def query_roster_pair(self, team1, team2, season):
        start = time.time()
        # get players from team1
        players1 = self.get_players_from_roster(team1, season)
        ids1 = players1.link.unique()
        # get players from team2
        players2 = self.get_players_from_roster(team2, season)
        ids2 = players2.link.unique()
        # all pairs of players, get names
        team1_players_list = []
        player_id_to_name = {}
        for player1 in ids1:
            playername = self.get_player_name_from_id(player1)
            team1_players_list.append(playername)
            player_id_to_name[player1] = playername
        team2_players_list = []
        for player2 in ids2:
            playername = self.get_player_name_from_id(player2)
            team2_players_list.append(playername)
            player_id_to_name[player2] = playername
        # even out length
        if len(team1_players_list) < len(team2_players_list):
            while len(team1_players_list) < len(team2_players_list):
                team1_players_list.append("zzzplaceholder")
        if len(team2_players_list) < len(team1_players_list):
            while len(team2_players_list) < len(team1_players_list):
                team2_players_list.append("zzzplaceholder")
        # alphabetize
        team1_players_list.sort(reverse=True)
        team1_players_index = {}
        for idx, playername in enumerate(team1_players_list):
            team1_players_index[playername] = idx
        team2_players_list.sort(reverse=True)
        team2_players_index = {}
        for idx, playername in enumerate(team2_players_list):
            team2_players_index[playername] = idx
        # turn placeholder strings into empty strings (now that indices are set)
        for idx, playername in enumerate(team1_players_list):
            if playername == "zzzplaceholder":
                team1_players_list[idx] = ""
        for idx, playername in enumerate(team2_players_list):
            if playername == "zzzplaceholder":
                team2_players_list[idx] = ""
        # find links
        connections = []
        # identify guys who appear on both rosters
        traded_players = set(ids1).intersection(ids2)
        for player1 in ids1:
            playername1 = player_id_to_name[player1]
            player_data = {} 
            potential_overlap, _, _, _, _ = self.get_overlapping_player_terms(player1) # TODO ASG
            for row in potential_overlap:
                if row['id'] in ids2:
                    playername2 = player_id_to_name[row['id']] #row['player']
                    relationships = self.is_before_after_during_season(season, row['year1'], row['month1'], row['day1'], row['year2'], row['month2'], row['day2'])
                    if 'before' not in relationships and 'during' not in relationships:
                        continue # ignore terms that only occur AFTER the current season
                    data = f"{row['team']} ({row['league']}, {row['years_str']})"#{row['year1']}-{row['year2']})"
                    player2_key = (playername2, row['id'])
                    if player2_key not in player_data:
                        player_data[player2_key] = []
                    player_data[player2_key].append((data, row['league']))
            output_tmp = []
            for playername2, player2_id in player_data:
                # create a link
                data_strs = []
                data_leagues = []
                for data_str, league in player_data[(playername2, player2_id)]:
                    data_strs.append(data_str)
                    data_leagues.append(league)
                idx1 = team1_players_index[playername1]
                idx2 = team2_players_index[playername2]
                relationship_type = self.categorize_league_list(data_leagues)
                link_str = ", ".join(data_strs)
                out_str = f"{playername1} ({link_str})--{playername2} ({link_str})" 
                if player1 in traded_players or player2_id in traded_players:
                    relationship_type = "gray" # gray out links for guys who appear on both rosters
                connections.append((idx1, idx2, out_str, relationship_type))
        # format output
        output = {'team1_players': team1_players_list, 'team2_players': team2_players_list, 'links': connections}
        end = time.time()
        print(f"elapsed time: {end-start}")
        return output
        
    def display_roster_history(self, team, season):
        # new end date based on input season (don't show all the way to most recent season)
        season_dates = self.season_calc.get_season_dates(season)
        latest_date = season_dates[1]
        # get all players from (team, year) roster
        players = self.get_players_from_roster(team, season)
        player_ids = players.link.unique()
        unsorted_output = []
        for player_id in player_ids:
            career = self.get_player_career(player_id, latest_date)
            tenure_time = pd.Timedelta(0, "d") 
            player_rows = []
            for row in career:
                if row["league"] == "nhl":
                    row["player"] = self.get_player_name_from_id(player_id)
                    player_rows.append(row)
                    if row["team_display"] == team: # add to tenured time
                        start_date = pd.to_datetime(f"{row['year1']}/{row['month1']}/{row['day1']}")
                        end_date = pd.to_datetime(f"{row['year2']}/{row['month2']}/{row['day2']}")
                        tenure_time += end_date-start_date 
            for row in player_rows:
                tenure_time_years = tenure_time/np.timedelta64(1,'Y')
                unsorted_output.append((tenure_time,[f"{row['player']} ({round(tenure_time_years)} y)", row['league'], row['team_display'], row['year1'], row['month1'], row['day1'], row['year2'], row['month2'], row['day2'], row['tooltip_str'], row['id']]))
        unsorted_output.sort(reverse=True)
        sorted_output = []
        for tenure_time, data in unsorted_output:
            sorted_output.append({"player": data[0], "league": data[1], "team_display": data[2], "year1": data[3], "month1": data[4], "day1": data[5], "year2": data[6], "month2": data[7], "day2": data[8], "tooltip_str": data[9], "id": data[10]})
        return sorted_output
    
    def query_roster(self, player_id, team, season):
        start = time.time()
        # get all players from (team, year) roster
        potential_teammates = self.get_players_from_roster(team, season)
        potential_teammate_ids = potential_teammates.link.unique()
        output = {"before": [], "during": [], "after": []}
        output_no_asg = {"before": [], "during": [], "after": []}
        # get all teammates for target player
        all_player_teammates, _, _, _, _ = self.get_overlapping_player_terms(player_id)
        # reformat
        overlapping_teammates = {}
        overlapping_teammates_no_asg = {}
        for row in all_player_teammates:
            if row['id'] in potential_teammate_ids:
                # add to all data
                if row['id'] not in overlapping_teammates:
                    overlapping_teammates[row['id']] = []
                overlapping_teammates[row['id']].append(row)
                # add to no_asg data
                if row['league'] != 'NHL ASG':
                    if row['id'] not in overlapping_teammates_no_asg:
                        overlapping_teammates_no_asg[row['id']] = []
                    overlapping_teammates_no_asg[row['id']].append(row)
        # all data
        for teammate_id in overlapping_teammates:
            overlap = overlapping_teammates[teammate_id]
            if len(overlap) > 0:
                playername = self.get_player_name_from_id(teammate_id)
                formatted_data = {"before": [], "after": [], "during": []}
                for term in overlap:
                    relationships = self.is_before_after_during_season(season, term['year1'], term['month1'], term['day1'], term['year2'], term['month2'], term['day2'])
                    for relationship in relationships:
                        formatted_data[relationship].append(f"{term['team']} ({term['league']}, {term['years_str']})")
                for relationship in formatted_data:
                    if len(formatted_data[relationship]) > 0:
                        player_data = {"playername": playername, "data": ", ".join(formatted_data[relationship])}
                        output[relationship].append((playername, player_data))
        sorted_output = {"before": [], "after": [], "during": []}
        for relationship in output:
            output[relationship].sort()
            for name, data in output[relationship]:
                sorted_output[relationship].append(data)
        # no asg data
        for teammate_id in overlapping_teammates_no_asg:
            overlap = overlapping_teammates_no_asg[teammate_id]
            if len(overlap) > 0:
                playername = self.get_player_name_from_id(teammate_id)
                formatted_data = {"before": [], "after": [], "during": []}
                for term in overlap:
                    relationships = self.is_before_after_during_season(season, term['year1'], term['month1'], term['day1'], term['year2'], term['month2'], term['day2'])
                    for relationship in relationships:
                        formatted_data[relationship].append(f"{term['team']} ({term['league']}, {term['years_str']})")
                for relationship in formatted_data:
                    if len(formatted_data[relationship]) > 0:
                        player_data = {"playername": playername, "data": ", ".join(formatted_data[relationship])}
                        output_no_asg[relationship].append((playername, player_data))
        sorted_output_no_asg = {"before": [], "after": [], "during": []}
        for relationship in output_no_asg:
            output_no_asg[relationship].sort()
            for name, data in output_no_asg[relationship]:
                sorted_output_no_asg[relationship].append(data)
        end = time.time()
        print(f"elapsed time: {end-start}")
        return sorted_output, sorted_output_no_asg

    # returns list of terms to specify whether the period START_YEAR to END_YEAR happens BEFORE/DURING/AFTER the specified season
    def is_before_after_during_season(self, season, year1, month1, day1, year2, month2, day2):
        start_date = pd.to_datetime(f"{year1}/{month1}/{day1}")
        end_date = pd.to_datetime(f"{year2}/{month2}/{day2}")
        season_dates = self.season_calc.get_season_dates(season)
        season_start = season_dates[0]
        season_end = season_dates[1]  # this is the season end date NOT including the offseason
        prev_season = self.season_calc.get_prev_season(season)
        prev_season_end = self.season_calc.get_season_dates(prev_season)[1] # strict end of last season (not including offseason)
        next_season = self.season_calc.get_next_season(season)
        next_season_start = self.season_calc.get_season_dates(next_season)[0]
        relationship = []
        if start_date < prev_season_end:
            relationship.append("before")
        if end_date > next_season_start: 
            relationship.append("after")
        if start_date <= season_start <= end_date:
            relationship.append("during")
        if start_date <= season_end <= end_date:
            relationship.append("during")
        return relationship

    def get_player_name_from_id(self, player_id):
        return self.name_db.get_name(player_id, "ep")
        # terms = self.get_terms_from_player_id(player_id)
        # return terms[0].player # player_id is unique, it doesn't matter which row we use for the name

    def player_to_description(self, player_row):
        year1 = pd.to_datetime(player_row.start_date).year
        year2 = pd.to_datetime(player_row.end_date).year
        return f"most recently {player_row.team} ({year1}-{year2})"

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

    # given an input name, return the player id (EliteProspects url)
    # if there are multiple possible players, return all possibilities, each with a description
    def retrieve_player_link(self, name):
        return self.name_db.get_possible_links(name, "ep")

    def get_teammates_intersection_format(self, teammates1, teammates2):
        # get intersection
        teammates1_dict = {}
        for row in teammates1:
            if row['id'] not in teammates1_dict:
                teammates1_dict[row['id']] = []
            teammates1_dict[row['id']].append(row)
        overlapping_teammates = {}
        for row in teammates2:
            if row['id'] in teammates1_dict:
                if row['id'] not in overlapping_teammates:
                    overlapping_teammates[row['id']] = []
                overlapping_teammates[row['id']].append(row)
        # format results
        output = []
        for player_id in overlapping_teammates:
            player_name = self.get_player_name_from_id(player_id)
            output.append((player_name, self.condense_terms_into_table_rows(teammates1_dict[player_id], overlapping_teammates[player_id])))
        output.sort()
        sorted_output = []
        for player_name, data in output:
            for row in data:
                sorted_output.append(row)
        return sorted_output

    def traverse_graph(self, player1_id, player2_id):
        teammates1, _, teammates1_no_asg, _, _ = self.get_overlapping_player_terms(player1_id)
        teammates2, _, teammates2_no_asg, _, _ = self.get_overlapping_player_terms(player2_id)
        sorted_output = self.get_teammates_intersection_format(teammates1, teammates2)
        sorted_output_no_asg = self.get_teammates_intersection_format(teammates1_no_asg, teammates2_no_asg)
        return sorted_output, sorted_output_no_asg

    def condense_terms_into_table_rows(self, rows1, rows2):
        player = rows1[0]["player"]
        max_rows = max(len(rows1), len(rows2))
        cells = []
        for i in range(max_rows):
            row = []
            if i == 0:
                row.append(player)
            else:
                row.append("")
            if i < len(rows1):
                row.append(f'{rows1[i]["team"]} ({rows1[i]["league"]}, {rows1[i]["years_str"]})')
            else:
                row.append('')
            if i < len(rows2):
                row.append(f'{rows2[i]["team"]} ({rows2[i]["league"]}, {rows2[i]["years_str"]})')
            else:
                row.append('')
            cells.append(row)
        return cells
