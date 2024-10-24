import pandas as pd
import time
import os.path
from datetime import date
import numpy as np
from sqlalchemy import create_engine, text as sql_text
from site_builder.normalize_name import normalize_name
import sys


from site_builder.build_db import get_js_date_values, get_years_str, get_player_term_tooltip_str, get_tournament_leagues
from site_builder.season_calculator import SeasonCalculator
class teammates_db():

    def __init__(self, config, db):
        
        self.db = db
        self.latest_date = pd.to_datetime(config['timeline_end']) # TODO set to end of current season?
        self.current_date = pd.to_datetime(config['current_date']) # set to last scraped day of game data
        self.tournament_leagues = get_tournament_leagues() #{'og': (2,1), 'wjc-20': (1,1), 'wc': (6,1), 'wjc-18': (4,1), 'whc-17': (11,0), 'wcup': (9,0), 'nhl-asg': (2,1)} # first value is month and second value is 0 if the first year in a season should be used, 1 if the second year in the season should be used
        self.drop_leagues = {'EHT', 'M-Cup'}
        self.game_types = {1: 'preseason', 2: 'regular season', 3: 'playoff'}
        self.season_calc = SeasonCalculator(date.today(), os.path.join(config['data_dir'], 'nhl_season_dates.txt')) # TODO do we need today's date for season calculator?
        self.game_columns = ['gameId', 'gameUrl', 'gameDate', 'gameType', 'seasonId', 'seasonName', 'awayTeam', 'homeTeam', 'awayScore', 'homeScore', 'venueName', 'winningTeam']
        self.game_player_columns = ['playerId', 'team', 'toi', 'assists', 'points', 'goals', 'shots', 'hits', 'powerPlayGoals', 'powerPlayPoints', 'pim', 'faceOffWinningPctg', 'faceoffs', 'shortHandedGoals', 'shPoints', 'blockedShots', 'plusMinus', 'powerPlayToi', 'shorthandedToi', 'evenStrengthShotsAgainst', 'powerPlayShotsAgainst', 'shorthandedShotsAgainst', 'saveShotsAgainst', 'evenStrengthGoalsAgainst', 'powerPlayGoalsAgainst', 'shorthandedGoalsAgainst', 'goalsAgainst', 'shotsAgainst', 'saves', 'savePercentage']
        self.team_names = {'Mighty Ducks of Anaheim': 'ANA', 'Anaheim Ducks': 'ANA', 'Arizona Coyotes': 'ARI', 'Atlanta Thrashers': 'ATL', 'Boston Bruins': 'BOS', 'Buffalo Sabres': 'BUF', 'Carolina Hurricanes': 'CAR', 'Columbus Blue Jackets': 'CBJ', 'Calgary Flames': 'CGY', 'Chicago Blackhawks': 'CHI', 'Colorado Avalanche': 'COL', 'Dallas Stars': 'DAL', 'Detroit Red Wings': 'DET', 'Edmonton Oilers': 'EDM', 'Florida Panthers': 'FLA', 'Los Angeles Kings': 'LAK', 'Minnesota Wild': 'MIN', 'Montreal Canadiens': 'MTL', 'Montréal Canadiens': 'MTL', 'New Jersey Devils': 'NJD', 'Nashville Predators': 'NSH', 'New York Islanders': 'NYI', 'New York Rangers': 'NYR', 'Ottawa Senators': 'OTT', 'Philadelphia Flyers': 'PHI', 'Phoenix Coyotes': 'PHX', 'Pittsburgh Penguins': 'PIT', 'Seattle Kraken': 'SEA', 'San Jose Sharks': 'SJS', 'St. Louis Blues': 'STL', 'Tampa Bay Lightning': 'TBL', 'Toronto Maple Leafs': 'TOR', 'Vancouver Canucks': 'VAN', 'Vegas Golden Knights': 'VGK', 'Winnipeg Jets': 'WPG', 'Washington Capitals': 'WSH'}

    def get_team_name_abbrev(self, team_name_full):
        if team_name_full in self.team_names:
            return self.team_names[team_name_full]
        else:
            return team_name_full

    def get_latest_date(self):
        return self.latest_date

    def categorize_league_list(self, league_list):
        league_list = list(set(league_list))
        contains_non_national = False
        national_set = {'World Juniors', 'Worlds', 'Olympics', 'World Juniors', 'WC-U18', 'WHC-17', 'World Cup'}
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

    def get_terms_from_player_id(self, player_id, latest_date=None):
        # use global latest date if none specified
        if not latest_date:
            latest_date = self.latest_date
        rows = pd.read_sql_query(sql=sql_text(f"select * from skaters join links using(playerId) where skaters.playerId = '{player_id}' and start_date <= date('{latest_date}')"), con=self.db.engine, parse_dates=['start_date', 'end_date']) 
        rows = rows.drop(['index'], axis=1, errors='ignore')
        rows['end_date'] = rows['end_date'].clip(None, latest_date) # TODO does this work?
        return rows
            
    # get player's individual timeline (the top row) including NHL playoff info
    def get_player_career(self, player_id, latest_date=None, to_dict=True):
        player_rows = self.get_terms_from_player_id(player_id, latest_date)
        player_rows = player_rows[~player_rows['league'].isin(self.drop_leagues)]
        player_playoffs = pd.read_sql_query(sql=sql_text(f"select * from player_playoffs where link='{player_id}'"), con=self.db.engine)
        player_playoffs["league"] = "nhl_playoffs"
        player_playoffs["player"] = ""
        player_playoffs["team_display_str"] = "" 
        output_df = pd.concat([player_rows, player_playoffs]) 
        start_date = output_df['start_date'].min()
        start_date = {"year": start_date.year, "month": start_date.month-1, "day": start_date.day}
        end_date = output_df['end_date'].max()
        end_date = {"year": end_date.year, "month": end_date.month-1, "day": end_date.day}
        output_df = output_df.drop(['start_date', 'end_date', 'team'], axis=1) # unneeded rows (will cause an issue with json in html)
        if to_dict:
            output = output_df.to_dict('records')
        return output, start_date, end_date

    # function for:
    # - one player career
    # - two players overlap (have they been teammates) and shared teammates
    def get_overlapping_player_terms(self, player1_id, player2_id=None, to_dict=True):
        start = time.time()
        if player2_id: 
            overlaps = pd.read_sql_query(sql=sql_text(f"select team, league, league_display_str, start_date_x, end_date_x, playerId_x, color_x as color, start_date_y, end_date_y, t.playerId_y, overlap_start_date, overlap_end_date, overlap_len, overlap_start_year_js, overlap_start_month_js, overlap_start_day_js, overlap_end_year_js, overlap_end_month_js, overlap_end_day_js, num_seasons, years_str, team_display_str, sum_overlap_len, pn.playerName as playerName_y, name_length as name_length_y from teammates t join links l1 on t.playerId_x = l1.playerId join links l2 on t.playerId_y = l2.playerId join (select playerId_y, sum(overlap_len) as sum_overlap_len from teammates t where playerId_x = '{player1_id}' and playerId_y = '{player2_id}' group by playerId_y) as sum_table on t.playerId_y = sum_table.playerId_y join player_names pn on l2.playerName = pn.playerName where playerId_x = '{player1_id}' and t.playerId_y = '{player2_id}'"), con=self.db.engine, parse_dates=['start_date_x', 'end_date_x', 'start_date_y', 'end_date_y', 'overlap_start_date', 'overlap_end_date'])
        else:
            overlaps = pd.read_sql_query(sql=sql_text(f"select team, league, league_display_str, start_date_x, end_date_x, playerId_x, color_x as color, start_date_y, end_date_y, t.playerId_y, overlap_start_date, overlap_end_date, overlap_len, overlap_start_year_js, overlap_start_month_js, overlap_start_day_js, overlap_end_year_js, overlap_end_month_js, overlap_end_day_js, num_seasons, years_str, team_display_str, sum_overlap_len, pn.playerName as playerName_y, name_length as name_length_y from teammates t join links l1 on t.playerId_x = l1.playerId join links l2 on t.playerId_y = l2.playerId join (select playerId_y, sum(overlap_len) as sum_overlap_len from teammates t where playerId_x = '{player1_id}' group by playerId_y) as sum_table on t.playerId_y = sum_table.playerId_y join player_names pn on l2.playerName = pn.playerName where playerId_x = '{player1_id}'"), con=self.db.engine, parse_dates=['start_date_x', 'end_date_x', 'start_date_y', 'end_date_y', 'overlap_start_date', 'overlap_end_date'])
        end = time.time()
        print(f"read_sql_query elapsed time: {end-start}")
        # TODO precompute tooltip string except for canon name ? (so that I don't need to store tournament information in this script)
        overlaps['tooltip_str'] = overlaps.apply(lambda x: self.get_tooltip_str(x.playerName_y, x.team_display_str, x.years_str, x.league, x.num_seasons), axis=1) 
        overlaps = overlaps[~overlaps['league'].isin(self.drop_leagues)] # TODO put this in --process
        end = time.time()
        print(f"drop leagues elapsed time: {end-start}")
        # TODO should sorting be a separate function?
        # sort orders
        overlaps.sort_values(by=['overlap_start_date', 'playerName_y'], inplace=True)
        end = time.time()
        print(f"sort by overlap_sort_date elapsed time: {end-start}")
        overlaps_sort_date_no_asg = overlaps.loc[overlaps['league']!='nhl-asg']
        end = time.time()
        print(f"remove asg elapsed time: {end-start}")
        # get max name length 
        if len(overlaps) > 0:
            idx = overlaps['name_length_y'].idxmax()
            max_name = overlaps.loc[idx]['playerName_y'] # with asg
        else:
            max_name = "" #self.get_name_from_ep_link(player1_id)
        if len(overlaps_sort_date_no_asg) > 0:
            idx = overlaps_sort_date_no_asg['name_length_y'].idxmax()
            max_name_no_asg = overlaps_sort_date_no_asg.loc[idx]['playerName_y'] # without asg
        else:
            max_name_no_asg = "" # TODO I think this goes unused right? if there are no overlaps? #self.get_name_from_ep_link(player2_id)
        end = time.time()
        print(f"get max name length elapsed time: {end-start}")
        # finish sorting
        overlaps_sort_date = overlaps
        overlaps_sort_len = overlaps.sort_values(by=['sum_overlap_len', 'playerName_y'], ascending=False)
        end = time.time()
        print(f"sort by overlap length elapsed time: {end-start}")
        overlaps_sort_len_no_asg = overlaps_sort_len.loc[overlaps_sort_len['league']!='nhl-asg'] # no asg
        end = time.time()
        print(f"remove asg elapsed time: {end-start}")
        if to_dict:
            overlaps_sort_len_no_asg = overlaps_sort_len_no_asg.to_dict('records') 
            overlaps_sort_date_no_asg = overlaps_sort_date_no_asg.to_dict('records') 
            overlaps_sort_date = overlaps_sort_date.to_dict('records')
            overlaps_sort_len = overlaps_sort_len.to_dict('records')
        end = time.time()
        print(f"to_dict elapsed time: {end-start}")
        return overlaps_sort_date, max_name, max_name_no_asg, overlaps_sort_date_no_asg, overlaps_sort_len, overlaps_sort_len_no_asg

    # TODO should I add a tournament flag in the database so that I don't have to store which leagues are tournament leagues in the code?
    # no I should precompute the tooltip string EXCEPT FOR the player name, that's the only thing that has to be added at the end? Or even I run the tooltip string generation after I disambiguate names etc
    def get_tooltip_str(self, teammate_name, team_display_str, years_str, league, num_seasons):
        tooltip_str = f"{teammate_name}<br>{team_display_str}<br>{years_str}"
        if league not in self.tournament_leagues:
            if num_seasons == 1:
                tooltip_str += f" ({num_seasons} season)"
            else:
                tooltip_str += f" ({num_seasons} seasons)"
        return tooltip_str
    
    def get_two_rosters_overlap(self, team1, team2, season):
        start = time.time()
        # get season dates
        season_dates = self.season_calc.get_season_dates(season)
        season_start_date = season_dates[0]
        season_end_date = season_dates[1] # this is the season end date NOT including the offseason
        # get overlap
        r1_q = f"""
                    select distinct links.playerId, playerName as playerName_x
                    from skaters join links on links.playerId=skaters.playerId
                    where league='NHL' and team='{team1}' 
                    and ((start_date <= '{season_start_date}' and end_date > '{season_start_date}') or (start_date > '{season_start_date}' and start_date <= '{season_end_date}'))
                """
        roster1 = pd.read_sql_query(sql=sql_text(r1_q), con=self.db.engine) 
        r2_q = f"""
                    select distinct links.playerId, playerName as playerName_y 
                    from skaters join links on links.playerId=skaters.playerId
                    where league='NHL' and team='{team2}' 
                    and ((start_date <= '{season_start_date}' and end_date > '{season_start_date}') or (start_date > '{season_start_date}' and start_date <= '{season_end_date}'))
                """
        roster2 = pd.read_sql_query(sql=sql_text(r2_q), con=self.db.engine) 
        q = f"""
                with r1 as 
                (
                    {r1_q}
                ),
                r2 as
                (
                    {r2_q}
                ), 
                r1t as 
                (
                    select * from 
                    r1 left join teammates on r1.playerId=teammates.playerId_x
                )
                select 
                    playerName_x,
                    playerName_y, 
                    team,
                    league_display_str,
                    years_str
                from 
                r2 left join r1t 
                    on r1t.playerId_y = r2.playerId 
                where r1t.overlap_start_date <= date('{season_end_date}')
            """ # TODO is the overlap_start_date filter correct here?
        # TODO the years_str returned here will be inaccurate - it was created before the season was selected so it could contain years into the future -- need to clip here and recompute years_str
        rows = pd.read_sql_query(sql=sql_text(q), con=self.db.engine) 
        # set up player lists for each team: alphabetical order, placeholders to make the chart line up
        team1_names = list(roster1['playerName_x'].unique())
        team2_names = list(roster2['playerName_y'].unique())
        if len(team1_names) < len(team2_names):
            padding = ["zzzplaceholder"] * (len(team2_names) - len(team1_names))
            team1_names += padding
        if len(team1_names) > len(team2_names):
            padding = ["zzzplaceholder"] * (len(team1_names) - len(team2_names))
            team2_names += padding
        team1_names = sorted(team1_names, reverse=True)
        team2_names = sorted(team2_names, reverse=True)
        # get indices for plotting
        team1_players_index = {}
        for idx, playername in enumerate(team1_names):
            team1_players_index[playername] = idx
        team2_players_index = {}
        for idx, playername in enumerate(team2_names):
            team2_players_index[playername] = idx
        # turn placeholder strings into empty strings (now that indices are set)
        for idx, playername in enumerate(team1_names):
            if playername == "zzzplaceholder":
                team1_names[idx] = ""
        for idx, playername in enumerate(team2_names):
            if playername == "zzzplaceholder":
                team2_names[idx] = ""
        # identify guys who appear on both rosters
        traded_players = set(team1_names).intersection(team2_names)
        # find links
        connections_asg = self.format_roster_overlap_for_plotting(rows, team1_players_index, team2_players_index, traded_players)
        rows_no_asg = rows.loc[rows['league_display_str']!='NHL ASG']
        connections_no_asg = self.format_roster_overlap_for_plotting(rows_no_asg, team1_players_index, team2_players_index, traded_players)
        end = time.time()
        print(f"elapsed time: {end-start}")
        return {'team1_players': team1_names, 'team2_players': team2_names, 'links': connections_asg}, {'team1_players': team1_names, 'team2_players': team2_names, 'links': connections_no_asg} #, 'links_no_asg':}

    def format_roster_overlap_for_plotting(self, rows, team1_players_index, team2_players_index, traded_players):
        # TODO there's probably a slightly more efficient way to do this in pandas but
        player_data = {}
        connections = []
        # for all overlapping terms, get team and league and term data, set up so we can combine this info
        for _, row in rows.iterrows(): 
            if row['playerName_x'] not in player_data:
                player_data[row['playerName_x']] = {}
            if row['playerName_y'] not in player_data[row['playerName_x']]:
                player_data[row['playerName_x']][row['playerName_y']] = []
            player_data[row['playerName_x']][row['playerName_y']].append((f"{row['team']} ({row['league_display_str']}, {row['years_str']})", row['league_display_str']))
        # now combine the info for all unique player x player pairs
        for player_name_x in player_data:
            for player_name_y in player_data[player_name_x]:
                # create a link
                data_strs = []
                data_leagues = []
                for data_str, league in player_data[player_name_x][player_name_y]:
                    data_strs.append(data_str)
                    data_leagues.append(league)
                idx1 = team1_players_index[player_name_x]
                idx2 = team2_players_index[player_name_y]
                relationship_type = self.categorize_league_list(data_leagues)
                link_str = ", ".join(data_strs)
                out_str = f"{player_name_x} ({link_str})--{player_name_y} ({link_str})" 
                if player_name_x in traded_players or player_name_y in traded_players:
                    relationship_type = "gray" # gray out links for guys who appear on both rosters
                connections.append((idx1, idx2, out_str, relationship_type))
        return connections
    
    def get_roster_history_range(self, team, start_season, end_season):
        start_season_dates = self.season_calc.get_season_dates(start_season)
        start_season_start_date = start_season_dates[0]
        end_season_dates = self.season_calc.get_season_dates(end_season)
        end_season_end_date = end_season_dates[1]
        # get all tenures (player, term) that overlap with these seasons
        q = f"""
                select 
                    distinct skaters.playerId, team, league, start_date, end_date,
                        start_year_js, start_month_js, start_day_js,
                        end_year_js, end_month_js, end_day_js, team_display_str,
                        tooltip_str, playerName
                from skaters left join links on skaters.playerId=links.playerId
                    where league='NHL' and team='{team}' 
                    and (('{start_season_start_date}' between start_date and end_date) or (start_date between '{start_season_start_date}' and '{end_season_end_date}'))
                
            """
        rows = pd.read_sql_query(sql=sql_text(q), con=self.db.engine, parse_dates=['start_date', 'end_date']) 
        # get tenure time with the particular team
        rows['tenure_length_years'] = rows['end_date'] - rows['start_date']
        rows['tenure_length_years'] = round(rows['tenure_length_years']/pd.Timedelta('365 days')).astype(int)
        # clip to the specified seasons (this determines the plot range not the tooltip strings)
        rows['orig_start_date'] = rows['start_date']
        rows['start_date'] = rows['start_date'].clip(start_season_start_date, None) 
        rows['end_date'] = rows['end_date'].clip(None, end_season_end_date)
        rows[['start_year_js', 'start_month_js', 'start_day_js']] = rows.apply(lambda x: get_js_date_values(x.start_date), axis=1, result_type='expand')
        rows[['end_year_js', 'end_month_js', 'end_day_js']] = rows.apply(lambda x: get_js_date_values(x.end_date), axis=1, result_type='expand')
        # sum and sort by tenure length
        tenure_rows = rows.groupby(['playerId'])['tenure_length_years'].sum().reset_index()
        tenure_rows.rename(columns={"tenure_length_years": "tenure_sum"}, inplace=True)
        rows = rows.merge(tenure_rows, how='left', on='playerId')
        rows['playerName'] = rows['playerName'] + " (" + rows['tenure_sum'].astype(str) + " y)"
        # sort output by tenure time
        # rows.sort_values(by=['tenure_sum', 'playerName'], inplace=True, ascending=False)
        rows.sort_values(by=['orig_start_date', 'playerName'], inplace=True, ascending=True)
        return rows.to_dict('records')
        
    # TODO not sure the cutoff here makes sense if it's the beginning of the new season?? guys who left at the end of last season vs guys who are still on the roster now? count as the same? 
    def get_roster_history(self, team, season):
        # new end date based on input season (don't show all the way to most recent season)
        season_dates = self.season_calc.get_season_dates(season)
        # get all players from (team, year) roster and their NHL teams
        q = f"""
                with roster as 
                (
                    select 
                        distinct playerId 
                    from skaters where league='NHL' and team='{team}' 
                        and ((start_date <= '{season_dates[0]}' and end_date > '{season_dates[0]}') or (start_date > '{season_dates[0]}' and start_date <= '{season_dates[1]}'))
                )
                select 
                    skaters.playerId, team, league, start_date, end_date,
                    start_year_js, start_month_js, start_day_js,
                    end_year_js, end_month_js, end_day_js, team_display_str,
                    tooltip_str, playerName
                from roster 
                    left join skaters on roster.playerId=skaters.playerId
                    left join links on skaters.playerId=links.playerId where skaters.league='NHL' and skaters.start_date <= date('{season_dates[1]}')
            """
        rows = pd.read_sql_query(sql=sql_text(q), con=self.db.engine, parse_dates=['start_date', 'end_date']) 
        rows['end_date'] = rows['end_date'].clip(None, season_dates[1]) # clip to the end of the relevant season
        # update other fields based on new end date
        rows[['end_year_js', 'end_month_js', 'end_day_js']] = rows.apply(lambda x: get_js_date_values(x.end_date), axis=1, result_type='expand')
        rows['years_str'] = rows.apply(lambda x: get_years_str(x.league, x.start_date, x.end_date), axis=1)
        rows['tooltip_str'] = rows.apply(lambda x: get_player_term_tooltip_str(x.team_display_str, x.years_str), axis=1)
        # get tenure time with the particular team
        rows['tenure_length_years'] = rows['end_date'] - rows['start_date']
        rows['tenure_length_years'] = round(rows['tenure_length_years']/pd.Timedelta('365 days')).astype(int)
        team_rows = rows.loc[rows['team']==team]
        team_rows = team_rows.groupby(['playerId'])['tenure_length_years'].sum().reset_index()
        team_rows.rename(columns={"tenure_length_years": "tenure_sum"}, inplace=True)
        rows = rows.merge(team_rows, how='left', on='playerId')
        rows['playerName'] = rows['playerName'] + " (" + rows['tenure_sum'].astype(str) + " y)"
        # sort output by tenure time
        rows.sort_values(by=['tenure_sum', 'playerName'], inplace=True, ascending=False)
        return rows.to_dict('records')
    
    def get_one_player_roster(self, player_id, team, season):
        # get seasons dates
        season_dates = self.season_calc.get_season_dates(season)
        season_start_date = season_dates[0]
        season_end_date = season_dates[1] # this is the season end date NOT including the offseason
        # get overlaps
        overlaps = pd.read_sql_query(sql=sql_text(f"select t.team, t.league, start_date_x, end_date_x, playerId_x, color_x as color, start_date_y, end_date_y, t.playerId_y, overlap_start_date, overlap_end_date, overlap_len, overlap_start_year_js, overlap_start_month_js, overlap_start_day_js, overlap_end_year_js, overlap_end_month_js, overlap_end_day_js, num_seasons, t.years_str, league_display_str, t.team_display_str, sum_overlap_len, pn.playerName as playerName_y, name_length as name_length_y from teammates t join links l1 on t.playerId_x = l1.playerId join links l2 on t.playerId_y = l2.playerId join (select playerId_y, sum(overlap_len) as sum_overlap_len from teammates t where playerId_x = '{player_id}' group by playerId_y) as sum_table on t.playerId_y = sum_table.playerId_y join player_names pn on l2.playerName = pn.playerName join (select * from skaters where league='NHL' and team='{team}' and ((start_date <= date('{season_start_date}') and end_date > date('{season_start_date}')) or (start_date >= date('{season_start_date}') and start_date < date('{season_end_date}')))) as team_roster on team_roster.playerId = t.playerId_y where playerId_x = '{player_id}' order by pn.playerName"), con=self.db.engine, parse_dates=['start_date_x', 'end_date_x', 'start_date_y', 'end_date_y', 'overlap_start_date', 'overlap_end_date'])
        overlaps['overlap_end_date'] = pd.to_datetime(overlaps['overlap_end_date'])
        overlaps['overlap_start_date'] = pd.to_datetime(overlaps['overlap_start_date'])
        # split into before/during/after
        output_asg = {"data": []}
        output_asg["data"].append({"time": "Before", "teammates": overlaps.loc[overlaps['overlap_end_date'] < season_start_date]})
        output_asg["data"].append({"time": "During", "teammates": overlaps.loc[(((overlaps['overlap_start_date'] >= season_start_date) & (overlaps['overlap_start_date'] < season_end_date)) | ((overlaps['overlap_end_date'] >= season_start_date) & (overlaps['overlap_end_date'] < season_end_date)))]})
        output_asg["data"].append({"time": "After", "teammates": overlaps.loc[overlaps['overlap_start_date'] > season_end_date]})
        # filter no asg data
        output_no_asg = {"data": []}
        output_no_asg["data"].append({"time": "Before", "teammates": output_asg["data"][0]["teammates"].loc[output_asg["data"][0]["teammates"]['league']!='nhl-asg']})
        output_no_asg["data"].append({"time": "During", "teammates": output_asg["data"][1]["teammates"].loc[output_asg["data"][1]["teammates"]['league']!='nhl-asg']})
        output_no_asg["data"].append({"time": "After", "teammates": output_asg["data"][2]["teammates"].loc[output_asg["data"][2]["teammates"]['league']!='nhl-asg']})
        for idx in [0, 1, 2]:
            output_asg["data"][idx]["teammates"]["description"] = output_asg["data"][idx]["teammates"].apply(lambda x: f"{x['team_display_str']} ({x['league_display_str']}, {x['years_str']})", axis=1)
            output_asg["data"][idx]["teammates"] = output_asg["data"][idx]["teammates"].groupby(['playerName_y'])['description'].apply(', '.join).reset_index().to_dict('records')
            output_no_asg["data"][idx]["teammates"]["description"] = output_no_asg["data"][idx]["teammates"].apply(lambda x: f"{x['team_display_str']} ({x['league_display_str']}, {x['years_str']})", axis=1)
            output_no_asg["data"][idx]["teammates"] = output_no_asg["data"][idx]["teammates"].groupby(['playerName_y'])['description'].apply(', '.join).reset_index().to_dict('records')
        total_teammates = 0
        for i in range(3):
            total_teammates += len(output_asg["data"][i]["teammates"])
        output_asg["num_teammates"] = total_teammates
        total_teammates = 0
        for i in range(3):
            total_teammates += len(output_no_asg["data"][i]["teammates"])
        output_no_asg["num_teammates"] = total_teammates
        return output_asg, output_no_asg

    def get_latest_game_date(self):
        return self.current_date
    
    def get_one_player_team_games(self, player_id, team):
        start = time.time()
        # get games this player played against team
        q = f"select *, case when winningTeam=game_player.team then 1 else 0 end as player1_win from game_player join games on game_player.gameId = games.gameId and games.gameDate < date('{self.current_date}') where playerId={player_id} and ((game_player.team='homeTeam' and games.awayTeam='{team}') or (game_player.team='awayTeam' and games.homeTeam='{team}')) order by gameDate"
        games_against = pd.read_sql_query(sql=sql_text(q), con=self.db.engine, parse_dates=['gameDate'])
        games_against['gameDate'] = games_against['gameDate'].astype(str)
        games_output = self.format_games_one_player(games_against)
        end = time.time()
        print(f"elapsed time: {end - start}")
        return games_output

    # restructure game data for HTML output
    def format_games_one_player(self, games):
        season_ids = games.seasonId.unique()
        season_ids.sort()
        data = {"count":  len(games), "record": self.get_win_loss_record(games), "split": []}
        for type_idx in self.game_types:
            data["split"].append({"type": self.game_types[type_idx], "count": len(games.loc[games.gameType == type_idx]), "record": self.get_win_loss_record(games.loc[games.gameType == type_idx])})
        data['seasons'] = []
        for season_id in season_ids:
            # get season rows
            season_data = {}
            season_rows = games.loc[games.seasonId == season_id]
            season_data = {"season_name": season_rows.iloc[0].seasonName, "count":  len(season_rows), "record": self.get_win_loss_record(season_rows), "split": []}
            for type_idx in self.game_types:
                rows = season_rows.loc[season_rows.gameType == type_idx]
                if len(rows) == 0:
                    continue
                season_data["split"].append({"type": self.game_types[type_idx], "count": len(rows), "record": self.get_win_loss_record(rows), "games": rows.to_dict('records')})
            data['seasons'].append(season_data)
        return data
    
    def format_games_two_player(self, games):
        season_ids = games.seasonId.unique()
        season_ids.sort()
        data = {"count":  len(games)}
        teammate_games = games.loc[(games['team_1'] == games['team_2'])]
        opponent_games = games.loc[(games['team_1'] != games['team_2'])]
        data["teammates"] = {"count":  len(teammate_games), "record": self.get_win_loss_record(teammate_games), "split": []}
        for type_idx in self.game_types:
            data["teammates"]["split"].append({"type": self.game_types[type_idx], "count": len(teammate_games.loc[games.gameType == type_idx]), "record": self.get_win_loss_record(teammate_games.loc[teammate_games.gameType == type_idx])})
        data["opponents"] = {"count":  len(opponent_games), "record": self.get_win_loss_record(opponent_games), "split": []}
        for type_idx in self.game_types:
            data["opponents"]["split"].append({"type": self.game_types[type_idx], "count": len(opponent_games.loc[opponent_games.gameType == type_idx]), "record": self.get_win_loss_record(opponent_games.loc[opponent_games.gameType == type_idx])})
        data['seasons'] = []
        for season_id in season_ids:
            # get season rows
            season_data = {}
            season_rows = games.loc[games.seasonId == season_id]
            season_rows['team_1_abbrev'] = season_rows['team_name_1'].apply(self.get_team_name_abbrev)
            season_rows['team_2_abbrev'] = season_rows['team_name_2'].apply(self.get_team_name_abbrev)
            season_data = {"season_name": season_rows.iloc[0].seasonName, "count":  len(season_rows)}
            # games as teammates
            teammate_games = season_rows.loc[(season_rows['team_1'] == season_rows['team_2'])]
            season_data["teammates"] = {"count":  len(teammate_games), "record": self.get_win_loss_record(teammate_games), "split": []}
            for type_idx in self.game_types:
                rows = teammate_games.loc[teammate_games.gameType == type_idx]
                if len(rows) == 0:
                    continue
                season_data["teammates"]["split"].append({"type": self.game_types[type_idx], "count": len(rows), "record": self.get_win_loss_record(rows), "games": rows.to_dict('records')})
            # games as opponents
            opponent_games = season_rows.loc[(season_rows['team_1'] != season_rows['team_2'])]
            season_data["opponents"] = {"count":  len(opponent_games), "record": self.get_win_loss_record(opponent_games), "split": []}
            for type_idx in self.game_types:
                rows = opponent_games.loc[opponent_games.gameType == type_idx]
                if len(rows) == 0:
                    continue
                season_data["opponents"]["split"].append({"type": self.game_types[type_idx], "count": len(rows), "record": self.get_win_loss_record(rows), "games": rows.to_dict('records')})
            data['seasons'].append(season_data)
        return data
    
    def get_win_loss_record(self, game_rows):
        total = len(game_rows)
        wins = game_rows['player1_win'].sum()
        losses = total - wins
        return {'wins': wins, 'losses': losses}
    
    def getGameTeam1(self, row):
        if row[f"team_1"] == 'homeTeam':
            return row['homeTeam']
        elif row[f"team_1"] == 'awayTeam':
            return row['awayTeam']
        else:
            return ""
    
    def getGameTeam2(self, row):
        if row[f"team_2"] == 'homeTeam':
            return row['homeTeam']
        elif row[f"team_2"] == 'awayTeam':
            return row['awayTeam']
        else:
            return ""
        
    def get_two_player_games(self, player1_id, player2_id):
        start = time.time()
        q = f"""
            with g1 as 
            (
                select 
                    {",".join([f"gp1.{x1} as {x1}_1" for x1 in self.game_player_columns])},
                    {",".join([f"games.{x}" for x in self.game_columns])},
                    case when winningTeam=gp1.team then 1 else 0 end as player1_win
                from game_player gp1 
                join games on gp1.gameId = games.gameId and games.gameDate < date('{self.current_date}') where playerId={player1_id}
            ),
            g2 as 
            (
                select 
                    games.gameId,
                    {",".join([f"gp2.{x2} as {x2}_2" for x2 in self.game_player_columns])},
                    case when winningTeam=gp2.team then 1 else 0 end as player2_win
                from game_player gp2 
                join games on gp2.gameId = games.gameId and games.gameDate < date('{self.current_date}') where playerId={player2_id}
            )
            select 
                *
            from g1 join g2 on g1.gameId = g2.gameId
            order by gameDate
        """
        common_games = pd.read_sql(sql=sql_text(q), con=self.db.engine, parse_dates=['gameDate'])
        common_games['gameDate'] = common_games['gameDate'].astype(str)
        common_games['team_name_1'] = common_games.apply(self.getGameTeam1, axis=1)
        common_games['team_name_2'] = common_games.apply(self.getGameTeam2, axis=1)
        games_output = self.format_games_two_player(common_games)
        end = time.time()
        print(f"elapsed time: {end - start}")
        return games_output

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

    def player_to_description(self, player_row):
        year1 = pd.to_datetime(player_row.start_date).year
        year2 = pd.to_datetime(player_row.end_date).year
        return f"most recently {player_row.team} ({year1}-{year2})"

    def get_links_from_name(self, input_name):
        tgt_name = normalize_name(input_name)
        possible_links = pd.read_sql_query(sql=sql_text(f'select playerId, link, links.playerName from norm_names join links on norm_names.playerName = links.playerName where norm_name="{tgt_name}" and playerId != ""'), con=self.db.engine)
        return possible_links

    # TODO make table formatting prettier -- maybe light grey highlighting the cell backgrounds? 
    def get_two_players_shared_teammates(self, player1_id, playerName_1, player2_id, playerName_2):
        # get player1 teammates
        teammates1_asg, _, _, teammates1_no_asg, _, _ = self.get_overlapping_player_terms(player1_id, to_dict=False)
        teammates1_asg['description'] = teammates1_asg.apply(lambda x: f"{x['team_display_str']} ({x['league_display_str']}, {x['years_str']})", axis=1)
        teammates1_asg = teammates1_asg.groupby(['playerName_y'])['description'].apply(', '.join).reset_index()
        teammates1_no_asg['description'] = teammates1_no_asg.apply(lambda x: f"{x['team_display_str']} ({x['league_display_str']}, {x['years_str']})", axis=1)
        teammates1_no_asg = teammates1_no_asg.groupby(['playerName_y'])['description'].apply(', '.join).reset_index()
        # get player2 teammates
        teammates2_asg, _, _, teammates2_no_asg, _, _ = self.get_overlapping_player_terms(player2_id, to_dict=False)
        teammates2_asg['description'] = teammates2_asg.apply(lambda x: f"{x['team_display_str']} ({x['league_display_str']}, {x['years_str']})", axis=1)
        teammates2_asg = teammates2_asg.groupby(['playerName_y'])['description'].apply(', '.join).reset_index()
        teammates2_no_asg['description'] = teammates2_no_asg.apply(lambda x: f"{x['team_display_str']} ({x['league_display_str']}, {x['years_str']})", axis=1)
        teammates2_no_asg = teammates2_no_asg.groupby(['playerName_y'])['description'].apply(', '.join).reset_index()
        # find intersections
        teammates_asg = teammates1_asg.merge(teammates2_asg, on='playerName_y', suffixes=('_1', '_2'))
        teammates_asg.rename(columns={"playerName_y": "", "description_1": f"played with {playerName_1}", "description_2": f"played with {playerName_2}"}, inplace=True)
        teammates_asg_out = teammates_asg.to_html(index=False)
        teammates_asg_out = teammates_asg_out.replace("<td>", "<td style='vertical-align: top;'>")
        teammates_no_asg = teammates1_no_asg.merge(teammates2_no_asg, on='playerName_y', suffixes=('_1', '_2'))
        teammates_no_asg.rename(columns={"playerName_y": "", "description_1": f"played with {playerName_1}", "description_2": f"played with {playerName_2}"}, inplace=True)
        teammates_no_asg_out = teammates_no_asg.to_html(index=False)
        teammates_no_asg_out = teammates_no_asg_out.replace("<td>", "<td style='vertical-align: top;'>")
        # get two players' term overlaps
        team_overlaps_sort_date, _, _, team_overlaps_sort_date_no_asg, _, _ = self.get_overlapping_player_terms(player1_id, player2_id)
        return team_overlaps_sort_date, team_overlaps_sort_date_no_asg, teammates_asg_out, len(teammates_asg), teammates_no_asg_out, len(teammates_no_asg)
