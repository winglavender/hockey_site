import pandas as pd
import sqlite3 as sql
import unicodedata

class hockey_db():

    def __init__(self):
        db_name = 'hockey_rosters_v9.db'
        conn = sql.connect(db_name)
        self.skaters = pd.read_sql_query('select * from skaters', conn)
        self.skaters.start_date = pd.to_datetime(self.skaters.start_date)
        self.skaters.end_date = pd.to_datetime(self.skaters.end_date)
        self.names = pd.read_sql_query('select * from names', conn)
        self.league_strings = {'nhl': 'NHL', 'og': 'Olympics', 'khl': 'KHL', 'ahl': 'AHL', 'wc': 'Worlds', 'ohl': 'OHL', 'whl': 'WHL', 'qmjhl': 'QMJHL', 'ushl': 'USHL', 'usdp': 'USDP', 'ncaa': 'NCAA', 'wjc-20': 'World Juniors', 'wjc-18': 'WC-U18', 'whc-17': 'WHC-17', 'wcup': 'World Cup'}
        self.tournament_leagues = set(['og', 'wc', 'wjc-20', 'wjc-18', 'whc-17', 'wcup'])

    def get_league_display_string(self, league_str):
        return self.league_strings[league_str]

    def get_team_display_string(self, league_str, team_str):
        if league_str == 'og':
            return f"{team_str} Olympics"
        elif league_str == 'wc':
            return f"{team_str} Worlds"
        elif league_str == 'wcup':
            return f"{team_str} World Cup"
        else:
            return team_str

    def is_tournament(self, league_str):
        if league_str in self.tournament_leagues:
            return True
        return False

    def convert_tournament_dates(self, start_date, end_date):
        start_year = start_date.year
        end_year = end_date.year
        return (pd.to_datetime(f"{start_year}/12/15"), pd.to_datetime(f"{end_year}/1/15"))

    def compute_overlap_interval(self, start_date1, end_date1, start_date2, end_date2):
        start_dates = pd.DataFrame([pd.to_datetime(start_date1), pd.to_datetime(start_date2)])
        end_dates = pd.DataFrame([pd.to_datetime(end_date1), pd.to_datetime(end_date2)])
        start_interval = start_dates.max().iloc[0]
        end_interval = end_dates.min().iloc[0]
        num_seasons = max(1, end_interval.year-start_interval.year)
        if start_interval <= end_interval:
            return (start_interval, end_interval), num_seasons
        else:
            return False, 0

    def get_terms_from_player_id(self, player_id):
        return self.skaters.loc[self.skaters.link==player_id]

    def get_overlapping_player_terms(self, player1_id, player2_id=None):
        output = []
        player_rows = self.get_terms_from_player_id(player1_id)
        for index, term in player_rows.iterrows():
            if player2_id: # only return overlaps involving this specific second target player
                overlaps_a = self.skaters.loc[(self.skaters.league==term.league) & (self.skaters.team==term.team) & (self.skaters.link==player2_id) & (self.skaters.start_date>=term.start_date) & (self.skaters.start_date<term.end_date)]
                overlaps_b = self.skaters.loc[(self.skaters.league==term.league) & (self.skaters.team==term.team) & (self.skaters.link==player2_id) & (term.start_date>self.skaters.start_date) & (term.start_date<self.skaters.end_date)]
            else: # return overlaps involving any other player
                overlaps_a = self.skaters.loc[(self.skaters.link!=player1_id) & (self.skaters.league==term.league) & (self.skaters.team==term.team) & (self.skaters.start_date>=term.start_date) & (self.skaters.start_date<term.end_date)]
                overlaps_b = self.skaters.loc[(self.skaters.link!=player1_id) & (self.skaters.league==term.league) & (self.skaters.team==term.team) & (term.start_date>self.skaters.start_date) & (term.start_date<self.skaters.end_date)]
            overlaps = pd.concat([overlaps_a, overlaps_b])
            team_display_str = self.get_team_display_string(term.league, term.team)
            for teammate_id in overlaps.link.unique():
                teammate_rows = overlaps.loc[overlaps.link==teammate_id] # get all overlapping terms for this teammate (same as overlaps if player2_id was specified)
                for index, teammate_term in teammate_rows.iterrows():
                    overlap_term, season_count = self.compute_overlap_interval(term.start_date, term.end_date, teammate_term.start_date, teammate_term.end_date)
                    tooltip_str = f"{teammate_term.player}<br>{team_display_str}<br>{overlap_term[0].year}-{overlap_term[1].year}"
                    if self.is_tournament(term.league):
                        overlap_term = self.convert_tournament_dates(overlap_term[0], overlap_term[1])
                    else:
                        if season_count == 1:
                            tooltip_str += f" ({season_count} season)"
                        else:
                            tooltip_str += f" ({season_count} seasons)"
                    output.append((overlap_term[0].year, [teammate_rows.iloc[0].player, term.league, term.team, team_display_str, overlap_term[0].year, overlap_term[0].month-1, overlap_term[0].day, overlap_term[1].year, overlap_term[1].month-1, overlap_term[1].day, tooltip_str]))
        # sort all overlaps by first overlap year
        output.sort()
        sorted_output = []
        for year, data in output:
            sorted_output.append({"player": data[0], "league": self.get_league_display_string(data[1]), "team": data[2], "team_display": data[3], "year1": data[4], "month1": data[5], "day1": data[6], "year2": data[7], "month2": data[8], "day2": data[9], "tooltip_str": data[10]})
        return sorted_output
    
    def query_roster(self, player_id, team, season):
        output = []
        # set up season start/end timestamps
        years = season.split("-")
        season_start = pd.to_datetime(f"{years[0]}/9/1")
        season_end = pd.to_datetime(f"{years[1]}/6/30")
        # get all players from (team, year) roster
        teammates_a = self.skaters.loc[(self.skaters.league=="nhl") & (self.skaters.team==team) & (self.skaters.link!=player_id) & (self.skaters.start_date >= season_start) & (self.skaters.start_date<season_end)]
        teammates_b = self.skaters.loc[(self.skaters.league=="nhl") & (self.skaters.team==team) & (self.skaters.link!=player_id) & (season_start>self.skaters.start_date) & (season_start<self.skaters.end_date)]
        teammates = pd.concat([teammates_a,teammates_b])
        teammate_ids = teammates.link.unique()
        output = {"before": [], "during": [], "after": []}
        for potential_teammate_id in teammate_ids:
            overlap = self.get_overlapping_player_terms(player_id, potential_teammate_id)
            if len(overlap) > 0:
                playername = self.get_player_name_from_id(potential_teammate_id)
                formatted_data = {"before": [], "after": [], "during": []}
                for term in overlap:
                    relationships = self.is_before_after_during_season(season, term['year1'], term['year2'])
                    for relationship in relationships:
                        formatted_data[relationship].append(f"{term['team']} ({term['league']}, {term['year1']}-{term['year2']})")
                for relationship in formatted_data:
                    if len(formatted_data[relationship]) > 0:
                        player_data = {"playername": playername, "data": ", ".join(formatted_data[relationship])}
                        output[relationship].append((playername, player_data))
        #output.sort()
        sorted_output = {"before": [], "after": [], "during": []}
        for relationship in output:
            output[relationship].sort()
            for name, data in output[relationship]:
                sorted_output[relationship].append(data)
        return sorted_output

    def is_before_after_during_season(self, season, start_year, end_year):
        season_years = season.split("-")
        season_start_year = int(season_years[0])
        season_end_year = int(season_years[1])
        relationship = []
        if start_year < season_start_year:
            relationship.append("before")
        if end_year > season_end_year:
            relationship.append("after")
        if start_year <= season_start_year and end_year >= season_end_year:
            relationship.append("during")
        return relationship

    def get_player_name_from_id(self, player_id):
        terms = self.get_terms_from_player_id(player_id)
        return terms.iloc[0].player # player_id is unique, it doesn't matter which row we use for the name

    def player_to_description(self, player_row):
        year1 = pd.to_datetime(player_row.start_date).year
        year2 = pd.to_datetime(player_row.end_date).year
        return f"most recently {player_row.team} ({year1}-{year2})"

    # for all multiple possible players for a given name input, 
    # list out all options, each with an attached description of the most recent NHL team they played for 
    def format_multiple_options(self, links):
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

    def strip_accents(self, text):
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
    def retrieve_player_link(self, name):
        tgt_name = self.strip_accents(name.lower())
        names = self.names.loc[self.names.norm_name==tgt_name]
        unique_ids = set()
        for index, name_row in names.iterrows():
            ids = self.skaters.loc[self.skaters.player==name_row.orig_name]
            unique_ids.update(list(ids.link.unique()))
        if len(unique_ids) == 1:
            return 1, (names.iloc[0].orig_name, list(unique_ids)[0])
        elif len(unique_ids) == 0:
            return 0, name
        else:
            # format output
            output = self.format_multiple_options(ids)
            return len(output), output
