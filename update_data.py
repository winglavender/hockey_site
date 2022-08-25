import pandas as pd
import scraper
import sqlite3 as sql
import sys
from os.path import exists
import unicodedata
import time
import re
from datetime import date
from season_calculator import SeasonCalculator

class DBBuilder:

    def __init__(self):
        self.today = date.today()
        self.season_calc = SeasonCalculator(self.today)
        self.transfers = None
        self.ahl_affiliates = None
        self.postseasons = None
        self.nhl_players = None
        self.earliest_transfer_date = "2000/08/01" # this is the earliest transfer date that exists in EliteProspects
        self.scrape_start_year = 2000 # this is the earliest year we will use to scrape data, could be set earlier at the expense of time/space
        # get today's date
        self.today_str = self.today.strftime("%Y%m%d")

    def format_nhl_players(self, db_filename):
        self.ahl_affiliates = {
            'Anaheim Ducks': 'San Diego Gulls',
            'Arizona Coyotes': 'Tucson Roadrunners',
            'Boston Bruins': 'Providence Bruins',
            'Buffalo Sabres': 'Rochester Americans',
            'Calgary Flames': 'Stockton Heat',
            'Carolina Hurricanes': 'Chicago Wolves',
            'Chicago Blackhawks': 'Rockford IceHogs',
            'Colorado Avalanche': 'Colorado Eagles',
            'Columbus Blue Jackets': 'Cleveland Monsters',
            'Dallas Stars': 'Texas Stars',
            'Detroit Red Wings': 'Grand Rapids Griffins',
            'Edmonton Oilers': 'Bakersfield Condors',
            'Florida Panthers': 'Charlotte Checkers',
            'Los Angeles Kings': 'Ontario Reign',
            'Minnesota Wild': 'Iowa Wild',
            'Montréal Canadiens': 'Laval Rockets',
            'Nashville Predators': 'Milwaukee Admirals',
            'New Jersey Devils': 'Utica Comets',
            'New York Islanders': 'Bridgeport Islanders',
            'New York Rangers': 'Hartford Wolf Pack',
            'Ottawa Senators': 'Belleville Senators',
            'Philadelphia Flyers': 'Lehigh Valley Phantoms',
            'Pittsburgh Penguins': 'Wilkes-Barre/Scranton Penguins',
            'San Jose Sharks': 'San Jose Barracuda',
            'Seattle Kraken': 'Charlotte Checkers',
            'St. Louis Blues': 'Springfield Thunderbirds',
            'Tampa Bay Lightning': 'Syracuse Crunch',
            'Toronto Maple Leafs': 'Toronto Marlies',
            'Vancouver Canucks': 'Abbotsford Canucks',
            'Vegas Golden Knights': 'Henderson Silver Knights',
            'Washington Capitals': 'Hershey Bears',
            'Winnipeg Jets': 'Manitoba Moose'
        }
        skaters, goalies, transfers, postseasons = self.read_from_raw_db(db_filename)
        #conn = sql.connect(db_filename)
        #skaters = pd.read_sql_query('select * from skaters', conn)
        #goalies = pd.read_sql_query('select * from goalies', conn)
        #self.transfers = pd.read_sql_query('select * from transfers', conn)
        self.transfers = transfers
        self.transfers.date = pd.to_datetime(self.transfers.date)
        #self.postseasons = pd.read_sql_query('select * from postseasons', conn)
        self.postseasons = postseasons
        # drop unused columns and join all players in one table
        skaters_temp = skaters.copy()
        skaters_temp = skaters_temp.drop(columns=["gp", "g", "a", "tp", "ppg", "pim", "+/-", "position"])
        goalies_temp = goalies.copy()
        goalies_temp = goalies_temp.drop(columns=["gp", "gaa", "sv%"])
        players = pd.concat([skaters_temp, goalies_temp])
        players.reset_index(drop=True, inplace=True)
        # drop non-NHL players
        nhl_ids = players.loc[players.league=="nhl"].link.unique()
        self.nhl_players = players.loc[players.link.isin(nhl_ids)]
        self.nhl_players.reset_index(drop=True, inplace=True)

    def read_from_raw_db(self, db_filename):
        conn = sql.connect(db_filename)
        skaters = pd.read_sql_query('select * from skaters', conn)
        skaters = skaters.drop(['level_0','index'], axis=1, errors='ignore')
        goalies = pd.read_sql_query('select * from goalies', conn)
        goalies = goalies.drop(['level_0','index'], axis=1, errors='ignore')
        transfers = pd.read_sql_query('select * from transfers', conn)
        transfers = transfers.drop(['level_0','index'], axis=1, errors='ignore')
        postseasons = pd.read_sql_query('select * from postseasons', conn)
        postseasons = postseasons.drop(['level_0','index'], axis=1, errors='ignore')
        return skaters, goalies, transfers, postseasons

    def id_adjacent_team_rows(self, team_rows):
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

    def build_player_timeline(self, rows, dedup_name=None):
        rows['start_date'] = 0
        rows['end_date'] = 0
        player = rows.iloc[0].player
        if dedup_name:
            player = dedup_name  # use the unique string representation of this player's name
        player_id = rows.iloc[0].link
        player_timeline = []
        # expand trade candidates with AHL teams
        player_trade_candidates = self.transfers.loc[self.transfers.link==player_id]
        ahl_rows = []
        for _, row in player_trade_candidates.iterrows():
            if row.from_team in self.ahl_affiliates:
                # new row: we remove the to_team because we don't want to increase the number of trades for that team
                ahl_rows.append([row.date, self.ahl_affiliates[row.from_team], "", row.link, row.league, row.player]) # TODO this will be a list of possible AHL affiliates
            if row.to_team in self.ahl_affiliates:
                # new row: we remove the from_team because we don't want to increase the number of trades for that team
                ahl_rows.append([row.date, "", self.ahl_affiliates[row.to_team], row.link, row.league,
                                 row.player])  # TODO this will be a list of possible AHL affiliates
                # new_row = {'date': row.date, 'from_team': row.from_team, 'to_team': self.ahl_affiliates[row.to_team] , 'link': row.link, 'league': row.league, 'player': row.player}
                # player_trade_candidates.append(new_row, ignore_index=True)
        ahl_df = pd.DataFrame(ahl_rows, columns=['date', 'from_team', 'to_team', 'link', 'league', 'player'])
        player_trade_candidates = player_trade_candidates.append(ahl_df)
        # condense adjacent seasons for the same team
        for league in rows.league.unique():
            league_rows = rows.loc[rows.league == league]
            for team in league_rows.team.unique():
                team_rows = rows.loc[(rows.team == team) & (rows.league == league)]
                # check for tournament special cases: no merging, each row becomes its own row in the timeline
                if league in ["wc", "wjc-20", "wjc-18", "og", "wcup", "whc-17", "nhl-asg"]:
                    seasons = team_rows['season'].tolist()
                    for season_idx, season in enumerate(seasons):
                        player_timeline.append([player, team, league, self.season_calc.season_dates[season][0],
                                                self.season_calc.season_dates[season][1], player_id])
                    continue
                adjacent_team_rows = self.id_adjacent_team_rows(team_rows)
                # for a list of adjacent seasons on a roster, we will add at least 1 tenure row to the player timeline
                for adj_season_rows in adjacent_team_rows:
                    # select all trades that occurred in this time period
                    term_start_date = self.season_calc.get_season_dates(adj_season_rows.iloc[0].season)[0]
                    term_start_season = adj_season_rows.iloc[0].season
                    term_end_date_soft = self.season_calc.get_season_dates(adj_season_rows.iloc[-1].season)[1]
                    term_end_date = self.season_calc.get_season_dates(adj_season_rows.iloc[-1].season)[2]
                    prev_term_offseason_start = self.season_calc.get_season_dates(self.season_calc.get_prev_season(adj_season_rows.iloc[0].season))[1]
                    # removed league equality check on the following query because it restricted trades to NHL teams only (even when the trade involved another team in AHL, NCAA etc)
                    # trade_candidates = self.transfers.loc[(self.transfers['date'] >= prev_term_offseason_start) & (self.transfers['date'] <= term_end_date) & (self.transfers.link==player_id) & ((self.transfers.to_team==team) | (self.transfers.from_team==team))]
                    trade_candidates = player_trade_candidates.loc[(player_trade_candidates['date'] >= prev_term_offseason_start) & (player_trade_candidates['date'] <= term_end_date) & ((player_trade_candidates.to_team==team) | (player_trade_candidates.from_team==team))]
                    # if no trades, create a new tenure row
                    if len(trade_candidates) == 0:
                        player_timeline.append([player, team, league, term_start_date, term_end_date_soft, player_id])
                    else: # iterate over trade candidates to build tenure rows
                        chronological_trades = trade_candidates.sort_values(by='date')
                        currently_on_team = True
                        current_start_date = term_start_date
                        left_team_season = 0
                        trade_idx = 0
                        for _, trade in chronological_trades.iterrows():
                            trade_season = self.season_calc.get_season_from_date(trade.date)[0]
                            if trade.from_team == trade.to_team:
                                # ignore extensions
                                continue
                            if trade_idx == 0 and trade_season == term_start_season:
                                # they were traded to the team in this season, so they started the season not on the team
                                currently_on_team = False
                            if team == trade.to_team and trade_season == self.season_calc.get_prev_season(term_start_season):
                                # they were traded to this team in the offseason before this season (see Dylan Strome, Johnny Gaudreau)
                                current_start_date = trade.date
                                currently_on_team = True
                            elif team == trade.to_team and not currently_on_team:
                                if left_team_season != 0 and left_team_season != self.season_calc.get_prev_season(trade_season) and left_team_season != trade_season:
                                    # if the last trade away was NOT last season or this season, then there's a missing transfer back (see lockouts)
                                    # we need to add a new tenure row for that span of time up to end of last season (assume they're traded BACK to this team in this season)
                                    last_season_end = self.season_calc.get_season_dates(
                                        self.season_calc.get_prev_season(
                                            self.season_calc.get_season_from_date(trade.date)[0]))[1]
                                    if current_start_date < last_season_end:
                                        # this is a hack
                                        player_timeline.append(
                                            [player, team, league, current_start_date, last_season_end, player_id])
                                        # print("ERROR")
                                        # print(player, team, league, current_start_date, last_season_end, player_id)
                                # beginning of a new tenure with this team
                                current_start_date = trade.date
                                currently_on_team = True
                            elif team == trade.to_team and currently_on_team:
                                # Tuukka Rask case: was UFA, not signed, rejoined this team in the middle of the season
                                # we need to add the last tenure (we know it exists and hasn't already been added because currently_on_team is True)
                                # TODO actually Tuukka Rask doesn't fall into this category because of the **** lockout, who does?
                                last_season_end = self.season_calc.get_season_dates(self.season_calc.get_prev_season(self.season_calc.get_season_from_date(trade.date)[0]))[1]
                                if current_start_date < last_season_end:
                                    # print("ERROR")
                                    # print(player, team, league, current_start_date, last_season_end, player_id)
                                    # this is a hack
                                    player_timeline.append([player, team, league, current_start_date, last_season_end, player_id])
                            elif team == trade.from_team and currently_on_team:
                                # a normal trade from this team: add a tenure row for the tenure up to this point
                                if current_start_date < trade.date - pd.to_timedelta(1, unit='d'):
                                    # print("ERROR")
                                    # print(player, team, league, current_start_date, trade.date - pd.to_timedelta(1, unit='d'), player_id)
                                    # this is a hack :(
                                    player_timeline.append([player, team, league, current_start_date, trade.date - pd.to_timedelta(1, unit='d'), player_id])
                                # set start date to beginning of next season in the case that there is no trade back (everyone e.g. Claude Giroux after the lockout in 2012)
                                # if there's a following trade back, this date won't be used
                                current_start_date = self.season_calc.get_season_dates(self.season_calc.get_next_season(self.season_calc.get_season_from_date(trade.date)[0]))[0]
                                left_team_season = self.season_calc.get_season_from_date(trade.date)[0]
                                currently_on_team = False
                            elif team == trade.from_team and not currently_on_team:
                                # this happens when the trade back to this team wasn't properly recorded (Claude Giroux and lockout)
                                # current_start_date should have already been set to the next season following the previous trade away
                                if current_start_date < trade.date - pd.to_timedelta(1, unit='d'):
                                    # this is a hack :(
                                    player_timeline.append([player, team, league, current_start_date,
                                                        trade.date - pd.to_timedelta(1, unit='d'), player_id])
                                current_start_date = self.season_calc.get_season_dates(self.season_calc.get_next_season(
                                    self.season_calc.get_season_from_date(trade.date)[0]))[0]
                                left_team_season = self.season_calc.get_season_from_date(trade.date)[0]
                                currently_on_team = False
                            trade_idx += 1
                        # if this tenure ended without a trade, add it
                        # OR we ended with a trade away but the player clearly came back in the following season because they remained on the roster (lockouts etc, see Claude Giroux)
                        if currently_on_team or left_team_season != adj_season_rows.iloc[-1].season:
                            if current_start_date > term_end_date_soft:
                                # off-season trade (back?) to this team for players that aren't listed on next season's roster yet
                                term_end = term_end_date
                            else:
                                term_end = term_end_date_soft
                            player_timeline.append([player, team, league, current_start_date, term_end, player_id])
                            # if current_start_date > term_end:
                            #     print("ERROR")
                            #     print(player, team, league, current_start_date, term_end, player_id)
        return player_timeline

    def process_player(self, playername):
        #print(self.valid_trades)
        player_rows = self.nhl_players.loc[self.nhl_players.player==playername]
        #print(player_rows)
        rows = self.build_player_timeline(player_rows)
        pd_rows= pd.DataFrame(rows, columns=['player', 'team', 'league', 'start_date', 'end_date', 'link'])
        print(pd_rows)
        return pd_rows

    def full_update(self):
        db_filename = f"data/hockey_rosters_{self.today_str}_raw.db"
        if exists(db_filename):
            print(f"Error: file {db_filename} already exists.")
            return
        # scrape data and save to raw db
        conn = sql.connect(db_filename)
        # get new league data
        leagues = ["nhl", "ahl", "ohl", "ushl", "khl", "wc", "wjc-20", "wjc-18", "mhl", "og", "qmjhl", "whl", "ncaa", "usdp", "whc-17", "wcup", "ushs-prep", "russia", "russia3", "shl", "liiga", "u20-sm-liiga", "u18-sm-sarja", "j20-superelit", "j18-allsvenskan", "elitserien"]
        # define seasons
        start_year = self.scrape_start_year
        end_year = int(self.season_calc.get_latest_season().split("-")[1])
        years = []
        for year in range(start_year, end_year):
            years.append(f"{year}-{year+1}")

        # get ASG data
        asg_data = scraper.get_asg()
        # get skaters
        skaters = scraper.get_skaters(leagues, years)
        skaters = pd.concat([skaters, asg_data], axis=0, ignore_index=True) # add ASG data to skaters table
        skaters.to_sql('skaters', conn) # save all skater info
        # get goalies
        goalies = scraper.get_goalies(leagues, years)
        goalies.to_sql('goalies', conn) # save all goalie info

        # scrape transfer data and save to raw db
        transfers = scraper.get_transfers("nhl", self.earliest_transfer_date)
        transfers.to_sql('transfers', conn)


        # scrape NHL postseasons and save to raw db
        postseasons = scraper.get_postseasons()
        postseasons.to_sql('postseasons', conn)
        return db_filename

    def is_valid_season(self, proposed_season):
        proposed_year1 = int(proposed_season.split("-")[0])
        if proposed_year1 < self.scrape_start_year:
            return False
        return True

    def add_asg(self, db_filename):
        # read raw db
        skaters, goalies, transfers, postseasons = self.read_from_raw_db(db_filename)
        skaters = skaters[skaters.team.str.contains("All Star Game")==False] # don't duplicate ASG data
        new_db_filename = db_filename.replace(".db", "_plusASG.db")
        # get ASG data
        asg_data = scraper.get_asg()
        asg_data = asg_data[asg_data['season'].apply(self.is_valid_season)]
        skaters = pd.concat([skaters, asg_data], axis=0, ignore_index=True) # add ASG data to skaters table
        skaters = skaters.drop(['level_0','index'], axis=1, errors='ignore')
        new_conn = sql.connect(new_db_filename)
        skaters.to_sql('skaters', new_conn) # save all skater info
        goalies.to_sql('goalies', new_conn) # save all goalie info
        transfers.to_sql('transfers',new_conn)
        postseasons.to_sql('postseasons',new_conn)

    # on a ~monthly basis, scrape new transfers and any updates to postseason
    def scrape_transfer_updates(self, db_filename):
        season = self.season_calc.get_latest_season()
        # check in and out db file existence
        out_db_filename = f"data/hockey_rosters_{self.today_str}_raw.db"
        if exists(out_db_filename):
            print(f"Error: file {out_db_filename} already exists.")
            return
        if not exists(db_filename):
            print(f"Error: file {db_filename} doesn't exist.")
        # load data from raw db, drop roster data from this season
        conn = sql.connect(db_filename)
        skaters = pd.read_sql_query('select * from skaters', conn)
        skaters = skaters.drop(['level_0','index'], axis=1, errors='ignore')
        skaters_prev = skaters[skaters.season!=season]
        goalies = pd.read_sql_query('select * from goalies', conn)
        goalies = goalies.drop(['level_0','index'], axis=1, errors='ignore')
        goalies_prev = goalies[goalies.season!=season]
        transfers = pd.read_sql_query('select * from transfers', conn)
        transfers = transfers.drop(['level_0','index'], axis=1, errors='ignore')
        # scrape roster info from this season
        new_skaters = scraper.get_skaters(["nhl","ahl"], [season])
        new_goalies = scraper.get_goalies(["nhl","ahl"], [season])
        all_skaters = pd.concat([skaters_prev, new_skaters])
        all_goalies = pd.concat([goalies_prev, new_goalies])
        # scrape new transfers
        prev_date_tmp = db_filename[-15:-7]
        prev_date = prev_date_tmp[0:4] + "/" + prev_date_tmp[4:6] + "/" + prev_date_tmp[6:8]
        new_transfers = scraper.get_transfers("nhl", prev_date)
        pd.set_option('display.max_rows', 500)
        print(new_transfers)
        all_transfers = pd.concat([transfers, new_transfers])
        all_transfers.drop_duplicates()
        # scrape NHL postseasons
        postseasons = scraper.get_postseasons()
        # write to new db
        conn_out = sql.connect(out_db_filename)
        all_skaters.to_sql('skaters', conn_out)
        all_goalies.to_sql('goalies', conn_out)
        all_transfers.to_sql('transfers', conn_out)
        postseasons.to_sql('postseasons', conn_out)

    def process_db(self, db_filename):
        # check in and out db file existence
        out_db_filename = db_filename.replace('raw', 'formatted')
        if exists(out_db_filename):
            print(f"Error: file {out_db_filename} already exists.")
            return
        # read in duplicate names
        dup_name_filename = db_filename.replace(".db", "_dup_names_disambig.txt")
        dup_names_link_to_str = {}
        name_map_list = [] # for names routing table
        with open(dup_name_filename, 'r') as in_file:
            for line in in_file:
                tmp = line.strip().split(",")
                dedup_name_str = f"{tmp[0]} ({tmp[3]})" 
                dup_names_link_to_str[tmp[2]] = dedup_name_str
                name_map_list.append([dedup_name_str, tmp[1]])
        # process raw data into app format and save to new db
        all_rows = []
        for player in self.nhl_players.link.unique():
            dedup_playername = None 
            if player in dup_names_link_to_str:
                dedup_playername = dup_names_link_to_str[player]
            player_rows = self.nhl_players.loc[self.nhl_players.link == player]
            all_rows.extend(self.build_player_timeline(player_rows, dedup_playername))
        processed_players = pd.DataFrame(all_rows,
                                         columns=['player', 'team', 'league', 'start_date', 'end_date', 'link'])
        # update names routing table with normalized names
        unique_names = processed_players.player.unique()
        for name in unique_names:
            lower = name.lower()
            name_map_list.append([name, lower])
            no_diacritics = strip_accents(lower)
            if lower != no_diacritics:
                name_map_list.append([name, no_diacritics])
        names = pd.DataFrame(name_map_list, columns =['orig_name', 'norm_name'])
        # save output to new db
        conn_out = sql.connect(out_db_filename)
        processed_players.to_sql('skaters', conn_out)
        pd.set_option('display.max_columns', None)
        names.to_sql('names', conn_out)
        self.postseasons.to_sql('postseasons', conn_out)

    def dump_duplicate_names(self, db_filename):
        out_filename = db_filename.replace(".db", "_dup_names.txt")
        unique_people_links = self.nhl_players.link.unique()
        unique_people_names = []
        for link in unique_people_links:
            unique_people_names.append(self.nhl_players.loc[self.nhl_players.link==link].iloc[0].player)
        norm_names = [strip_accents(name.lower()) for name in unique_people_names]
        norm_names_seen = {}
        output_duplicates = []
        for idx, name in enumerate(norm_names):
            if name not in norm_names_seen:
                norm_names_seen[name] = []
            norm_names_seen[name].append(idx)
        for name in norm_names_seen:
            if len(norm_names_seen[name]) > 1:
                for idx in norm_names_seen[name]:
                    output_duplicates.append((name, unique_people_names[idx], unique_people_links[idx]))
        with open(out_filename, 'w') as out_file:
            for norm_name, name, link in output_duplicates:
                out_file.write(f"{name},{norm_name},{link}\n")


def strip_accents(text):
    try:
        text = unicode(text, 'utf-8')
    except NameError: # unicode is a default on python 3
        pass
    text = unicodedata.normalize('NFD', text)\
           .encode('ascii', 'ignore')\
           .decode("utf-8")
    text = text.replace("-", " ") # handle hyphenated names
    text = re.sub(r'[^\w\s]', '', text) # remove punctuation (e.g. "J.T. Brown")
    return str(text)


if __name__ == "__main__":
    function = sys.argv[1]
    start = time.time()
    db_builder = DBBuilder()
    if len(sys.argv) >= 3:
        db_filename = sys.argv[2]
        if not exists(db_filename):
            print(f"Error: file {db_filename} doesn't exist.")
            sys.exit(0)
    if function == "scrape_full":  # run whenever new guys are introduced, probably once a year at the beginning of the season, runs a full scrape from scratch
        db_filename = db_builder.full_update()
    elif function == "dump_duplicate_names": # run after a new full scrape, because new guys means possible new overlapping names
        db_builder.format_nhl_players(db_filename)
        #db_builder.read_from_db(db_filename)
        db_builder.dump_duplicate_names(db_filename)
    elif function == "process": # run after full_scrape and dump_duplicate_names and after manually adding disambiguating name strings
        db_builder.format_nhl_players(db_filename)
        #db_builder.read_from_db(db_filename)
        db_builder.process_db(db_filename)#'data/hockey_rosters_20220804_raw.db')
    elif function == "scrape_updates":  # run weekly/monthly to get new trades/transfers (will not grab new guys)
        db_builder.scrape_transfer_updates(db_filename)
        db_builder.process_db(db_filename)
    elif function == "add_asg":
        #db_builder.read_from_db(db_filename)
        db_builder.add_asg(db_filename)
    elif function == "test_player_process":
        player_name = sys.argv[3]
        db_builder.format_nhl_players(db_filename)
        db_builder.process_player(player_name)
    elif function == "test_scraper":
        players = scraper.get_asg()
    else:
        print("Specify a valid function to run (full/partial).")
    end = time.time()
    execution_time = end - start
    print("--- %s minutes ---" % (execution_time / 60))
