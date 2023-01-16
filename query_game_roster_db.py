import pandas as pd
import sqlite3 as sql
import numpy as np
import time
pd.set_option('display.max_columns',None)


class game_roster_db():

    def __init__(self, name_db):
        self.name_db = name_db
        self.latest_date = pd.to_datetime('2022-12-06')  # update this to the last accurate game data I have (probably the day before the scrape date)
        data_root_name = 'game_records_20002023_20221207' # update this for new data files
        self.games = pd.read_csv(f"{data_root_name}_games.zip", compression='zip')#, dtype={'seasonId': 'int', 'seasonName': 'str', 'homeTeamGoals': 'int'})
        self.games['gameDateTimestamp'] = pd.to_datetime(self.games['gameDate'])
        self.games = self.games[self.games.gameDateTimestamp < self.latest_date]
        self.players = pd.read_csv(f"{data_root_name}_players.zip", compression='zip')
        self.scratches = pd.read_csv(f"{data_root_name}_scratches.zip", compression='zip')
        self.game_player = pd.read_csv(f"{data_root_name}_game_player.zip", compression='zip', dtype={'assists': 'str', 'goals': 'str', 'powerPlayAssists': 'str'})
        self.team_names = {'Mighty Ducks of Anaheim': 'ANA', 'Anaheim Ducks': 'ANA', 'Arizona Coyotes': 'ARI', 'Atlanta Thrashers': 'ATL', 'Boston Bruins': 'BOS', 'Buffalo Sabres': 'BUF', 'Carolina Hurricanes': 'CAR', 'Columbus Blue Jackets': 'CBJ', 'Calgary Flames': 'CGY', 'Chicago Blackhawks': 'CHI', 'Colorado Avalanche': 'COL', 'Dallas Stars': 'DAL', 'Detroit Red Wings': 'DET', 'Edmonton Oilers': 'EDM', 'Florida Panthers': 'FLA', 'Los Angeles Kings': 'LAK', 'Minnesota Wild': 'MIN', 'Montreal Canadiens': 'MTL', 'Montréal Canadiens': 'MTL', 'New Jersey Devils': 'NJD', 'Nashville Predators': 'NSH', 'New York Islanders': 'NYI', 'New York Rangers': 'NYR', 'Ottawa Senators': 'OTT', 'Philadelphia Flyers': 'PHI', 'Phoenix Coyotes': 'PHX', 'Pittsburgh Penguins': 'PIT', 'Seattle Kraken': 'SEA', 'San Jose Sharks': 'SJS', 'St. Louis Blues': 'STL', 'Tampa Bay Lightning': 'TBL', 'Toronto Maple Leafs': 'TOR', 'Vancouver Canucks': 'VAN', 'Vegas Golden Knights': 'VGK', 'Winnipeg Jets': 'WPG', 'Washington Capitals': 'WSH'}
        self.game_types = [('PR', 'preseason'), ('R', 'regular season'), ('P', 'playoff'), ('A', 'All-Star')]


    def get_latest_date(self):
        return self.latest_date.date()

    def get_team_name_abbrev(self, team_name_full):
        if team_name_full in self.team_names:
            return self.team_names[team_name_full]
        else:
            return team_name_full

    def get_games_vs_team(self, player_id, team_name):
        player_games = self.get_player_games(player_id)
        team_games = self.get_team_games(team_name)
        # identify games where player played AGAINST the team
        player_games_vs = player_games[player_games['gameId'].isin(team_games['gameId'])] # games involving team_name
        player_games_vs = player_games_vs[player_games['team'] != team_name] # games where team_name is the opponent
        player_games_vs_info = player_games_vs.merge(self.games, on='gameId')
        player_games_vs_info['player1_win'] = player_games_vs_info['winningTeam'] == player_games_vs_info['team']
        player_games_vs_info['team_abbrev'] = player_games_vs_info['team'].apply(lambda x: self.get_team_name_abbrev(x))
        return player_games_vs_info

    def get_team_games(self, team_name):
        games = pd.concat([self.games.loc[self.games.awayTeam == team_name], self.games.loc[
            self.games.homeTeam == team_name]])
        return games

    def get_common_games(self, player1_id, player2_id):
        player1_games = self.get_player_games(player1_id)
        player2_games = self.get_player_games(player2_id)
        common_games = player1_games.merge(player2_games, suffixes=('_1', '_2'), on='gameId')
        common_games_info = common_games.merge(self.games, on='gameId')
        common_games_info['teammates'] = np.where(common_games_info['team_1']==common_games_info['team_2'],True,False)
        common_games_info['team_1_abbrev'] = common_games_info['team_1'].apply(lambda x: self.get_team_name_abbrev(x))
        common_games_info['team_2_abbrev'] = common_games_info['team_2'].apply(lambda x: self.get_team_name_abbrev(x))
        # compute whether p1's team won or not
        common_games_info['player1_win'] = common_games_info['winningTeam']==common_games_info['team_1']
        print(common_games_info)
        return common_games_info

    def get_results_html_vs_team(self, player1_id, team_name):
        start = time.time()
        vs_games = self.get_games_vs_team(player1_id, team_name)
        data = {}
        # get all summary game counts
        data['summary'] = self.get_summary_counts_vs_team(vs_games)
        # restructure game data for HTML output
        season_ids = vs_games.seasonId.unique()
        season_ids.sort()
        data['seasons'] = []
        for season_id in season_ids:
            # get season rows
            season_data = {}
            season_rows = vs_games.loc[vs_games.seasonId == season_id]
            season_name = season_rows.iloc[0].seasonName
            season_data['name'] = season_name
            season_data['summary'] = self.get_summary_counts_vs_team(season_rows)
            # add game data
            opponent_rows = season_rows
            opponent_rows_split = self.split_game_data_by_type(opponent_rows)
            season_data['opponent_games'] = opponent_rows_split
            data['seasons'].append(season_data)
        end = time.time()
        print(f"elapsed time: {end - start}")
        return data

    def get_results_html(self, player1_id, player2_id):
        start = time.time()
        common_games = self.get_common_games(player1_id, player2_id)
        data = {}
        # get all summary game counts
        data['summary'] = self.get_summary_counts(common_games)
        # restructure game data for HTML output
        season_ids = common_games.seasonId.unique()
        season_ids.sort()
        data['seasons'] = []
        for season_id in season_ids:
            # get season rows
            season_data = {}
            season_rows = common_games.loc[common_games.seasonId == season_id]
            season_name = season_rows.iloc[0].seasonName
            season_data['name'] = season_name
            season_data['summary'] = self.get_summary_counts(season_rows)
            # add game data
            teammate_rows = season_rows.loc[season_rows.teammates==True]
            teammate_rows_split = self.split_game_data_by_type(teammate_rows)
            opponent_rows = season_rows.loc[season_rows.teammates==False]
            opponent_rows_split = self.split_game_data_by_type(opponent_rows)
            season_data['teammate_games'] = teammate_rows_split
            season_data['opponent_games'] = opponent_rows_split
            data['seasons'].append(season_data)
        end = time.time()
        print(f"elapsed time: {end - start}")
        return data

    def split_game_data_by_type(self, game_rows):
        game_rows_split = []
        for game_type_id, display_name in self.game_types:
            type_data = {'display_name': display_name}
            rows = game_rows.loc[game_rows.gameType == game_type_id]
            if len(rows) == 0:
                continue
            type_data['games'] = rows.to_dict('records')
            type_data['games_record'] = self.get_win_loss_record(rows)
            game_rows_split.append(type_data)
        return game_rows_split

    def get_win_loss_record(self, game_rows):
        total = len(game_rows)
        wins = game_rows['player1_win'].sum()
        losses = total - wins
        return {'wins': wins, 'losses': losses}

    def get_summary_counts_vs_team(self, game_rows):
        data = {}
        data['total_games'] = len(game_rows)
        opponent_games = game_rows
        data['opponent_games'] = len(opponent_games)
        data['opponent_games_record'] = self.get_win_loss_record(opponent_games)
        data['opponent_games_preseason'] = len(opponent_games.loc[opponent_games.gameType == 'PR'])
        data['opponent_games_preseason_record'] = self.get_win_loss_record(opponent_games.loc[opponent_games.gameType == 'PR'])
        data['opponent_games_regular'] = len(opponent_games.loc[opponent_games.gameType == 'R'])
        data['opponent_games_regular_record'] = self.get_win_loss_record(opponent_games.loc[opponent_games.gameType == 'R'])
        data['opponent_games_playoff'] = len(opponent_games.loc[opponent_games.gameType == 'P'])
        data['opponent_games_playoff_record'] = self.get_win_loss_record(opponent_games.loc[opponent_games.gameType == 'P'])
        data['opponent_games_allstar'] = len(opponent_games.loc[opponent_games.gameType == 'A'])
        data['opponent_games_allstar_record'] = self.get_win_loss_record(opponent_games.loc[opponent_games.gameType == 'A'])
        return data

    def get_summary_counts(self, game_rows):
        data = {}
        data['total_games'] = len(game_rows)
        teammate_games = game_rows.loc[game_rows.teammates == True]
        data['teammate_games'] = len(teammate_games)
        data['teammate_games_record'] = self.get_win_loss_record(teammate_games)
        data['teammate_games_preseason'] = len(teammate_games.loc[teammate_games.gameType == 'PR'])
        data['teammate_games_preseason_record'] = self.get_win_loss_record(teammate_games.loc[teammate_games.gameType == 'PR'])
        data['teammate_games_regular'] = len(teammate_games.loc[teammate_games.gameType == 'R'])
        data['teammate_games_regular_record'] = self.get_win_loss_record(teammate_games.loc[teammate_games.gameType == 'R'])
        data['teammate_games_playoff'] = len(teammate_games.loc[teammate_games.gameType == 'P'])
        data['teammate_games_playoff_record'] = self.get_win_loss_record(teammate_games.loc[teammate_games.gameType == 'P'])
        data['teammate_games_allstar'] = len(teammate_games.loc[teammate_games.gameType == 'A'])
        data['teammate_games_allstar_record'] = self.get_win_loss_record(teammate_games.loc[teammate_games.gameType == 'A'])
        opponent_games = game_rows.loc[game_rows.teammates == False]
        data['opponent_games'] = len(opponent_games)
        data['opponent_games_record'] = self.get_win_loss_record(opponent_games)
        data['opponent_games_preseason'] = len(opponent_games.loc[opponent_games.gameType == 'PR'])
        data['opponent_games_preseason_record'] = self.get_win_loss_record(opponent_games.loc[opponent_games.gameType == 'PR'])
        data['opponent_games_regular'] = len(opponent_games.loc[opponent_games.gameType == 'R'])
        data['opponent_games_regular_record'] = self.get_win_loss_record(opponent_games.loc[opponent_games.gameType == 'R'])
        data['opponent_games_playoff'] = len(opponent_games.loc[opponent_games.gameType == 'P'])
        data['opponent_games_playoff_record'] = self.get_win_loss_record(opponent_games.loc[opponent_games.gameType == 'P'])
        data['opponent_games_allstar'] = len(opponent_games.loc[opponent_games.gameType == 'A'])
        data['opponent_games_allstar_record'] = self.get_win_loss_record(opponent_games.loc[opponent_games.gameType == 'A'])
        return data

    def display_results(self, player1_name, player2_name, game_rows):

        print(f"{player1_name} and {player2_name}") 
        print()
        print(f"{len(game_rows)} games in common:")
        print()
        print(
            f"as opponents: {len(game_rows.loc[game_rows.teammates == False])} games")
        if len(game_rows.loc[game_rows.teammates == False]) > 0:
            print(f"{len(game_rows.loc[(game_rows.teammates == False) & (game_rows.gameType == 'R')])} regular season,")
            print(f"{len(game_rows.loc[(game_rows.teammates == False) & (game_rows.gameType == 'PR')])} preseason,")
            print(f"{len(game_rows.loc[(game_rows.teammates == False) & (game_rows.gameType == 'P')])} playoffs,")
            print(f"{len(game_rows.loc[(game_rows.teammates == False) & (game_rows.gameType == 'A')])} All-Star game,")
        print()
        print(f"as teammates: {len(game_rows.loc[game_rows.teammates==True])} games")
        if len(game_rows.loc[game_rows.teammates == True]) > 0:
            print(f"{len(game_rows.loc[(game_rows.teammates==True)&(game_rows.gameType=='R')])} regular season,")
            print(f"{len(game_rows.loc[(game_rows.teammates==True)&(game_rows.gameType=='PR')])} preseason,")
            print(f"{len(game_rows.loc[(game_rows.teammates==True)&(game_rows.gameType=='P')])} playoffs,")
            print(f"{len(game_rows.loc[(game_rows.teammates==True)&(game_rows.gameType=='A')])} All-Star game,")
        print()

        # print game-specific stats
        season_ids = game_rows.seasonId.unique()
        season_ids.sort()
        for season_id in season_ids:
            # get rows
            season_rows = game_rows.loc[game_rows.seasonId==season_id]
            season_name = game_rows.iloc[0].seasonName
            print(season_name)
            for _, row in season_rows.iterrows():
                print(f"{row.gameDate}\t{row.awayTeam} @ {row.homeTeam}\t{row.gameId}")
                print(f"{player1_name} ({row.team_1})\t{player2_name} ({row.team_2})")
                print(f"g: {row.s_goals_1}\t\t\t\t\t{row.s_goals_2}")
                print(f"a: {row.s_assists_1}\t\t\t\t\t{row.s_assists_2}")
                print(f"toi: {row.s_timeOnIce_1}\t\t\t\t{row.s_timeOnIce_2}")
                print()
            print("\n**\n")

    def display_games(self, game_player_rows):
        if len(game_player_rows) == 0:
            print("None")

        for _, row in game_player_rows.iterrows():
            game_id = row.gameId
            game_data = self.games.loc[self.games.gameId==game_id].iloc[0]
            print(f"{game_data.gameDate}\t{game_data.seasonName}\t{game_data.awayTeam} @ {game_data.homeTeam}\t{game_data.awayTeamGoals}-{game_data.homeTeamGoals}\t{game_data.venue}")

    def get_player_games(self, player_id):
        scratch_games_ids = self.scratches.loc[self.scratches.playerId==player_id].gameId.values.tolist()
        all_games = self.game_player.loc[self.game_player.playerId == player_id]
        games = all_games.loc[~all_games.gameId.isin(scratch_games_ids)]
        return games

    # given an input name, return the player id (EliteProspects url)
    # if there are multiple possible players, return all possibilities, each with a description
    def get_player_id(self, name):
        return self.name_db.get_possible_links(name, "nhl")


from query_name_db import name_db

if __name__ == "__main__":
    names_db = name_db()
    db = game_roster_db(names_db)
    print(db.get_player_id('Connor McDavid'))
    player1_id = '8478402'
    player2_id = '8477934'
    # common_games = db.get_common_games(player1_id, player2_id)
    # print(common_games)
    # db.get_team_games('Boston Bruins')
    # db.get_games_vs_team(8478402, 'Pittsburgh Penguins')
    print(db.get_results_html_vs_team(8478402, 'Pittsburgh Penguins'))
