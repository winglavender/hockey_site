from flask import Flask, render_template, request, session
from sqlalchemy import create_engine, text as sql_text
from flask_sqlalchemy import SQLAlchemy
from teammates_db import teammates_db
import os
import yaml
from pathlib import Path
root_dir = str(Path.cwd())
import time

app = Flask(__name__)
if os.getenv('PYANYWHERE'):
    print("running on pythonanywhere")
    app.config.update(SECRET_KEY = os.getenv("SECRET_KEY"))
    with open('hockey_site/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    config['root_dir'] = root_dir
    SQLALCHEMY_DATABASE_URI = "mysql://{username}:{password}@{hostname}/{databasename}".format(
    username=os.getenv("username"),# "hockeyteammates",
    password=os.getenv("password"),
    hostname=os.getenv("hostname"), #"hockeyteammates.mysql.pythonanywhere-services.com",
    databasename=os.getenv("databasename") #"hockeyteammates$default",
    )
    app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
    app.config["SQLALCHEMY_POOL_RECYCLE"] = 280 # specifically for pythonanywhere, this needs to be less than 300 seconds 
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {'pool_recycle': 280}
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    engine = SQLAlchemy(app)
else:
    print("running locally")
    app.config.from_pyfile('config.py')
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    config['root_dir'] = root_dir + "/.."
    config['filename_date'] = config['current_date'].replace("-", "")
    out_db = os.path.join(config['data_dir'], f"{config['filename_date']}.db")
    engine = create_engine(f"sqlite:///{out_db}")
print(root_dir)

# populate NHL team data
nhl_team_data = {'team_order': [], 'team_seasons': {}}
with open(config['root_dir'] + '/hockey_db_data/nhl_team_data.txt','r') as in_file:
    line_count = 1
    for line in in_file:
        if line_count == 1:
            # grab latest year
            end_year = int(line.strip())
        else:
            tokens = line.strip().split(",")
            start_year = int(tokens[1])
            seasons = []
            for i in range(end_year,start_year,-1):
                seasons.append(f"{i-1}-{i}")
            nhl_team_data['team_order'].append(tokens[0])
            nhl_team_data['team_seasons'][tokens[0]] = seasons
        line_count += 1
nhl_team_data['team_order'].sort()
nhl_team_data['team1_seasons'] = nhl_team_data['team_seasons'][nhl_team_data['team_order'][0]]
nhl_team_data['team2_seasons'] = nhl_team_data['team_seasons'][nhl_team_data['team_order'][1]]
# set up team pair seasons
nhl_team_data['team_pair_seasons'] = {}
for team1 in nhl_team_data['team_order']:
    seasons1 = nhl_team_data['team_seasons'][team1]
    nhl_team_data['team_pair_seasons'][team1] = {}
    for team2 in nhl_team_data['team_order']:
        seasons2 = nhl_team_data['team_seasons'][team2]
        if len(seasons1) < len(seasons2): # assumes all teams go up to current season (so comparing lengths of list is sufficient)
            season_list = seasons1
        else:
            season_list = seasons2
        nhl_team_data['team_pair_seasons'][team1][team2] = season_list

@app.route("/")
def home():
    session.clear()
    return render_template("index.html",nhl_team_data=nhl_team_data,update_date=config['current_date'])

# ONE PLAYER FUNCTIONS 
# one player: view teammates over career
@app.route("/one_player_career", methods=["GET","POST"])
def one_player_career():
    start_page = time.time()
    if request.method == "POST":
        end = time.time()
        print(f"start method elapsed time: {end-start_page}")
        session.clear()
        target = request.form['target'].strip()
        db = teammates_db(config, engine)
        output = db.get_links_from_name(target)
        end = time.time()
        print(f"get id elapsed time: {end-start_page}")
        session["task"] = "one_player_career"
        # if num_results == 1:
        if len(output) == 1:
            # output is unique player link
            data_asg, longest_name_asg, longest_name_no_asg, data_no_asg, data_length_asg, data_length_no_asg = db.get_overlapping_player_terms(output.iloc[0]['playerId'])
            end = time.time()
            print(f"get overlapping player terms elapsed time: {end-start_page}")
            career_data, start_date, end_date = db.get_player_career(output.iloc[0]['playerId'])
            end = time.time()
            print(f"get career elapsed time: {end-start_page}")
            return render_template('one_player_career.html', data_asg=data_asg, data_no_asg=data_no_asg, data_length_asg=data_length_asg, data_length_no_asg=data_length_no_asg, career_data=career_data, playername1=output.iloc[0]['playerName'], display_name_asg=longest_name_asg, display_name_no_asg=longest_name_no_asg, start_date=start_date, end_date=end_date)
        elif len(output) == 0:
            # output is player name searched
            return render_template('error_no_results_name.html', playername=target)
        else:
            # output contains list of options for player
            session["player_to_clarify"] = "player1"
            print(output.to_dict('records'))
            return render_template('options_1.html', data=output.to_dict('records'))
    else:
        return render_template('error.html')
    
# one player: view teammates on specific team roster
@app.route("/one_player_roster", methods=["GET", "POST"])
def one_player_roster():
    if request.method == "POST":
        session.clear()
        player = request.form['player'].strip()
        team = request.form['team']
        season = request.form['season']
        db = teammates_db(config, engine)
        session["task"] = "one_player_roster"
        session["team"] = team
        session["season"] = season
        output = db.get_links_from_name(player)
        if len(output) == 1:
            # we have a unique player id
            data, data_no_asg = db.get_one_player_roster(output.iloc[0]['playerId'], team, season)
            return render_template('one_player_roster.html', playername=output.iloc[0]['playerName'], team=team, season=season, data=data, data_no_asg=data_no_asg)
        elif len(output) == 0:
            return render_template('error_no_results_name.html', playername=player)
        elif len(output) > 1:
            # clarify player id
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output.to_dict('records'))
    else:
        return render_template('error.html')

# one player: games played vs a team 
@app.route("/one_player_team_games", methods=["GET", "POST"])
def one_player_team_games():
    if request.method == "POST":
        session.clear()
        player = request.form['player'].strip()
        team = request.form['team']
        db = teammates_db(config, engine)
        session["task"] = "one_player_team_games"
        session["team"] = team
        # num_results, target
        # output = db.get_player_id(player)
        output = db.get_links_from_name(player)
        if len(output) == 1:
            # we have a unique player id
            data = db.get_one_player_team_games(output.iloc[0]['playerId'], team)
                # int(float(output[0]['link'])), team)
            return render_template('one_player_team_games.html', playername=output.iloc[0]['playerName'], team=team, data=data, latest_game_date=db.get_latest_game_date().date())
        elif len(output) == 0:
            return render_template('error_no_results_name.html', playername=player)
        elif len(output) > 1:
            # clarify player id
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output.to_dict('records'))
    else:
        return render_template('error.html')
    
# TWO PLAYERS 
# two players: have they been teammates
@app.route("/two_players_results", methods=["GET","POST"])
def two_players_results():
    if request.method == "POST":
        session.clear()
        db = teammates_db(config, engine)
        player1 = request.form['player1'].strip()
        output1 = db.get_links_from_name(player1)
        player2 = request.form['player2'].strip()
        output2 = db.get_links_from_name(player2)
        session["task"] = "two_players_results"
        if len(output1) == 1 and len(output2) == 1:
            # we have unique player ids for both
            if output1.iloc[0]['playerId'] == output2.iloc[0]['playerId']:
                return render_template('error_two_players_same.html')
            data, _, _, data_no_asg, _, _ = db.get_overlapping_player_terms(output1.iloc[0]['playerId'], output2.iloc[0]['playerId'])
            return render_template('two_players_results.html', data=data, data_no_asg=data_no_asg, playername1=output1.iloc[0]['playerName'], playername2=output2.iloc[0]['playerName'])
        elif len(output1) == 0:
            return render_template('error_no_results_name.html', playername=player1)
        elif len(output2) == 0:
            return render_template('error_no_results_name.html', playername=player2)
        elif len(output1) > 1 and len(output2) == 1:
            # clarify player 1
            session["player2"], session["player2_id"] = (output2.iloc[0]['playerName'], output2.iloc[0]['playerId']) 
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output1.to_dict('records'))
        elif len(output2) > 1 and len(output1) == 1:
            # clarify player 2
            session["player1"], session["player1_id"] = (output1.iloc[0]['playerName'], output1.iloc[0]['playerId']) 
            session["player_to_clarify"] = "player2"
            return render_template('options_1.html', data=output2.to_dict('records'))
        else:
            # clarify both players
            print(output1.to_dict('records'))
            print(output2.to_dict('records'))
            return render_template('options_2.html', data1=output1.to_dict('records'), data2=output2.to_dict('records'))
    else:
        return render_template('error.html')
    
# two players: compare shared teammates
@app.route("/two_players_shared_teammates", methods=["GET", "POST"])
def two_players_shared_teammates():
    if request.method == "POST":
        session.clear()
        db = teammates_db(config, engine)
        player1 = request.form['player1'].strip()
        output1 = db.get_links_from_name(player1)
        player2 = request.form['player2'].strip()
        output2 = db.get_links_from_name(player2)
        session["task"] = "two_players_shared_teammates"
        if len(output1) == 1 and len(output2) == 1:
            # we have unique player ids for both
            if output1.iloc[0]['playerId'] == output2.iloc[0]['playerId']:
                return render_template('error_traverse_same_player.html')
            team_overlaps_sort_date, team_overlaps_sort_date_no_asg, teammates_asg, len_teammates_asg, teammates_no_asg, len_teammates_no_asg = db.get_two_players_shared_teammates(output1.iloc[0]['playerId'], output1.iloc[0]['playerName'], output2.iloc[0]['playerId'], output2.iloc[0]['playerName'])
            return render_template('two_players_shared_teammates.html', data=teammates_asg, data_no_asg=teammates_no_asg, team_data=team_overlaps_sort_date, data_len_asg=len_teammates_asg, team_data_no_asg=team_overlaps_sort_date_no_asg, data_len_no_asg=len_teammates_no_asg, playername1=output1.iloc[0]['playerName'], playername2=output2.iloc[0]['playerName'])
        elif len(output1) == 0:
            return render_template('error_no_results_name.html', playername=player1)
        elif len(output2) == 0:
            return render_template('error_no_results_name.html', playername=player2)
        elif len(output1) > 1 and len(output2) == 1:
            # clarify player 1
            session["player2"], session["player2_id"] = (output2.iloc[0]['playerName'], output2.iloc[0]['playerId']) #target2
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output1.to_dict('records'))
        elif len(output2) > 1 and len(output1) == 1:
            # clarify player 2
            session["player1"], session["player1_id"] = (output1.iloc[0]['playerName'], output1.iloc[0]['playerId'])
            session["player_to_clarify"] = "player2"
            return render_template('options_1.html', data=output2.to_dict('records'))
        else:
            # clarify both players
            return render_template('options_2.html', data1=output1.to_dict('records'), data2=output2.to_dict('records'))
    else:
        return render_template('error.html')

# two players: games played
@app.route("/two_players_games", methods=["GET","POST"])
def two_players_games():
    if request.method == "POST":
        session.clear()
        session["task"] = "two_players_games"
        db = teammates_db(config, engine)
        player1 = request.form['player1'].strip()
        output1 = db.get_links_from_name(player1)
        player2 = request.form['player2'].strip()
        output2 = db.get_links_from_name(player2)
        if len(output1) == 1 and len(output2) == 1:
            # we have unique player ids for both
            if output1.iloc[0]['playerId'] == output2.iloc[0]['playerId']:
                return render_template('error_same_player_games.html')
            data = db.get_two_player_games(output1.iloc[0]['playerId'], output2.iloc[0]['playerId'])
            return render_template('two_players_games.html', data=data, playername1=output1.iloc[0]['playerName'], playername2=output2.iloc[0]['playerName'], latest_date=db.get_latest_game_date().date())
        elif len(output1) == 0:
            return render_template('error_no_results_name.html', playername=player1)
        elif len(output2) == 0:
            return render_template('error_no_results_name.html', playername=player2)
        elif len(output1) > 1 and len(output2) == 1:
            # clarify player 1
            session["player2"], session["player2_id"] = (output2.iloc[0]['playerName'], output2.iloc[0]['playerId']) #target2
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output1.to_dict('records'))
        elif len(output2) > 1 and len(output1) == 1:
            # clarify player 2
            session["player1"], session["player1_id"] = (output1.iloc[0]['playerName'], output1.iloc[0]['playerId'])
            session["player_to_clarify"] = "player2"
            return render_template('options_1.html', data=output2.to_dict('records'))
        else:
            # clarify both players
            return render_template('options_2.html', data1=output1.to_dict('records'), data2=output2.to_dict('records'))
    else:
        return render_template('error.html')

# TEAM
# team: view roster history
@app.route("/team_history", methods=["GET", "POST"])
def team_history():
    if request.method == "POST":
        session.clear()
        team = request.form['team_hist']
        season = request.form['season_hist']
        db = teammates_db(config, engine)
        data = db.get_roster_history(team, season)
        return render_template('team_history.html', data=data, team=team, season=season)
    else:
        return render_template('error.html')
    
# team: view roster history range
@app.route("/team_history_range", methods=["GET", "POST"])
def team_history_range():
    if request.method == "POST":
        session.clear()
        team = request.form['team_hist_range']
        start_season = request.form['season_hist_range_start']
        print(start_season)
        end_season = request.form['season_hist_range_end']
        start_season_start = int(start_season.split("-")[0])
        end_season_start = int(end_season.split("-")[0])
        if end_season_start < start_season_start:
            return render_template('error_prev_season.html')
        db = teammates_db(config, engine)
        data = db.get_roster_history_range(team, start_season, end_season)
        return render_template('team_history_range.html', data=data, team=team, start_season=start_season, end_season=end_season)
    else:
        return render_template('error.html')

# team: compare rosters
@app.route("/team_compare_rosters", methods=["GET", "POST"])
def team_compare_rosters():
    if request.method == "POST":
        session.clear()
        team1 = request.form['team1']
        team2 = request.form['team2']
        if team1 == team2:
            return render_template('error_teams_same.html')
        season = request.form['season_pair']
        db = teammates_db(config, engine)
        data_asg, data_no_asg = db.get_two_rosters_overlap(team1, team2, season)
        return render_template('team_comparison.html', team1=team1, team2=team2, season=season, data_asg=data_asg, data_no_asg=data_no_asg)
    else:
        return render_template('error.html')

# clarify one player name
# TODO check all of these
@app.route("/options_result_1", methods=["GET", "POST"])
def options_result_1():
    if request.method == "POST":
        target = request.form['playerid']
        tmp = target.split("#")
        #if "player1_id" not in session:
        if session["player_to_clarify"] == "player1":
            session["player1"] = tmp[0]
            session["player1_id"] = tmp[1] 
        elif session["player_to_clarify"] == "player2":
            session["player2"] = tmp[0]
            session["player2_id"] = tmp[1]
        if "player2_id" in session and session["player1_id"] == session["player2_id"]:
            return render_template(f"error_two_players_same.html")
        db = teammates_db(config, engine)
        latest_date = db.get_latest_game_date().date()
        if session["task"] == "one_player_career":
            data, longest_name_asg, longest_name_no_asg, data_no_asg, data_length, data_length_no_asg  = db.get_overlapping_player_terms(session["player1_id"])
            career_data, start_date, end_date = db.get_player_career(session["player1_id"])
            return render_template(f'one_player_career.html', data_asg=data, data_no_asg=data_no_asg, data_length_asg=data_length, data_length_no_asg=data_length_no_asg, career_data=career_data, playername1=session.get("player1"), display_name_asg=longest_name_asg, display_name_no_asg=longest_name_no_asg, start_date=start_date, end_date=end_date)
        elif session["task"] == "one_player_roster":
            data, data_no_asg = db.get_one_player_roster(session["player1_id"], session["team"], session["season"])
            return render_template('one_player_roster.html', playername=session.get("player1"), team=session.get("team"), season=session.get("season"), data=data, data_no_asg=data_no_asg)
        elif session["task"] == "one_player_team_games":
            data = db.get_one_player_team_games(session["player1_id"], session.get("team"))
            return render_template(f'one_player_team_games.html', playername=session.get("player1"),
                                       team=session.get("team"), data=data, latest_date=latest_date)        
        elif session["task"] == "two_players_results":
            data, _, _, data_no_asg, _, _ = db.get_overlapping_player_terms(session["player1_id"], session["player2_id"])
            return render_template(f'two_players_results.html', data=data, data_no_asg=data_no_asg, playername1=session.get("player1"), playername2=session.get("player2"))
        elif session["task"] == "two_players_shared_teammates":
            team_overlaps_sort_date, team_overlaps_sort_date_no_asg, teammates_asg, len_teammates_asg, teammates_no_asg, len_teammates_no_asg = db.get_two_players_shared_teammates(session["player1_id"], session["player1"], session["player2_id"], session["player2"])
            return render_template('two_players_shared_teammates.html', data=teammates_asg, data_no_asg=teammates_no_asg, team_data=team_overlaps_sort_date, data_len_asg=len_teammates_asg, team_data_no_asg=team_overlaps_sort_date_no_asg, data_len_no_asg=len_teammates_no_asg,  playername1=session["player1"], playername2=session["player2"])
            # team_data, _, team_data_no_asg, _, _ = db.get_overlapping_player_terms(session["player1_id"], session["player2_id"])
            # return render_template('two_players_shared_teammates.html', data=data, data_no_asg=data_no_asg, team_data=team_data, team_data_no_asg=team_data_no_asg, playername1=session["player1"], playername2=session["player2"])
        elif session["task"] == "two_players_games":
            data = db.get_two_player_games(session["player1_id"], session["player2_id"])
            return render_template(f'two_players_games.html', data=data, playername1=session.get("player1"), playername2=session.get("player2"), latest_date=latest_date)

    else:
        return render_template('error.html')

# clarify two player names
@app.route("/options_result_2", methods=["GET", "POST"])
def options_result_2():
    if request.method == "POST":
        tmp1 = request.form['playerid1'].split("#")
        tmp2 = request.form['playerid2'].split("#")
        session["player1"] = tmp1[0]
        session["player1_id"] = tmp1[1]
        session["player2"] = tmp2[0]
        session["player2_id"] = tmp2[1]
        if session["player1_id"] == session["player2_id"]:
            return render_template("error_two_players_same.html")
        db = teammates_db(config, engine)
        if session["task"] == "two_players_results":
            data, _, _, data_no_asg, _, _ = db.get_overlapping_player_terms(session["player1_id"], session["player2_id"])
            return render_template(f'two_players_results.html', data=data, data_no_asg=data_no_asg, playername1=session.get("player1"), playername2=session.get("player2"))
        elif session["task"] == "two_players_shared_teammates":
            team_overlaps_sort_date, team_overlaps_sort_date_no_asg, teammates_asg, len_teammates_asg, teammates_no_asg, len_teammates_no_asg = db.get_two_players_shared_teammates(session["player1_id"], session["player1"], session["player2_id"], session["player2"])
            return render_template('two_players_shared_teammates.html', data=teammates_asg, data_no_asg=teammates_no_asg, team_data=team_overlaps_sort_date, data_len_asg=len_teammates_asg, team_data_no_asg=team_overlaps_sort_date_no_asg, data_len_no_asg=len_teammates_no_asg,  playername1=session["player1"], playername2=session["player2"])
            # data, data_no_asg = db.get_two_players_shared_teammates(session["player1_id"], session["player2_id"])
            # team_data, _, team_data_no_asg, _, _ = db.get_overlapping_player_terms(session["player1_id"], session["player2_id"])
            # return render_template('two_players_shared_teammates.html', data=data, data_no_asg=data_no_asg, team_data=team_data, team_data_no_asg=team_data_no_asg, playername1=session["player1"], playername2=session["player2"])
        elif session["task"] == "two_players_games":
            data = db.get_two_player_games(session["player1_id"], session["player2_id"])
            return render_template(f'two_players_games.html', data=data, playername1=session.get("player1"),
                                   playername2=session.get("player2"), latest_date = db.get_latest_game_date().date())
    else:
        return render_template('error.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
