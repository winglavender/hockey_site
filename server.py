from flask import Flask, render_template, request, session
from query_name_db import name_db
from query_db import hockey_db
from query_game_roster_db import game_roster_db
import os

app = Flask(__name__)
if os.getenv('PYANYWHERE'):
    local = False
else:
    local = True
if local:
    app.config.from_pyfile('config.py')
else:
    app.config.update(SECRET_KEY = os.getenv("SECRET_KEY"))

# populate NHL team data
nhl_team_data = {'team_order': [], 'team_seasons': {}}
with open('nhl_team_data.txt','r') as in_file:
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
    return render_template("index.html",nhl_team_data=nhl_team_data)

@app.route("/form_result", methods=["GET","POST"])
def form_result():
    if request.method == "POST":
        session.clear()
        target = request.form['target'].strip()
        names_db = name_db()
        db = hockey_db(names_db)
        output = db.retrieve_player_link(target)
        session["task"] = "career"
        # if num_results == 1:
        if len(output) == 1:
            # output is unique player link
            # orig_name, player_id = output[0]
            data, longest_name = db.get_overlapping_player_terms(output[0]['link'])
            career_data = db.get_player_career(output[0]['link'])
            return render_template('career_results.html', data=data, career_data=career_data, playername1=output[0]['player'], display_name=longest_name)
        # elif num_results == 0:
        elif len(output) == 0:
            # output is player name searched
            return render_template('no_results.html', playername=target)
        else:
            # output contains list of options for player
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output)
    else:
        return render_template('error.html')

@app.route("/pair_form_result", methods=["GET","POST"])
def pair_form_result():
    if request.method == "POST":
        session.clear()
        names_db = name_db()
        db = hockey_db(names_db)
        player1 = request.form['player1'].strip()
        # num_results1, target1\
        output1 = db.retrieve_player_link(player1)
        player2 = request.form['player2'].strip()
        # num_results2, target2\
        output2 = db.retrieve_player_link(player2)
        session["task"] = "pair"
        # if num_results1 == 1 and num_results2 == 1:
        if len(output1) == 1 and len(output2) == 1:
            # we have unique player ids for both
            # orig_name1, player_id1 = target1
            # orig_name2, player_id2 = target2
            if output1[0]['link'] == output2[0]['link']:
                return render_template('pair_same_player.html')
            data, _ = db.get_overlapping_player_terms(output1[0]['link'], output2[0]['link'])
            if len(data) == 0:
                return render_template('no_pair_results.html', playername1=output1[0]['player'], playername2=output2[0]['player'])
            else:
                return render_template('pair_results.html', data=data, playername1=output1[0]['player'], playername2=output2[0]['player'])
        elif len(output1) == 0:
            return render_template('no_results.html', playername=player1)
        elif len(output2) == 0:
            return render_template('no_results.html', playername=player2)
        elif len(output1) > 1 and len(output2) == 1:
            # clarify player 1
            session["player2"], session["player2_id"] = (output2[0]['player'], output2[0]['link']) #target2
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output1)
        elif len(output2) > 1 and len(output1) == 1:
            # clarify player 2
            session["player1"], session["player1_id"] = (output1[0]['player'], output1[0]['link']) #target1
            session["player_to_clarify"] = "player2"
            return render_template('options_1.html', data=output2)
        else:
            # clarify both players
            return render_template('options_2.html', data1=output1, data2=output2)
    else:
        return render_template('error.html')

# compare game rosters for two players to get games in common (teammates/opponents)
@app.route("/game_result", methods=["GET","POST"])
def game_result():
    if request.method == "POST":
        session.clear()
        session["task"] = "games"
        names_db = name_db()
        db = game_roster_db(names_db)
        latest_date = db.get_latest_date()
        player1 = request.form['player1'].strip()
        output1 = db.get_player_id(player1)
        player2 = request.form['player2'].strip()
        output2 = db.get_player_id(player2)
        if len(output1) == 1 and len(output2) == 1:
            # we have unique player ids for both
            if output1[0]['link'] == output2[0]['link']:
                return render_template('same_player_games.html')
            data = db.get_results_html(int(output1[0]['link']), int(output2[0]['link']))
            return render_template('game_results.html', data=data, playername1=output1[0]['player'], playername2=output2[0]['player'], latest_date=latest_date)
        elif len(output1) == 0:
            return render_template('no_results.html', playername=player1)
        elif len(output2) == 0:
            return render_template('no_results.html', playername=player2)
        elif len(output1) > 1 and len(output2) == 1:
            # clarify player 1
            session["player2"], session["player2_id"] = (output2[0]['player'], output2[0]['link']) #target2
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output1)
        elif len(output2) > 1 and len(output1) == 1:
            # clarify player 2
            session["player1"], session["player1_id"] = (output1[0]['player'], output1[0]['link'])
            session["player_to_clarify"] = "player2"
            return render_template('options_1.html', data=output2)
        else:
            # clarify both players
            return render_template('options_2.html', data1=output1, data2=output2)
    else:
        return render_template('error.html')


@app.route("/team_year_result", methods=["GET", "POST"])
def team_year_result():
    if request.method == "POST":
        session.clear()
        team = request.form['team_hist']
        season = request.form['season_hist']
        names_db = name_db()
        db = hockey_db(names_db)
        session["task"] = "roster_history"
        session["team"] = team
        session["season"] = season
        data = db.display_roster_history(team, season)
        return render_template('team_history_result.html', data=data, team=team, season=season)
    else:
        return render_template('error.html')


@app.route("/player_team_game_result", methods=["GET", "POST"])
def player_team_game_result():
    if request.method == "POST":
        session.clear()
        player = request.form['player'].strip()
        team = request.form['team']
        names_db = name_db()
        db = game_roster_db(names_db)
        latest_date = db.get_latest_date()
        session["task"] = "team_games"
        session["team"] = team
        # num_results, target
        output = db.get_player_id(player)
        if len(output) == 1:
            # we have a unique player id
            data = db.get_results_html_vs_team(int(output[0]['link']), team)
            return render_template('vs_team_game_results.html', playername=output[0]['player'], team=team, data=data, latest_date=latest_date)
        elif len(output) == 0:
            return render_template('no_results.html', playername=player)
        elif len(output) > 1:
            # clarify player id
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output)
    else:
        return render_template('error.html')

@app.route("/player_team_year_result", methods=["GET", "POST"])
def player_team_year_result():
    if request.method == "POST":
        session.clear()
        player = request.form['player'].strip()
        team = request.form['team']
        season = request.form['season']
        names_db = name_db()
        db = hockey_db(names_db)
        session["task"] = "roster"
        session["team"] = team
        session["season"] = season
        # num_results, target\
        output = db.retrieve_player_link(player)
        if len(output) == 1:
            # we have a unique player id
            data, len_data = db.query_roster(output[0]['link'], team, season)
            if len_data == 0:
                return render_template('no_roster_results.html', playername=output[0]['player'], team=team, season=season)
            else:
                return render_template('roster_results.html', playername=output[0]['player'], team=team, season=season, data=data)
        elif len(output) == 0:
            return render_template('no_results.html', playername=player)
        elif len(output) > 1:
            # clarify player id
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output)
    else:
        return render_template('error.html')

@app.route("/roster_pair_result", methods=["GET", "POST"])
def roster_pair_result():
    if request.method == "POST":
        session.clear()
        team1 = request.form['team1']
        team2 = request.form['team2']
        if team1 == team2:
            return render_template('rosters_same_team.html')
        season = request.form['season_pair']
        names_db = name_db()
        db = hockey_db(names_db)
        session["task"] = "roster"
        session["team1"] = team1
        session["team2"] = team2
        session["season_pair"] = season
        data = db.query_roster_pair(team1, team2, season)
        # len_data
        return render_template('roster_pair_results.html', team1=team1, team2=team2, season=season, data=data)
    else:
        return render_template('error.html')

@app.route("/graph_traverse_result", methods=["GET", "POST"])
def graph_traverse_result():
    if request.method == "POST":
        session.clear()
        names_db = name_db()
        db = hockey_db(names_db)
        player1 = request.form['player1'].strip()
        # num_results1, target1 \
        output1 = db.retrieve_player_link(player1)
        player2 = request.form['player2'].strip()
        # num_results2, target2\
        output2 = db.retrieve_player_link(player2)
        session["task"] = "traverse"
        if len(output1) == 1 and len(output2) == 1:
            # we have unique player ids for both
            # orig_name1, player_id1 = target1
            # orig_name2, player_id2 = target2
            if output1[0]['link'] == output2[0]['link']:
                return render_template('traverse_same_player.html')
            data = db.traverse_graph(output1[0]['link'], output2[0]['link'])
            team_data, _ = db.get_overlapping_player_terms(output1[0]['link'], output2[0]['link'])
            if len(data) == 0:
                return render_template('no_traverse_results.html', playername1=output1[0]['player'], playername2=output2[0]['player'])
            elif len(team_data) == 0:
                return render_template('traverse_results.html', data=data, playername1=output1[0]['player'], playername2=output2[0]['player'])
            else:
                return render_template('traverse_results_same_team.html', data=data, team_data=team_data, playername1=output1[0]['player'], playername2=output2[0]['player'])
        elif len(output1) == 0:
            return render_template('no_results.html', playername=player1)
        elif len(output2) == 0:
            return render_template('no_results.html', playername=player2)
        elif len(output1) > 1 and len(output2) == 1:
            # clarify player 1
            session["player2"], session["player2_id"] = (output2[0]['player'], output2[0]['link']) #target2
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=output1)
        elif len(output2) > 1 and len(output1) == 1:
            # clarify player 2
            session["player1"], session["player1_id"] = (output1[0]['player'], output1[0]['link'])
            session["player_to_clarify"] = "player2"
            return render_template('options_1.html', data=output2)
        else:
            # clarify both players
            return render_template('options_2.html', data1=output1, data2=output2)
    else:
        return render_template('error.html')

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
            return render_template(f"{session['task']}_same_player.html")
        names_db = name_db()
        db = hockey_db(names_db)
        games_db = game_roster_db(names_db)
        latest_date = games_db.get_latest_date()
        # data = []
        if session["task"] == "career":
            data, longest_name = db.get_overlapping_player_terms(session["player1_id"])
            career_data = db.get_player_career(session["player1_id"])
            return render_template(f'career_results.html', data=data, career_data=career_data, playername1=session.get("player1"), display_name=longest_name)
        elif session["task"] == "traverse":
            data = db.traverse_graph(session["player1_id"], session["player2_id"])
            team_data, _ = db.get_overlapping_player_terms(session["player1_id"], session["player2_id"])
            if len(data) == 0:
                return render_template('no_traverse_results.html', playername1=session["player1"], playername2=session["player2"])
            elif len(team_data) == 0:
                return render_template('traverse_results.html', data=data, playername1=session["player1"], playername2=session["player2"])
            else:
                return render_template('traverse_results_same_team.html', data=data, team_data=team_data, playername1=session["player1"], playername2=session["player2"])
        elif session["task"] == "pair":
            data, _ = db.get_overlapping_player_terms(session["player1_id"], session["player2_id"])
            if len(data) == 0:
                return render_template(f'no_pair_results.html', playername1=session.get("player1"), playername2=session.get("player2"))
            else:
                return render_template(f'pair_results.html', data=data, playername1=session.get("player1"), playername2=session.get("player2"))
        elif session["task"] == "roster":
            data, len_data = db.query_roster(session["player1_id"], session["team"], session["season"])
            if len_data == 0:
                return render_template('no_roster_results.html', playername=session.get("player1"),team=session.get("team"), season=session.get("season"))
            else:
                return render_template('roster_results.html', playername=session.get("player1"), team=session.get("team"), season=session.get("season"), data=data)
        elif session["task"] == "games":
            data = games_db.get_results_html(int(session["player1_id"]), int(session["player2_id"]))
            return render_template(f'game_results.html', data=data, playername1=session.get("player1"),
                                       playername2=session.get("player2"), latest_date=latest_date)
        elif session["task"] == "team_games":
            data = games_db.get_results_html_vs_team(int(session["player1_id"]), session.get("team"))
            return render_template(f'vs_team_game_results.html', data=data, playername=session.get("player1"),
                                       team=session.get("team"), latest_date=latest_date)

    else:
        return render_template('error.html')

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
            return render_template(f"{session['task']}_same_player.html")
        names_db = name_db()
        db = hockey_db(names_db)
        games_db = game_roster_db(names_db)
        # data = []
        if session["task"] == "traverse":
            data = db.traverse_graph(session["player1_id"], session["player2_id"])
            team_data, _ = db.get_overlapping_player_terms(session["player1_id"], session["player2_id"])
            if len(data) == 0:
                return render_template('no_traverse_results.html', playername1=session["player1"], playername2=session["player2"])
            elif len(team_data) == 0:
                return render_template('traverse_results.html', data=data, playername1=session["player1"], playername2=session["player2"])
            else:
                return render_template('traverse_results_same_team.html', data=data, team_data=team_data, playername1=session["player1"], playername2=session["player2"])
        elif session["task"] == "pair":
            data, _ = db.get_overlapping_player_terms(session["player1_id"], session["player2_id"])
            if len(data) == 0:
                return render_template(f'no_pair_results.html', playername1=session.get("player1"), playername2=session.get("player2"))
            else:
                return render_template(f'pair_results.html', data=data, playername1=session.get("player1"), playername2=session.get("player2"))
        elif session["task"] == "games":
            data = games_db.get_results_html(int(session["player1_id"]), int(session["player2_id"]))
            # if len(data) == 0:
            #     return render_template(f'no_pair_results.html', playername1=session.get("player1"),
            #                            playername2=session.get("player2"))
            # else:
            return render_template(f'game_results.html', data=data, playername1=session.get("player1"),
                                   playername2=session.get("player2"))
    else:
        return render_template('error.html')

@app.route("/details")
def details():
    return render_template("details.html")

@app.route("/data")
def data():
    return render_template("data.html")

@app.route("/contact")
def contact():
    return render_template("contact.html")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
