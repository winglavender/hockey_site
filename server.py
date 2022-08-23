from flask import Flask, render_template, request, session
from query_db import hockey_db 
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
with open('data/nhl_team_data.txt','r') as in_file:
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
        db = hockey_db()
        num_results, output = db.retrieve_player_link(target)
        session["task"] = "career"
        if num_results == 1:
            # output is unique player link
            orig_name, player_id = output
            data, longest_name = db.get_overlapping_player_terms(player_id)
            career_data = db.get_player_career(player_id)
            return render_template('career_results.html', data=data, career_data=career_data, playername1=orig_name, display_name=longest_name)
        elif num_results == 0:
            # output is player name searched
            return render_template('no_results.html', playername=output)
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
        db = hockey_db()
        player1 = request.form['player1'].strip()
        num_results1, target1 = db.retrieve_player_link(player1)
        player2 = request.form['player2'].strip()
        num_results2, target2 = db.retrieve_player_link(player2)
        session["task"] = "pair"
        if num_results1 == 1 and num_results2 == 1:
            # we have unique player ids for both
            orig_name1, player_id1 = target1
            orig_name2, player_id2 = target2
            if player_id1 == player_id2:
                return render_template('pair_same_player.html')
            data, _ = db.get_overlapping_player_terms(player_id1, player_id2)
            if len(data) == 0:
                return render_template('no_pair_results.html', playername1=orig_name1, playername2=orig_name2)
            else:
                return render_template('pair_results.html', data=data, playername1=orig_name1, playername2=orig_name2)
        elif num_results1 == 0:
            return render_template('no_results.html', playername=target1)
        elif num_results2 == 0:
            return render_template('no_results.html', playername=target2)
        elif num_results1 > 1 and num_results2 == 1:
            # clarify player 1
            session["player2"], session["player2_id"] = target2
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=target1)
        elif num_results2 > 1 and num_results1 == 1:
            # clarify player 2
            session["player1"], session["player1_id"] = target1
            session["player_to_clarify"] = "player2"
            return render_template('options_1.html', data=target2)
        else:
            # clarify both players
            return render_template('options_2.html', data1=target1, data2=target2)
    else:
        return render_template('error.html')

@app.route("/team_year_result", methods=["GET", "POST"])
def team_year_result():
    if request.method == "POST":
        session.clear()
        team = request.form['team_hist']
        season = request.form['season_hist']
        db = hockey_db()
        session["task"] = "roster_history"
        session["team"] = team
        session["season"] = season
        data = db.display_roster_history(team, season)
        return render_template('team_history_result.html', data=data, team=team, season=season)
    else:
        return render_template('error.html')

@app.route("/player_team_year_result", methods=["GET", "POST"])
def player_team_year_result():
    if request.method == "POST":
        session.clear()
        player = request.form['player'].strip()
        team = request.form['team']
        season = request.form['season']
        db = hockey_db()
        session["task"] = "roster"
        session["team"] = team
        session["season"] = season
        num_results, target = db.retrieve_player_link(player)
        if num_results == 1:
            # we have a unique player id
            orig_name, player_id = target
            data, len_data = db.query_roster(player_id, team, season)
            if len_data == 0:
                return render_template('no_roster_results.html', playername=orig_name,team=team, season=season)
            else:
                return render_template('roster_results.html', playername=orig_name, team=team, season=season, data=data)
        elif num_results == 0:
            return render_template('no_results.html', playername=target)
        elif num_results > 1:
            # clarify player id
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=target)
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
        db = hockey_db()
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
        db = hockey_db()
        player1 = request.form['player1'].strip()
        num_results1, target1 = db.retrieve_player_link(player1)
        player2 = request.form['player2'].strip()
        num_results2, target2 = db.retrieve_player_link(player2)
        session["task"] = "traverse"
        if num_results1 == 1 and num_results2 == 1:
            # we have unique player ids for both
            orig_name1, player_id1 = target1
            orig_name2, player_id2 = target2
            if player_id1 == player_id2:
                return render_template('traverse_same_player.html')
            data = db.traverse_graph(player_id1, player_id2)
            team_data, _ = db.get_overlapping_player_terms(player_id1, player_id2)
            if len(data) == 0:
                return render_template('no_traverse_results.html', playername1=orig_name1, playername2=orig_name2)
            elif len(team_data) == 0:
                return render_template('traverse_results.html', data=data, playername1=orig_name1, playername2=orig_name2)
            else:
                return render_template('traverse_results_same_team.html', data=data, team_data=team_data, playername1=orig_name1, playername2=orig_name2)
        elif num_results1 == 0:
            return render_template('no_results.html', playername=target1)
        elif num_results2 == 0:
            return render_template('no_results.html', playername=target2)
        elif num_results1 > 1 and num_results2 == 1:
            # clarify player 1
            session["player2"], session["player2_id"] = target2
            session["player_to_clarify"] = "player1"
            return render_template('options_1.html', data=target1)
        elif num_results2 > 1 and num_results1 == 1:
            # clarify player 2
            session["player1"], session["player1_id"] = target1
            session["player_to_clarify"] = "player2"
            return render_template('options_1.html', data=target2)
        else:
            # clarify both players
            return render_template('options_2.html', data1=target1, data2=target2)
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
        db = hockey_db()
        data = []
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
        db = hockey_db()
        data = []
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
