from flask import Flask, render_template, request, session
from query_db import query_career_teammates, retrieve_player_link, query_pair_teammates
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

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/form_result", methods=["GET","POST"])
def form_result():
    if request.method == "POST":
        target = request.form['target']
        output, num_results = retrieve_player_link(target)
        if num_results == 1:
            # output is unique player link
            data = query_career_teammates(output) 
            return render_template('results.html', data=data, player=target)
        elif num_results == 0:
            # output is player name searched
            return render_template('no_results.html', playername=output)
        else:
            return render_template('options.html', data=output)
    else:
        return render_template('error.html')

@app.route("/pair_form_result", methods=["GET","POST"])
def pair_form_result():
    if request.method == "POST":
        player1 = request.form['player1']
        session["player1"] = player1
        target1, num_results1 = retrieve_player_link(player1)
        player2 = request.form['player2']
        session["player2"] = player2
        target2, num_results2 = retrieve_player_link(player2)
        if num_results1 == 1 and num_results2 == 1:
            # we have unique player ids for both
            if target1 == target2:
                return render_template('pair_same_player.html')
            data = query_pair_teammates(target1, target2)
            if len(data) == 0:
                return render_template('no_pair_results.html', playername1=player1, playername2=player2)
            else:
                return render_template('pair_results.html', data=data, playername1=player1, playername2=player2)
        elif num_results1 == 0:
            return render_template('no_results.html', playername=target1)
        elif num_results2 == 0:
            return render_template('no_results.html', playername=target2)
        elif num_results1 > 1 and num_results2 == 1:
            # clarify player 1
            session["player2_id"] = target2
            return render_template('options_1.html', data=target1)
        elif num_results2 > 1 and num_results1 == 1:
            # clarify player 2
            session["player2"] = player2
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
        if "player1_id" not in session:
            session["player1_id"] = target
        elif "player2_id" not in session:
            session["player2_id"] = target
        if session["player1_id"] == session["player2_id"]:
            return render_template("pair_same_player.html")
        data = query_pair_teammates(session["player1_id"], session["player2_id"])
        if len(data) == 0:
            return render_template('no_pair_results.html', playername1=session.get("player1"), playername2=session.get("player2"))
        else:
            return render_template('pair_results.html', data=data, playername1=session.get("player1"), playername2=session.get("player2"))
    else:
        return render_template('error.html')

@app.route("/options_result_2", methods=["GET", "POST"])
def options_result_2():
    if request.method == "POST":
        session["player1_id"] = request.form['playerid1']
        session["player2_id"] = request.form['playerid2']
        if session["player1_id"] == session["player2_id"]:
            return render_template("pair_same_player.html")
        data = query_pair_teammates(session["player1_id"], session["player2_id"])
        if len(data) == 0:
            return render_template('no_pair_results.html', playername1=session.get("player1"), playername2=session.get("player2"))
        else:
            return render_template('pair_results.html', data=data, playername1=session.get("player1"), playername2=session.get("player2"))
    else:
        return render_template('error.html')

@app.route("/options_result", methods=["GET", "POST"])
def options_result():
    if request.method == "POST":
        target = request.form['playerid']
        data = query_career_teammates(target)
        return render_template('results.html', data=data)
    else:
        return render_template('error.html')

@app.route("/version_notes")
def version_notes():
    return render_template("version_notes.html")

@app.route("/data")
def data():
    return render_template("data.html")

@app.route("/test")
def test():
    data = []
    with open('data_cmd.txt') as in_file:
        for line in in_file:
            tmp = line.strip().split("\t")
            playername = tmp[0]
            ranges = tmp[1].split(",")
            for range_str in ranges:
               # range_str = tmp[1].split(",")[0]
                new_str = range_str[1:-1]
                tmp1 = new_str.split("-")
                year1 = int(tmp1[0])
                month1 = 8 
                day1 = 1
                year2 = int(tmp1[1])
                month2 = 5 
                day2 = 30
                data.append((playername,f'Edmonton Oilers', year1, month1, day1, year2, month2, day2)) 
    return render_template("test.html",data=data)

if __name__ == "__main__":
    app.run()
