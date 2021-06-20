from flask import Flask, render_template, request
from query_db import query_db, retrieve_link_db

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/form_result", methods=["GET","POST"])
def form_result():
    if request.method == "POST":
        target = request.form['target']
        output, num_results = retrieve_link_db(target)
        if num_results == 1:
            # output is unique player link
            data = query_db(output) 
            return render_template('results.html', data=data)
        elif num_results == 0:
            # output is player name searched
            return render_template('no_results.html', playername=output)
        else:
            return render_template('options.html', data=output)
    else:
        return render_template('error.html')

@app.route("/options_result", methods=["GET", "POST"])
def options_result():
    if request.method == "POST":
        target = request.form['playerid']
        print(target)
        data = query_db(target)
        return render_template('results.html', data=data)
    else:
        return render_template('error.html')

@app.route("/version_notes")
def version_notes():
    return render_template("version_notes.html")

@app.route("/test")
def test():
    data = {}
    data['teammates'] = []
    with open('data_cmd.txt') as in_file:
        for line in in_file:
            tmp = line.strip().split("\t")
            playername = tmp[0]
            range_str = tmp[1].split(",")[0]
            range_str = range_str[1:-1]
            tmp1 = range_str.split("-")
            year1 = int(tmp1[0])
            year2 = int(tmp1[1])
            data['teammates'].append((playername, year1, year2)) 
            data['player'] = tmp[0]
    return render_template("test.html",data=data)

if __name__ == "__main__":
    app.run()
