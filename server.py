from flask import Flask, render_template, request
from query_db import query_db

app = Flask(__name__)
#if os.getenv('HEROKU'):
#    local = False
#else:
#    local = True

# todo secret key for session
#if local:
#    app.config.from_pyfile('instance/config.py')
#else:
#    app.config.update(SECRET_KEY=os.getenv('SECRET_KEY'))

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/form_result", methods=["GET","POST"])
def form_result():
    if request.method == "POST":
        target = request.form['target']
        data = query_db(target) 
        return render_template('results.html', data=data)
    else:
        return render_template('error.html')

if __name__ == "__main__":
    app.run()
