from flask import Flask
app = Flask(__name__)


@app.route("/temperatures")
def hello():
    return """{"mytemp1":21.7,"mytemp2":21.0}"""


if __name__ == "__main__":
    app.run(port=5001)
