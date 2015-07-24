#!/usr/bin/env python3

import os.path
import time
from functools import wraps
from flask import Flask
from flask import render_template
from flask import request
from flask import Response
from flask import abort
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

PORT = 12300
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
app = Flask(__name__)

# Set cache timeout for all static content.
# Otherwise the dynamic image content does not work
# correctly due to browser caching.
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 60


def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    return username == 'admin' and password == 'secret'


def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


@app.before_request
def check_login():
    if request.endpoint == 'static':
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            abort(401)
    return None


@app.route("/")
@requires_auth
def index():
    return render_template("index.html", last_update=get_update_time())


def get_update_time():
    try:
        image_file = os.path.join(THIS_DIR, "static/images/temperatures/hour.png")
        return time.ctime(os.path.getmtime(image_file))
    except OSError:
        return "Never"


@app.route("/temperature/<name>")
@requires_auth
def get_temperature_detailed(name=None):
    return render_template("detailed.html", variable="temperatures", name=name)


if __name__ == "__main__":
    app.debug = True
    http_server = HTTPServer(WSGIContainer(app))
    http_server.listen(PORT)
    IOLoop.instance().start()
