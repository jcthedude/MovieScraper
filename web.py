from globals import *
from flask import Flask, render_template


application = Flask(__name__)


@application.route("/")
def get_shows():
    _shows = collection_show.find({"order": {"$lte": 100}}, {'id': 1, 'name': 1, 'order': 1, 'banner': 1, 'rating': 1, '_id': 0}).sort([("order", 1)])
    shows = [show for show in _shows]

    return render_template('tv.html', shows=shows)

if __name__ == "__main__":
    application.run(host='0.0.0.0', debug=True)
