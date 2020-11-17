# System packages
from flask import Flask
from flask_restful import Api

# Own packages
from resources import GetTweetVolume

# Initiate app
app = Flask(__name__)
api = Api(app)

# Routes
api.add_resource(GetTweetVolume, "/get-tweet-volume")


# Test route
@app.route("/", methods=["GET"])
def index():
    return {"message": "Hello world!"}


if __name__ == "__main__":
    app.run(debug=True)
