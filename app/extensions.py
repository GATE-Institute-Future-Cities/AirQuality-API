from flask_sqlalchemy import SQLAlchemy
from flask_restx import Api
from flask_httpauth import HTTPDigestAuth

api = Api(title='AirQuality')
db = SQLAlchemy()
auth = HTTPDigestAuth()
