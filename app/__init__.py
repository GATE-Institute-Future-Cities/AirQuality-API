from flask import Flask
from .extensions import api, db
from .SpecificDates import specifiedDates_apis
from .stations import Station_apis
import os
from config import db_uri

    
app = Flask(__name__) 

app.config['SQLALCHEMY_DATABASE_URI'] = db_uri

api.init_app(app)
db.init_app(app)
api.add_namespace(specifiedDates_apis)
api.add_namespace(Station_apis)

app.config['SECRET_KEY'] = os.urandom(24) ## random secret KEY

if __name__ == '__main__':
    app.run()

