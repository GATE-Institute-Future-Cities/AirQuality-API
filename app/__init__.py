from flask import Flask
from .extensions import api, db
from .aq_apis import Airquality_apis
import os

def create_app():
    
    app = Flask(__name__) 
    
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:mohi1234@localhost/ExEa_main'
    
    api.init_app(app)
    db.init_app(app)
    api.add_namespace(Airquality_apis)
    
    app.config['SECRET_KEY'] = os.urandom(24) ## random secret KEY
    
    return app