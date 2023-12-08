from flask_restx import Resource, Namespace, reqparse
from .extensions import api
from .authentication import auth
from flask import jsonify
import psycopg2

Station_apis= Namespace('station')

conn = psycopg2.connect(database="ExEa_main", user="postgres", password="mohi1234", host="localhost", port="5432")

def get_paramName(paramIds): ### function that gets all the ids for all the elements that we recive from the db and serches for the paramabreviation in the paramtype table
    cursor = conn.cursor()
    all_params = []
    for param in paramIds:
        cursor.execute('SELECT parameterabbreviation FROM parametertype WHERE id = %s', (param,))
        paramname = cursor.fetchone()[0]
        all_params.append(paramname)
    return all_params

def getStationId(stationName):
    cursor = conn.cursor()
    cursor.execute('SELECT stationid FROM airqualitystation WHERE stationname = %s', (stationName,))
    stationid = cursor.fetchone()[0]
    return stationid

@Station_apis.route('/api/stations')
class AllStations(Resource):
    @api.doc(description='Get all info about every station we have in the database')
    @auth.login_required
    def get(self):
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM airqualitystation')
        all_stations = cursor.fetchall() ## get all the stations in the database
        result = []

        for station in all_stations:
            stationid = station[0] ## station id
            stationname = station[1] ## station name
            stationserialnumber = station[2] ## serial number 
            stationmodelid = station[3] ## station model ID
            stationoperatorid = station[5] ## station operator ID 
            
            cursor = conn.cursor()
            cursor.execute('SELECT stationlatitude, stationlongitude FROM stationlocation WHERE id = %s', (stationid,))
            longlat = cursor.fetchall() ## get the long lang for the current station
            
            cursor.execute('SELECT brandname FROM stationmodel WHERE id = %s', (stationmodelid,))
            model = cursor.fetchone()[0] ## get the brand name of the model
            
            
            cursor.execute('SELECT operatorname, operatorweb, operatoremail FROM stationoperator WHERE id = %s', (stationoperatorid,))
            operator = cursor.fetchall() ## get the operator name
            
            
            record = {
                'id': stationid,
                'name': stationname,## name
                'serialNumber': stationserialnumber, ## serial number 
                'model': model, # brand name of the model
                'latitude': longlat[0][0], ## LATIDUTE
                'longitude':longlat[0][1], ## LONGITUDE
                'operator': operator[0][0], ## operator name
                'operatorWeb': operator[0][1], ## operator website
                'operatorEmail': operator[0][2], ## operator email
                'contactEmail': '', ## empty for now
                'contactPhone': '',
            }
            result.append(record)

        cursor.close()
        return jsonify(result)
    
    
@Station_apis.route('/api/stations/<stationName>')
class StationInfo(Resource):
    @api.doc(description='Get all info about a specific station we have in the database')
    @auth.login_required
    def get(self, stationName):
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM airqualitystation WHERE stationname = %s', (stationName,))
        stationinfo = cursor.fetchone() ## get all info about the station in the database
        
        print(stationinfo)
        stationId = stationinfo[0] ## station Id
        stationserialnumber = stationinfo[2] ## serial number 
        stationmodelid = stationinfo[3] ## station model ID
        stationoperatorid = stationinfo[5] ## station operator ID 
        
        cursor = conn.cursor()
        cursor.execute('SELECT stationlatitude, stationlongitude FROM stationlocation WHERE id = %s', (stationId,))
        longlat = cursor.fetchall() ## get the long lang for the current station
        
        cursor.execute('SELECT brandname FROM stationmodel WHERE id = %s', (stationmodelid,))
        model = cursor.fetchone()[0] ## get the brand name of the model
        
        
        cursor.execute('SELECT operatorname, operatorweb, operatoremail FROM stationoperator WHERE id = %s', (stationoperatorid,))
        operator = cursor.fetchall() ## get the operator name
            
            
        cursor.close()
        return jsonify({
            'id': stationId,
            'name': stationName,## name
            'serialNumber': stationserialnumber, ## serial number 
            'model': model, # brand name of the model
            'latitude': longlat[0][0], ## LATIDUTE
            'longitude':longlat[0][1], ## LONGITUDE
            'operator': operator[0][0], ## operator name
            'operatorWeb': operator[0][1], ## operator website
            'operatorEmail': operator[0][2], ## operator email
            'contactEmail': '', ## empty for now
            'contactPhone': '',
        })