from flask_restx import Resource, Namespace, reqparse
from .extensions import api
from .authentication import auth
from flask import jsonify
import psycopg2

Airquality_apis= Namespace('airquality')

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


@Airquality_apis.route('/api/stations/<stationName>')
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

@Airquality_apis.route('/api/telemetry/<stationName>/values/lasthour')
class AllValuesLastHour(Resource):
    @api.doc(description='gets the value in the last hour for each element and for the specific station')
    @auth.login_required
    def get(self, stationName):
        cursor = conn.cursor()
        
        stationId = getStationId(stationName)
        
        cursor.execute(f'SELECT MAX(measurementdatetime) FROM airqualityobserved')
        recent_datetime = cursor.fetchone()[0] ## most recent time added to the database
        
        cursor.execute('SELECT measuredparameterid, measuredvalue FROM airqualityobserved WHERE measurementdatetime = %s AND stationid = %s', (recent_datetime, stationId))
        val_element = cursor.fetchall() ## get all the values and elements at the most recent datetime
        
        elemntIds = [el[0] for el in val_element] ## all the ids for the elements
        elementNames = get_paramName(elemntIds) ## func to get the names of the elements
        elementNames.insert(0, 'Time') ## time column is a default col
                
        values = [val[1] for val in val_element] ## all the values for each element in the most recent time
        values.insert(0, recent_datetime) ## insert the time with the values
        
        return(jsonify({
            'columns': elementNames,
            'values': values,
        }))
        
        
        
        