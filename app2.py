from flask import Flask
import os
from flask_sqlalchemy import SQLAlchemy
from flask_restx import Api
from flask_httpauth import HTTPDigestAuth
from flask_restx import Resource, Namespace, reqparse
from flask import jsonify
import psycopg2

api = Api(title='AirQuality')
db = SQLAlchemy()
auth = HTTPDigestAuth()
    
app = Flask(__name__) 

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


@Airquality_apis.route('/api/stations')
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
        
# parser to parse the 'selectedElements' query parameter as a list of integers
parser = reqparse.RequestParser()
parser.add_argument('selectedElements', type=int, action='split', help='List of selected element ids', required=True)

@Airquality_apis.route('/api/telemetry/values/<stationName>/<timeframe>')
class SelectedData(Resource):
    @api.doc(description='retrives information about AQ values for a list of specified elements in the specified timeframe')
    @api.expect(parser)
    @auth.login_required
    def get(self, stationName, timeframe):
        
        cursor = conn.cursor()
        args = parser.parse_args()
        selectedElements = args['selectedElements'] ## list of the chosen element ids
        
        stationId = getStationId(stationName) ##station id
        
        all_elements = get_paramName(selectedElements) ## selected elements is a list of ids for the specified elements we call to func to covert to param abreviation
                
        values = []
        for elId in selectedElements:
             cursor.execute('SELECT measuredvalue FROM airqualityobserved WHERE measuredparameterid = %s AND measurementdatetime = %s AND stationid = %s', (elId, timeframe, stationId))
             value = cursor.fetchone()[0]
             values.append(value)
            
        cursor.close()
        return jsonify({
            'dateTime': timeframe,
            'elements': all_elements,
            'values': values,
        })
        
@Airquality_apis.route('/api/telemetry/values/<elementAbbreviation>/<stationName>/<fromDate>/<toDate>')
class SelectedDatetime(Resource):
    @api.doc(description = 'get specific element values fromdate todate included')
    @auth.login_required
    def get(self, elementAbbreviation, stationName, fromDate, toDate):
        cursor = conn.cursor()
        
        stationId = getStationId(stationName) ## station id
        
        cursor.execute('SELECT id FROM parametertype WHERE parameterabbreviation = %s', (elementAbbreviation,))
        elementId = cursor.fetchone()[0]
        
        cursor.execute('SELECT parametername FROM parametertype WHERE parameterabbreviation = %s', (elementAbbreviation,) )
        elementName = cursor.fetchone()[0]
        
        cursor.execute('SELECT measurementdatetime, measuredvalue FROM airqualityobserved WHERE stationid = %s AND measuredparameterid = %s AND measurementdatetime BETWEEN %s AND %s', (stationId, elementId, fromDate, toDate))
        data = cursor.fetchall()
        
        return jsonify({
            'element': elementName,
            'fromDate': fromDate,
            'toDate': toDate,
            'values': data,
            
        })
        
@Airquality_apis.route('/api/telemetry/values/elements/<stationName>/<fromDate>/<toDate>')
class SelectedDatetimeValues(Resource):
    @api.doc(description = 'get elements values fromdate todate included')
    @api.expect(parser)
    @auth.login_required
    def get(self, stationName, fromDate, toDate):
        
        cursor = conn.cursor()
        
        stationId = getStationId(stationName)
        
        args = parser.parse_args()
        selectedElements = args['selectedElements'] ## list of the chosen element ids
        
        values = {}     
        for el in selectedElements:
            
            cursor.execute('SELECT measuredvalue FROM airqualityobserved WHERE stationid = %s AND measuredparameterid = %s AND measurementdatetime BETWEEN %s AND %s', (stationId, el, fromDate, toDate))
            data = cursor.fetchall() ## get the values for the specified element which is el in between the dates inserted including the value from the dates
            
            cursor.execute('SELECT parameterabbreviation FROM parametertype WHERE id = %s', (el,) )
            elementName = cursor.fetchone()[0] ## getting the name abbriviation
            

            if elementName not in values: ##simple check if the elements is in dict or not
                values[elementName] = []
                
            values[elementName].extend([d[0] for d in data]) ## getting the values as ints instead of a list with the value
            
            
        return jsonify({
            'FromDate': fromDate,
            'ToDate': toDate,
            'Data': values,
        })
            
        
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:mohi1234@localhost/ExEa_main'

api.init_app(app)
db.init_app(app)
api.add_namespace(Airquality_apis)

app.config['SECRET_KEY'] = os.urandom(24) ## random secret KEY


users= {
    'mo': '123',
}

@auth.get_password
def get_pw(username):
    if username in users:
        return users.get(username)
    return None

if __name__ == "__main__":
    app.run()
