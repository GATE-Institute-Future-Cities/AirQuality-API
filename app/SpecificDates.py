from flask_restx import Resource, Namespace, reqparse
from .extensions import api
from .authentication import auth
from flask import jsonify
import psycopg2

specifiedDates_apis= Namespace('specifiedDates')

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

# parser to parse the 'selectedElements' query parameter as a list of integers
parser = reqparse.RequestParser()
parser.add_argument('selectedElements', type=int, action='split', help='List of selected element ids', required=True)

@specifiedDates_apis.route('/api/telemetry/values/<stationName>/<timeframe>')
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
        
        
@specifiedDates_apis.route('/api/telemetry/values/<elementAbbreviation>/<stationName>/<fromDate>/<toDate>')
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