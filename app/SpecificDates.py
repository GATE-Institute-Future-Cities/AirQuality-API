from flask_restx import Resource, Namespace, reqparse
from .extensions import api
from .authentication import auth
from flask import jsonify
import psycopg2
from config import db_config

specifiedDates_apis= Namespace('date')

conn = psycopg2.connect(**db_config)



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
        
@specifiedDates_apis.route('/api/telemetry/values/elements/<stationName>/<fromDate>/<toDate>')
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
            
        
@specifiedDates_apis.route('/api/telemetry/<stationName>/values/lasthour')
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