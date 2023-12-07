from flask_restx import Resource, Namespace, reqparse
from .extensions import api
from .authentication import auth
from flask import jsonify
import psycopg2

specifiedDates_apis= Namespace('specifiedDates')

conn = psycopg2.connect(database="ExEa_main", user="postgres", password="mohi1234", host="localhost", port="5432")


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
        