from flask import Flask
from routes.route import route_page
from flask_swagger_ui import get_swaggerui_blueprint
from flask_cors import CORS
app = Flask(__name__)
app.json.sort_keys = False
CORS(app)


### swagger specific ###
SWAGGER_URL = ''
API_URL = '/static/swagger.json'
SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "DEF Pumps",
    }
)
app.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)
### end swagger specific ###

app.register_blueprint(route_page)
# app.run(debug=False,port=5060,host="115.124.120.251")
app.run(debug=False)