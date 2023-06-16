from flask import Flask
from routes.route import route_page

app = Flask(__name__)
app.register_blueprint(route_page)
# app.run(debug=False,port=5060,host="115.124.120.251")
app.run(debug=False)