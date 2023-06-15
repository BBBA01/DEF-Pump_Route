from flask import Flask
from routes.route import route_page

app = Flask(__name__)
app.register_blueprint(route_page)
app.run(debug=False)