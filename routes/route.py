from flask import jsonify, request
import requests
import pandas as pd
from controllers.Extraction.extraction import Extracting
from controllers.DistanceAway.distanceaway import DistanceAwayFromStartingPoint
from controllers.Filteration.filtration import Filtering
from controllers.RouteFinding.Algo01 import Route_plan_without_priority
from controllers.RouteFinding.Algo02 import Route_plan_with_priority_V1
from controllers.RouteFinding.Algo03 import Route_plan_with_priority_V2
from flask import Blueprint
from config.config import ManufacturingHuburl

route_page = Blueprint("simple_page", __name__)


@route_page.route("/api/v1/route_plan", methods=["POST"])
def create_post():
    Product_Type = request.json["Product_Type"]
    Starting_Point = request.json["Starting_Point"]
    Tank_Capacity = request.json["Tank_Capacity"]
    minimum_multiple = request.json["minimum_multiple"]
    No_of_days_for_delivery = request.json["No_of_days_for_delivery"]
    response = requests.get(ManufacturingHuburl)
    data = response.json()

    # convert the JSON data to a Pandas DataFrame
    Starting_Point_df = pd.DataFrame(data)
    Starting_Point_latitude = float(
        Starting_Point_df.loc[
            Starting_Point_df["cityName"] == Starting_Point, "latitude"
        ].values[0]
    )
    Starting_Point_longitude = float(
        Starting_Point_df.loc[
            Starting_Point_df["cityName"] == Starting_Point, "longitude"
        ].values[0]
    )

    df = Extracting(Product_Type)
    df = DistanceAwayFromStartingPoint(
        df, Starting_Point_latitude, Starting_Point_longitude
    )
    df, total_requirement, excess_capacity, Not_selected = Filtering(
        df, Tank_Capacity, No_of_days_for_delivery, minimum_multiple
    )

    optimal_route1 = Route_plan_without_priority(
        df, Starting_Point, Starting_Point_latitude, Starting_Point_longitude
    )
    optimal_route2 = Route_plan_with_priority_V1(
        df, Starting_Point, Starting_Point_latitude, Starting_Point_longitude
    )
    optimal_route3 = Route_plan_with_priority_V2(
        df, Starting_Point, Starting_Point_latitude, Starting_Point_longitude
    )

    return jsonify(
        Total_requirement=total_requirement,
        Excess_capacity=excess_capacity,
        Not_selected=Not_selected,
        Routes={
            "Algorithm_1": {
                "Description": "Routing based on Nearest Branch",
                "Route": optimal_route1[0],
                "Total_distance": optimal_route1[1],
            },
            "Algorithm_2": {
                "Description": "Routing based on Requirement Priority V1",
                "Route": optimal_route2[0],
                "Total_distance": optimal_route2[1],
            },
            "Algorithm_3": {
                "Description": "Routing based on Requirement Priority V2",
                "Route": optimal_route3[0],
                "Total_distance": optimal_route3[1],
            },
        },
    )
