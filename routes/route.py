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
    Product_TypeId = request.json["ProductTypeId"]
    Starting_PointId = request.json["StartingPointId"]
    Tank_Capacity = request.json["TankCapacity"]
    minimum_multiple = request.json["MinimumMultiple"]
    No_of_days_for_delivery = request.json["No_of_days_for_delivery"]
    response = requests.get(ManufacturingHuburl)
    data = response.json()


    # convert the JSON data to a Pandas DataFrame
    Starting_Point_df = pd.DataFrame(data)
    Starting_PointName = Starting_Point_df.loc[
        Starting_Point_df["cityId"] == Starting_PointId, "cityName"
    ].values[0]
    
    Starting_Point_latitude =Starting_Point_df.loc[Starting_Point_df["cityId"] == Starting_PointId, "latitude"].values[0]
    
    Starting_Point_longitude = Starting_Point_df.loc[Starting_Point_df["cityId"] == Starting_PointId, "longitude"].values[0]
    

    df = Extracting(Product_TypeId)
    df = DistanceAwayFromStartingPoint(
        df, Starting_Point_latitude, Starting_Point_longitude
    )
    df, total_requirement, excess_capacity, Not_selected = Filtering(
        df, Tank_Capacity, No_of_days_for_delivery, minimum_multiple
    )

    optimal_route1 = Route_plan_without_priority(
        df, Starting_PointName,str(Starting_PointId), Starting_Point_latitude, Starting_Point_longitude
    )
    # optimal_route2 = Route_plan_with_priority_V1(
    #     df, Starting_PointName,str(Starting_PointId), Starting_Point_latitude, Starting_Point_longitude
    # )
    # optimal_route3 = Route_plan_with_priority_V2(
    #     df, Starting_PointName,str(Starting_PointId),Starting_Point_latitude, Starting_Point_longitude
    # )

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
            # "Algorithm_2": {
            #     "Description": "Routing based on Requirement Priority V1",
            #     "Route": optimal_route2[0],
            #     "Total_distance": optimal_route2[1],
            # },
            # "Algorithm_3": {
            #     "Description": "Routing based on Requirement Priority V2",
            #     "Route": optimal_route3[0],
            #     "Total_distance": optimal_route3[1],
            # },
        },
    )
