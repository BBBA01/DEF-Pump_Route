from flask import jsonify, request

from controllers.Extraction.extraction import Extracting, ExtractingFromOfficeId
from controllers.Extraction.extraExtraction import ExtractingFromDeliveryPlan
from controllers.DistanceAway.distanceaway import DistanceAwayFromStartingPoint
from controllers.Filteration.filtration import Filtering
from controllers.RouteFinding.Algo01 import Route_plan_without_priority
from controllers.Fetching.fetching import Fetching

# from controllers.RouteFinding.Algo02 import Route_plan_with_priority_V1
# from controllers.RouteFinding.Algo03 import Route_plan_with_priority_V2
from flask import Blueprint

route_page = Blueprint("simple_page", __name__)


@route_page.route("/api/v1/route_plan", methods=["POST"])
def create_post():
    Product_TypeId = request.json["ProductTypeId"]
    Starting_PointId = request.json["StartingPointId"]
    Tank_Capacity = request.json["TankCapacity"]
    minimum_multiple = request.json["MinimumMultiple"]
    No_of_days_for_delivery = request.json["No_of_days_for_delivery"]
    DeliveryPlanId = request.json["DeliveryPlanId"]
    Office_list = request.json["OfficeIdList"]

   
    FetchedData_df,Starting_Point_df=Fetching(Starting_PointId)
   
    Starting_PointName = Starting_Point_df["CityName"][0]

    Starting_Point_latitude = Starting_Point_df["Latitude"][0]

    Starting_Point_longitude = Starting_Point_df["Longitude"][0]

    if len(DeliveryPlanId) > 0:
        df = ExtractingFromDeliveryPlan(FetchedData_df,Product_TypeId, DeliveryPlanId)
    elif len(Office_list) > 0:
        df = ExtractingFromOfficeId(FetchedData_df,Product_TypeId, Office_list)
    else:
        df = Extracting(FetchedData_df,Product_TypeId)

    df = DistanceAwayFromStartingPoint(
        df, Starting_Point_latitude, Starting_Point_longitude
    )
    df, total_requirement, excess_capacity, Not_selected = Filtering(
        df, Tank_Capacity, No_of_days_for_delivery, minimum_multiple
    )

    optimal_route1 = Route_plan_without_priority(
        df,
        Starting_PointName,
        str(Starting_PointId),
        Starting_Point_latitude,
        Starting_Point_longitude,
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
