from flask import jsonify, request
import pandas as pd
import pyodbc
import warnings

warnings.filterwarnings("ignore")
from config.config import ConnectionString

from controllers.Extraction.extraction import Extracting, ExtractingFromOfficeId
from controllers.Extraction.extraExtraction import ExtractingFromDeliveryPlan
from controllers.Dashboard.Sales.dropdown_list import dropdown_list
from controllers.Dashboard.Sales.sales_list import sales_based_on_admin
from controllers.Dashboard.Sales.card_details import CardDetails
from controllers.Filteration.filtration import Filtering
from controllers.RouteFinding.Algo01 import Route_plan_without_priority

from flask import Blueprint

route_page = Blueprint("simple_page", __name__)


@route_page.route("/api/v1/route_plan", methods=["POST"])
def create_post():
    request_data = request.get_json()

    Product_TypeId = None
    Starting_PointId = None
    Tank_Capacity = None
    minimum_multiple = None
    No_of_days_for_delivery = None
    DeliveryPlanId = None
    Office_list = None
    Starting_PointName = None
    Starting_Point_latitude = None
    Starting_Point_longitude = None

    if request_data:
        if "ProductTypeId" in request_data:
            Product_TypeId = request_data["ProductTypeId"]

        if "StartingPointId" in request_data:
            Starting_PointId = request_data["StartingPointId"]

        if "TankCapacity" in request_data:
            Tank_Capacity = request_data["TankCapacity"]

        if "MinimumMultiple" in request_data:
            minimum_multiple = request_data["MinimumMultiple"]

        if "No_of_days_for_delivery" in request_data:
            No_of_days_for_delivery = request_data["No_of_days_for_delivery"]

        if "DeliveryPlanId" in request_data:
            DeliveryPlanId = request_data["DeliveryPlanId"]

        if "OfficeIdList" in request_data:
            Office_list = request_data["OfficeIdList"]

    cnxn = pyodbc.connect(ConnectionString)

    if DeliveryPlanId:
        (
            df,
            Starting_PointId,
            Starting_PointName,
            Starting_Point_latitude,
            Starting_Point_longitude,
            total_requirement,
            excess_capacity,
        ) = ExtractingFromDeliveryPlan(DeliveryPlanId, cnxn)
        Not_selected = []

    elif Office_list and len(Office_list) > 0:
        df, Not_selected2 = ExtractingFromOfficeId(
            Product_TypeId, Office_list, cnxn, No_of_days_for_delivery, minimum_multiple
        )

        df, total_requirement, excess_capacity, Not_selected = Filtering(
            df, Tank_Capacity, No_of_days_for_delivery, minimum_multiple
        )
        Not_selected = pd.merge(Not_selected, Not_selected2, how="outer")
        Not_selected = Not_selected.to_dict(orient="records")

        Starting_Point_df = pd.read_sql_query(
            f"Select HubName,Latitude,Longitude from Hub where HubId={Starting_PointId}",
            cnxn,
        )
        Starting_PointName = Starting_Point_df["HubName"][0]
        Starting_Point_latitude = Starting_Point_df["Latitude"][0]
        Starting_Point_longitude = Starting_Point_df["Longitude"][0]
    else:
        df = Extracting(Product_TypeId, cnxn)

        df, total_requirement, excess_capacity, Not_selected = Filtering(
            df, Tank_Capacity, No_of_days_for_delivery, minimum_multiple
        )
        Not_selected = Not_selected.to_dict(orient="records")
        Starting_Point_df = pd.read_sql_query(
            f"Select HubName,Latitude,Longitude from Hub where HubId={Starting_PointId}",
            cnxn,
        )
        Starting_PointName = Starting_Point_df["HubName"][0]
        Starting_Point_latitude = Starting_Point_df["Latitude"][0]
        Starting_Point_longitude = Starting_Point_df["Longitude"][0]

    cnxn.close()

    optimal_route1 = Route_plan_without_priority(
        df,
        Starting_PointName,
        str(Starting_PointId),
        Starting_Point_latitude,
        Starting_Point_longitude,
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
        },
    )


@route_page.route("/api/v1/dashboard/dropdown_list/<string:_UserId>", methods=["GET"])
def dropdown(_UserId):
    user_id = _UserId
  
    cnxn = pyodbc.connect(ConnectionString)
    df = dropdown_list(user_id,cnxn)
    cnxn.close()
    return jsonify(df.to_dict(orient="records"))


@route_page.route("/api/v1/dashboard/sales_list/<string:_FromDate>/<string:_ToDate>/<string:_OfficeId>/<string:_IsAdmin>", methods=["GET"])
def sales_list(_FromDate,_ToDate,_OfficeId,_IsAdmin):
    from_date = _FromDate
    to_date = _ToDate
    office_id = _OfficeId
    is_admin = int(_IsAdmin)

    cnxn = pyodbc.connect(ConnectionString)
    df,product_type_list = sales_based_on_admin(office_id,is_admin,from_date,to_date,cnxn)
    cnxn.close()
    return jsonify({"graph1":df,"graph2":product_type_list})

@route_page.route("/api/v1/dashboard/card_details_list/<string:_OfficeId>/<string:_IsAdmin>", methods=["GET"])
def card_details_list(_OfficeId,_IsAdmin):

    office_id = _OfficeId
    is_admin = int(_IsAdmin)

    cnxn = pyodbc.connect(ConnectionString)
    json = CardDetails(office_id,is_admin,cnxn)
    cnxn.close()
    return json
