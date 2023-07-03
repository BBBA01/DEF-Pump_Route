import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from flask import Response

def ExtractingFromDeliveryPlan(DeliveryPlanId,cnxn):

    df=pd.read_sql_query(f'''
                         SELECT
   df.OfficeId,
    df.OfficeName,
   df.Longitude,
    df.Latitude,
    df.ProductTypeId,
    df.CurrentStock,
    df.totalCapacity,
    df.avgSales,
	d.ContainerSize,
	d.StartPointID,
	d.StartLatitude,
	d.StartLongitude,
	d.DeliveryPlanId,
    d.CityName,
    d.PlannedQuantity,d.CurrentQuantity,d.AvailableQuantity
FROM(
SELECT
    o.OfficeId,
    o.OfficeName,
    o.Longitude,
    o.Latitude,
    cs.ProductTypeId,
    cs.CurrentStock,
    gm.totalCapacity,
    s.avgSales
FROM
    Office o
LEFT JOIN
    CurrentStockDetails cs ON o.OfficeId = cs.OfficeId
LEFT JOIN
    (
        SELECT
            OfficeId,
            SUM(Capacity) AS totalCapacity
        FROM
            GodownMaster
        GROUP BY
            OfficeId
    ) gm ON o.OfficeId = gm.OfficeId
LEFT JOIN
    (
        SELECT
            OfficeId,
            AVG(Quantity) AS avgSales
        FROM
            Sales
        WHERE
            Total > 0
        GROUP BY
            OfficeId
    ) s ON o.OfficeId = s.OfficeId
	)df

    Inner JOIN(
    Select dpd.DeliveryPlanId,dpd.OfficeId,dpd.PlannedQuantity,dpd.CurrentQuantity,dpd.AvailableQuantity,dpd.ApproveStatus,
    dp.StartPointId,dp.ContainerSize,M.Latitude As StartLatitude,
    M.Longitude As StartLongitude,M.CityName

    from DeliveryPlanDetails dpd

    left join
    DeliveryPlan dp

    on dpd.DeliveryPlanId=dp.DeliveryPlanId

    left join
    CityMaster M

    on dp.StartPointId=M.CityId
    Where 
    dp.DeliveryPlanId={DeliveryPlanId} And
    (ApproveStatus IS Null OR ApproveStatus!=-1)
        )As d
    On d.OfficeId=df.OfficeId  
;
                         ''',cnxn)
    df.rename(
            columns={
                "OfficeId": "officeId",
                "OfficeName": "officeName",
                "Longitude": "longitude",
                "Latitude": "latitude",
                "CurrentQuantity": "currentStock",
                "AvailableQuantity": "availableQuantity",
                "PlannedQuantity": "availableQuantity",
                "PlannedQuantity": "atDeliveryRequirement",
                "ProductTypeId": "productTypeId",
            },
            inplace=True,
        )
  # if totalCapacity value is 0 then replace it to 2000
    if (len(df)>0):
        df[["currentStock","totalCapacity"]].fillna(0,inplace=True)
        df["totalCapacity"].replace(to_replace = 0,value = 2000,inplace=True)
        

        df[["latitude","longitude"]].dropna(inplace=True)
        df.reset_index(inplace=True,drop=True)

        Starting_PointId = df["StartPointID"][0]
        Starting_PointName = df["CityName"][0]
        Starting_Point_latitude = df["StartLatitude"][0]
        Starting_Point_longitude = df["StartLongitude"][0]
        total_requirement=df['atDeliveryRequirement'].dropna().sum()
        excess_capacity= df["ContainerSize"][0]-total_requirement

        return df,Starting_PointId,Starting_PointName,Starting_Point_latitude,Starting_Point_longitude,total_requirement,excess_capacity
    else:
        return df,0,0,0,0,0,0