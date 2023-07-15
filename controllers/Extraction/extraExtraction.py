import pandas as pd
import warnings
warnings.filterwarnings('ignore')

def ExtractingFromDeliveryPlan(DeliveryPlanId,cnxn):

    df=pd.read_sql_query(f'''
                         SELECT
   df.OfficeId,
    df.OfficeName,
   df.Longitude,
    df.Latitude,
    
	d.ContainerSize,
	d.StartPointID,
	d.StartLatitude,
	d.StartLongitude,
	d.DeliveryPlanId,
    d.HubName,
    d.PlannedQuantity,d.CurrentQuantity,d.AvailableQuantity
FROM
    Office df

    Inner JOIN(
    Select dpd.DeliveryPlanId,dpd.OfficeId,dpd.PlannedQuantity,dpd.CurrentQuantity,dpd.AvailableQuantity,dpd.ApproveStatus,
    dp.StartPointId,dp.ContainerSize,M.Latitude As StartLatitude,
    M.Longitude As StartLongitude,M.HubName

    from DeliveryPlanDetails dpd

    left join
    DeliveryPlan dp

    on dpd.DeliveryPlanId=dp.DeliveryPlanId

    left join
    Hub M

    on dp.StartPointId=M.HubId
	
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
                "PlannedQuantity": "atDeliveryRequirement"
            },
            inplace=True,
        )
  # if totalCapacity value is 0 then replace it to 2000
    if (len(df)>0):
        df["totalCapacity"]=df["availableQuantity"]+df["currentStock"]
        df[["currentStock","totalCapacity"]].fillna(0,inplace=True)
        df["totalCapacity"].replace(to_replace = 0,value = 2000,inplace=True)
        

        df[["latitude","longitude"]].dropna(inplace=True)
        df.reset_index(inplace=True,drop=True)

        Starting_PointId = df["StartPointID"][0]
        Starting_PointName = df["HubName"][0]
        Starting_Point_latitude = df["StartLatitude"][0]
        Starting_Point_longitude = df["StartLongitude"][0]
        total_requirement=df['atDeliveryRequirement'].dropna().sum()
        excess_capacity= df["ContainerSize"][0]-total_requirement

        return df,Starting_PointId,Starting_PointName,Starting_Point_latitude,Starting_Point_longitude,total_requirement,excess_capacity
    else:
        return df,0,0,0,0,0,0