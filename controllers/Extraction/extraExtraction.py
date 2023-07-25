import pandas as pd
from controllers.Extraction.extraction import Extracting
import warnings
warnings.filterwarnings('ignore')

def ExtractingFromDeliveryPlan(DeliveryPlanId,cnxn,No_of_days_for_delivery):

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
    d.PlannedQuantity,d.CurrentQuantity,d.AvailableQuantity,d.ProductId,d.DeliveryLimit,
                         d.PlanDate,d.ExpectedDeliveryDate,d.DeliveryPlanStatusId,d.CreatedBy,d.UpdatedBy,d.CreatedOn,d.UpdatedOn,
                         d.DeliveryPlanDetailsId,d.SequenceNo,d.AdminId
FROM
    Office df

    Inner JOIN(
    Select dpd.DeliveryPlanId,dpd.OfficeId,dpd.PlannedQuantity,dpd.CurrentQuantity,dpd.AvailableQuantity,dpd.ApproveStatus,
    dp.StartPointId,dp.ContainerSize,M.Latitude As StartLatitude,
    M.Longitude As StartLongitude,M.HubName,dp.ProductId,dp.DeliveryLimit,
                         dp.PlanDate,dp.ExpectedDeliveryDate,dp.DeliveryPlanStatusId,dp.CreatedBy,dp.UpdatedBy,dp.CreatedOn,dp.UpdatedOn,
                         dpd.DeliveryPlanDetailsId,dpd.AdminId,dpd.SequenceNo

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
    df["totalCapacity"]=df["availableQuantity"]+df["currentStock"]
    if (len(df)>0):
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
        product_id=df["ProductId"][0]
        minimum_multiple=df["DeliveryLimit"][0]
        PlanDate=df["PlanDate"][0]
        ExpectedDeliveryDate=df["ExpectedDeliveryDate"][0]
        DeliveryPlanStatusId=df["DeliveryPlanStatusId"][0]
        CreatedBy=df["CreatedBy"][0]
        UpdatedBy=df["UpdatedBy"][0]
        CreatedOn=df["CreatedOn"][0]
        UpdatedOn=df["UpdatedOn"][0]
        # Converting the datetime object into string
        PlanDate=PlanDate.strftime("%Y-%m-%d") if PlanDate is not None else None
        ExpectedDeliveryDate=ExpectedDeliveryDate.strftime("%Y-%m-%d") if ExpectedDeliveryDate is not None else None
        CreatedOn=CreatedOn.strftime("%Y-%m-%d") if CreatedOn is not None else None 
        UpdatedOn=UpdatedOn.strftime("%Y-%m-%d") if UpdatedOn is not None else None

        No_of_days_for_delivery=0 if No_of_days_for_delivery is None else No_of_days_for_delivery

        Unselected_df=Extracting( product_id,cnxn)
        office_list=df["officeId"].to_list()
        Unselected_df=Unselected_df[~Unselected_df["officeId"].isin(office_list)]
    
        Unselected_df["atDeliveryRequirement"]=Unselected_df["totalCapacity"]-Unselected_df["currentStock"]+Unselected_df["avgSales"]*No_of_days_for_delivery 
        Unselected_df["atDeliveryRequirement"] = Unselected_df.apply(lambda row: row["totalCapacity"] if row["atDeliveryRequirement"] > row["totalCapacity"] else row["atDeliveryRequirement"], axis=1)
        Unselected_df["requirement%"]=Unselected_df["atDeliveryRequirement"]/Unselected_df["totalCapacity"]*100
        Unselected_df["requirement%"].fillna(0,inplace=True)
        Unselected_df["atDeliveryRequirement"]= (Unselected_df["atDeliveryRequirement"]//minimum_multiple)*minimum_multiple
        Unselected_df["currentStock"]=Unselected_df["currentStock"]-Unselected_df["avgSales"]*No_of_days_for_delivery
        Unselected_df["availableQuantity"]=Unselected_df["totalCapacity"]-Unselected_df["currentStock"]
        Unselected_df["atDeliveryRequirement"].replace(to_replace=0, value=minimum_multiple, inplace=True)
                                        
        
        Unselected_df.sort_values(by="requirement%",inplace=True,ascending=False)
        Unselected_df.reset_index(drop=True,inplace=True)

        return df,Starting_PointId,Starting_PointName,Starting_Point_latitude,Starting_Point_longitude,total_requirement,excess_capacity,Unselected_df[["officeName","latitude","longitude","atDeliveryRequirement","officeId","totalCapacity","currentStock","availableQuantity"]].to_dict(orient="records"),minimum_multiple,PlanDate,ExpectedDeliveryDate,int(DeliveryPlanStatusId),CreatedBy,UpdatedBy,CreatedOn,UpdatedOn
    else:
        return df,0,0,0,0,0,0,[],None,None,None,None,None,None,None,None