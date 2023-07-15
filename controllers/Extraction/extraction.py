import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import datetime


def Extracting( Product_Type,cnxn):
    Begindate = datetime.datetime.now()
    Begindate = Begindate.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
   
    df=pd.read_sql_query(f'''
                         SELECT
    df.OfficeId,
        df.OfficeName,
    df.Longitude,
        df.Latitude,
        df.ProductTypeId,
        df.CurrentStock,
        df.totalCapacity,
        df.avgSales

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
           Select Sum(su.Capacity) As totalCapacity,su.OfficeId From
(SELECT GM2.OfficeId, GM2.Capacity,GPM.ProductId
        FROM
            GodownMaster GM2
		Left Join
GodownProductMapper GPM ON GPM.GodownId=GM2.GodownId)su
Where su.ProductId={Product_Type}
Group By
su.OfficeId
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
    WHERE
        df.OfficeId NOT IN (
    Select 
    d.OfficeId
    FROM
    (
    SELECT
        dp.DeliveryPlanStatusId,
        dp.DeliveryPlanId,
        dp.PlanDate,
        dp.ExpectedDeliveryDate,
        dp.ProductId,
        dpd.OfficeId,
        dpd.ApproveStatus
    FROM

        DeliveryPlan dp

        left join
        DeliveryPlanDetails dpd 
        on dp.DeliveryPlanId=dpd.DeliveryPlanId

        WHERE
        dp.ProductId = {Product_Type}
        AND dp.CreatedOn <= '{Begindate}'
        AND dp.ExpectedDeliveryDate >= '{Begindate}'
        AND dp.DeliveryPlanStatusId <= 3
        AND (dpd.ApproveStatus is NULL OR dpd.ApproveStatus!=-1)
        )As d
)  ;
 ''',cnxn)
    df.rename(
            columns={
                "OfficeId": "officeId",
                "OfficeName": "officeName",
                "Longitude": "longitude",
                "Latitude": "latitude",
                "CurrentStock": "currentStock",
                "ProductTypeId": "productTypeId",
            },
            inplace=True,
        )

   

    # if totalCapacity value is 0 then replace it to 2000
    df[["currentStock", "totalCapacity"]].fillna(0, inplace=True)
    df["totalCapacity"].replace(to_replace=0, value=2000, inplace=True)

    df["requirement%"] = (
        abs(df["totalCapacity"] - df["currentStock"]) / df["totalCapacity"]
    ) * 100

    df = df[df["productTypeId"] == Product_Type]
    

    df["requirement%"].fillna(0, inplace=True)
    df.dropna(inplace=True)
    
    df.sort_values(by="requirement%", inplace=True, ascending=False)
    df.reset_index(inplace=True, drop=True)

    return df


def ExtractingFromOfficeId( Product_Type, OfficeList,cnxn,No_of_days_for_delivery,minimum_multiple):

    df=pd.read_sql_query(f'''SELECT
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
        Select Sum(su.Capacity) As totalCapacity,su.OfficeId From
(SELECT GM2.OfficeId, GM2.Capacity,GPM.ProductId
        FROM
            GodownMaster GM2
		Left Join
GodownProductMapper GPM ON GPM.GodownId=GM2.GodownId)su
Where su.ProductId={Product_Type}
Group By
su.OfficeId
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
	Where
o.OfficeId IN {tuple(OfficeList) if len(OfficeList)>1 else f"('{OfficeList[0]}')"}

    ;
''',cnxn)
    df.rename(
            columns={
                "OfficeId": "officeId",
                "OfficeName": "officeName",
                "Longitude": "longitude",
                "Latitude": "latitude",
                "CurrentStock": "currentStock",
                "ProductTypeId": "productTypeId",
            },
            inplace=True,
        )
    # if totalCapacity value is 0 then replace it to 2000
    df[["currentStock", "totalCapacity"]].fillna(0, inplace=True)
    df["totalCapacity"].replace(to_replace=0, value=2000, inplace=True)


    df["requirement%"] = (
        abs(df["totalCapacity"] - df["currentStock"]) / df["totalCapacity"]
    ) * 100

    df = df[df["productTypeId"] == Product_Type]
   

    df["requirement%"].fillna(0, inplace=True)
    df.dropna(inplace=True)
    
    df.sort_values(by="requirement%", inplace=True, ascending=False)
    df.reset_index(inplace=True, drop=True)

    df2=Extracting( Product_Type,cnxn)

    Not_selected=pd.merge(df2,df,indicator=True,how='outer').query('_merge=="left_only"').drop('_merge',axis=1)
    Not_selected["atDeliveryRequirement"]=Not_selected["totalCapacity"]-Not_selected["currentStock"]+Not_selected["avgSales"]*No_of_days_for_delivery 
    Not_selected["atDeliveryRequirement"] = Not_selected.apply(lambda row: row["totalCapacity"] if row["atDeliveryRequirement"] > row["totalCapacity"] else row["atDeliveryRequirement"], axis=1)

    Not_selected["requirement%"]=Not_selected["atDeliveryRequirement"]/Not_selected["totalCapacity"]*100
    Not_selected["requirement%"].fillna(0,inplace=True)
    Not_selected["atDeliveryRequirement"]= (Not_selected["atDeliveryRequirement"]//minimum_multiple)*minimum_multiple
    Not_selected["currentStock"]=Not_selected["currentStock"]-Not_selected["avgSales"]*No_of_days_for_delivery
    Not_selected["availableQuantity"]=Not_selected["totalCapacity"]-Not_selected["currentStock"]
    Not_selected["atDeliveryRequirement"].replace(to_replace=0, value=minimum_multiple, inplace=True)
                                     
    
    Not_selected.sort_values(by="requirement%",inplace=True,ascending=False)
    Not_selected.reset_index(drop=True,inplace=True)

    return df,Not_selected[["officeName","latitude","longitude","atDeliveryRequirement","officeId","totalCapacity","currentStock","availableQuantity"]]
