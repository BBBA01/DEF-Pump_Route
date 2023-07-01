import pyodbc 
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from config.config import ConnectionString

def ExtractingFromDeliveryPlan(df,Product_Type,DeliveryPlanId):

    # if totalCapacity value is 0 then replace it to 2000
    df[["currentStock","totalCapacity"]].fillna(0,inplace=True)
    df["totalCapacity"].replace(to_replace = 0,value = 2000,inplace=True)
    cnxn = pyodbc.connect(ConnectionString)
    DeliveryPlanDetails = pd.read_sql_query(f'select OfficeId,DeliveryPlanId from DeliveryPlanDetails',cnxn)

    for i in DeliveryPlanId:
        DeliveryPlanDetails_df=DeliveryPlanDetails.loc[DeliveryPlanDetails["DeliveryPlanId"]==i]
        office_ids = DeliveryPlanDetails_df['OfficeId'].to_list()
            
        #  Select the rows where officeId is not in the office_ids
        df = df[~df["officeId"].isin(office_ids)]
    
    df.reset_index(inplace=True,drop=True)
 
    df["requirement%"]=(abs(df["totalCapacity"]-df["currentStock"])/df["totalCapacity"])*100

    df=df[df["productTypeId"]==Product_Type]
    df.reset_index(inplace=True,drop=True)
    for i in range(len(df)):
        df.loc[i,"requirement%"]=100 if df.loc[i,"requirement%"]>100 else df.loc[i,"requirement%"]

    df["requirement%"].fillna(0,inplace=True)
    df.dropna(inplace=True)
    df.reset_index(inplace=True,drop=True)
    df.sort_values(by="requirement%",inplace=True,ascending=False)
    df.reset_index(inplace=True,drop=True)

    return df