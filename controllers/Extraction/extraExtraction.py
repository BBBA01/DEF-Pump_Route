import requests
import pandas as pd
from config.config import url,DeliveryPlanurl

def ExtractingFromDeliveryPlan(Product_Type,DeliveryPlanId):

    response = requests.get(url)
    data = response.json()

    # convert the JSON data to a Pandas DataFrame
    df = pd.DataFrame(data,columns=['productTypeId','officeName',"latitude","longitude","avgSales","officeId"])
   

    godown_df = pd.DataFrame(data)

    # Define a function that takes a row of the dataframe as input and returns a tuple of total_capacity and total_current_stock
    def calculate_total_capacity_and_stock(row):

        if len(row["godownProducts"]):
            total_capacity = sum(list(pd.DataFrame(row["godownProducts"])["capacity"]))
            total_current_stock = sum(list(pd.DataFrame(row["godownProducts"])["currentStock"]))
        else:
            total_capacity=0
            total_current_stock=0

        return total_capacity, total_current_stock

    # Use apply() method to apply the function on each row of the dataframe and store the results in new columns
    godown_df[["totalCapacity", "totalCurrentStock"]] = godown_df.apply(calculate_total_capacity_and_stock, axis=1, result_type="expand")

    # Extract the columns into separate lists
    currentStock = godown_df["totalCurrentStock"].tolist()
    capacity = godown_df["totalCapacity"].tolist()
    
    df = df.assign(currentStock=currentStock,totalCapacity=capacity)

    for i in DeliveryPlanId:
        response2=requests.get(f"{DeliveryPlanurl}/{i}")
        data2 = response2.json()

        if len(data2["deliveryPlanDetailsList"])>0:
            DeliveryPlan_df=pd.DataFrame(data2["deliveryPlanDetailsList"])
            office_ids = DeliveryPlan_df['office'].apply(lambda x: x['officeId']).to_list()
            
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