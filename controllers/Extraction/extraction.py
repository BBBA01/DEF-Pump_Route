import requests
import pandas as pd
from config.config import url

def Extracting(Product_Type):

    response = requests.get(url)
    data = response.json()

    # convert the JSON data to a Pandas DataFrame
    df = pd.DataFrame(data,columns=['productTypeName','officeName',"latitude","longitude","avgSales"])

    capacity=[]
    currentStock=[]

    for x in data:
        total_capacity=0
        total_current_stock=0 
    
        for y in x["godownProducts"]:
            total_capacity=total_capacity+y["capacity"]
            y["currentStock"]=y["currentStock"] if y["currentStock"] else 0
            total_current_stock=total_current_stock+y["currentStock"]
        currentStock.append(total_current_stock)
        capacity.append(total_capacity)
    
    df = df.assign(currentStock=currentStock,totalCapacity=capacity)
    df["requirement%"]=(abs(df["totalCapacity"]-df["currentStock"])/df["totalCapacity"])*100

    df=df[df["productTypeName"]==Product_Type]
    df.reset_index(inplace=True,drop=True)
    for i in range(len(df)):
        df.loc[i,"requirement%"]=100 if df.loc[i,"requirement%"]>100 else df.loc[i,"requirement%"]

    df.dropna(inplace=True)
    df.reset_index(inplace=True,drop=True)
    df.sort_values(by="requirement%",inplace=True,ascending=False)
    df.reset_index(inplace=True,drop=True)

    return df