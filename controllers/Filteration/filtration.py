import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

def Filtering(df,Tank_Capacity,No_of_days_for_delivery,minimum_multiple):
    df["AtDeliveryRequirement"]=df["totalCapacity"]-df["currentStock"]+df["avgSales"]*No_of_days_for_delivery
    for i in range(len(df)):
        df.loc[i,"AtDeliveryRequirement"]=df.loc[i,"totalCapacity"] if df.loc[i,"AtDeliveryRequirement"]>df.loc[i,"totalCapacity"] else df.loc[i,"AtDeliveryRequirement"]
    df["requirement%"]=df["AtDeliveryRequirement"]/df["totalCapacity"]*100
    df=df.loc[df["AtDeliveryRequirement"] >= minimum_multiple]
                                     
    df["AtDeliveryRequirement"]= (df["AtDeliveryRequirement"]//minimum_multiple)*minimum_multiple
    
    df.sort_values(by="requirement%",inplace=True,ascending=False)
    df.reset_index(drop=True,inplace=True)

    Update_df=pd.DataFrame(columns=df.columns)
    total_requirement=0
    for i in range(len(df)):
        if Tank_Capacity>total_requirement+df.loc[i,"AtDeliveryRequirement"]:
            total_requirement=total_requirement+df.loc[i,"AtDeliveryRequirement"]
            Update_df.loc[i]=df.loc[i]

    Not_selected=pd.concat([df,Update_df]).drop_duplicates(keep=False)
    df=Update_df
    df.reset_index(drop=True,inplace=True)
    df.sort_values(by="requirement%",inplace=True,ascending=False)
    total_requirement=sum(df["AtDeliveryRequirement"])
    excess_capacity=Tank_Capacity-total_requirement

    return df,total_requirement,excess_capacity,Not_selected[["officeName","latitude","longitude","AtDeliveryRequirement","officeId"]].to_dict(orient="records")
