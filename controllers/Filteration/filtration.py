import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

def Filtering(df,Tank_Capacity,No_of_days_for_delivery,minimum_multiple):
    df["atDeliveryRequirement"]=df["totalCapacity"]-df["currentStock"]+df["avgSales"]*No_of_days_for_delivery
    for i in range(len(df)):
        df.loc[i,"atDeliveryRequirement"]=df.loc[i,"totalCapacity"] if df.loc[i,"atDeliveryRequirement"]>df.loc[i,"totalCapacity"] else df.loc[i,"atDeliveryRequirement"]
    df["requirement%"]=df["atDeliveryRequirement"]/df["totalCapacity"]*100
    df["requirement%"].fillna(0,inplace=True)
    rest_df=df.loc[df["atDeliveryRequirement"] < minimum_multiple]
    df=df.loc[df["atDeliveryRequirement"] >= minimum_multiple]
                                     
    df["atDeliveryRequirement"]= (df["atDeliveryRequirement"]//minimum_multiple)*minimum_multiple
    
    df.sort_values(by="requirement%",inplace=True,ascending=False)
    df.reset_index(drop=True,inplace=True)

    Update_df=pd.DataFrame(columns=df.columns)
    total_requirement=0
    for i in range(len(df)):
        if Tank_Capacity>=total_requirement+df.loc[i,"atDeliveryRequirement"]:
            total_requirement=total_requirement+df.loc[i,"atDeliveryRequirement"]
            Update_df.loc[i]=df.loc[i]

    Not_selected=pd.concat([df,Update_df]).drop_duplicates(keep=False)
    rest_df.sort_values(by="requirement%",inplace=True,ascending=False,ignore_index=True)
    Not_selected=pd.concat([Not_selected,rest_df],ignore_index=True)
    # Not_selected.sort_values(by="requirement%",inplace=True,ascending=False,ignore_index=True)
    df=Update_df
    df.reset_index(drop=True,inplace=True)
    df.sort_values(by="requirement%",inplace=True,ascending=False)
    total_requirement=sum(df["atDeliveryRequirement"])
    excess_capacity=Tank_Capacity-total_requirement

    return df,total_requirement,excess_capacity,Not_selected[["officeName","latitude","longitude","atDeliveryRequirement","officeId","totalCapacity","currentStock"]].to_dict(orient="records")
