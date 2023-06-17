import pandas as pd
from controllers.DistanceAway.distanceaway import haversine


def Route_plan_without_priority(df,startingPoint,startingPointId,startingLatitude,startingLongitude):

    distance_matrix = []
    lat_lon_office=df[["latitude","longitude","officeName","AtDeliveryRequirement","officeId"]]
    lat_lon_office.loc[len(lat_lon_office.index)]=[startingLatitude,startingLongitude,startingPoint,0,startingPointId]
    lat_lon_office.reset_index(drop=True,inplace=True)


    for i in range(len(lat_lon_office)-1,-1,-1):
        temp=[]
        for j in range(len(lat_lon_office)-1,-1,-1):
            temp.append(haversine(float(lat_lon_office.loc[i,"latitude"]), float(lat_lon_office.loc[i,"longitude"]),float(lat_lon_office.loc[j,"latitude"]), float(lat_lon_office.loc[j,"longitude"])))
        distance_matrix.append(temp)


    stops=list(lat_lon_office["officeName"][::-1])

    distance_matrix_df=pd.DataFrame(data=distance_matrix,index=stops,columns=stops)
    start=startingPoint
    distance_matrix_df.drop(columns=start,inplace=True)
    optimal_route=[start]
    min_distance=0
    for i in range(len(distance_matrix_df)-1):
        optimal_route.append(distance_matrix_df.loc[start].idxmin())
        min_distance+=distance_matrix_df.loc[start].min()
        start=distance_matrix_df.loc[start].idxmin()
        distance_matrix_df.drop(columns=start,inplace=True)

    min_distance+=df.loc[df["officeName"]==optimal_route[-1]]["distanceAwayFromStartingPoint"].values[0]
    optimal_route.append(startingPoint)

    # optimal_route add latitude and longitude
    new_df=pd.DataFrame(columns=lat_lon_office.columns)
    for i in range(len(optimal_route)):
        optimal_route[i]=lat_lon_office.loc[lat_lon_office["officeName"]==optimal_route[i]]
        optimal_route[i]=optimal_route[i].values[0]
        new_df.loc[i]=list(optimal_route[i])
       
    return new_df.to_dict(orient='records') ,min_distance
