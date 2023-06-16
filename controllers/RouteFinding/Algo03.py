import pandas as pd
import numpy as np
from controllers.DistanceAway.distanceaway import haversine
import itertools

def Route_plan_with_priority_V2(df,startingPoint,startingLatitude,startingLongitude):
    final_route=[]
    global final_min_distance
    final_min_distance=0.0

    def find_route(lat_lon_office):
        distance_matrix = []
        global final_min_distance
        

        stops=list(lat_lon_office["officeName"][::-1])

        def distance_matrix(lat_lon_office):
            distance_matrix = []
            for i in range(len(lat_lon_office)-1,-1,-1):
                temp=[]
                for j in range(len(lat_lon_office)-1,-1,-1):
                    temp.append(haversine(float(lat_lon_office.loc[i,"latitude"]), float(lat_lon_office.loc[i,"longitude"]),float(lat_lon_office.loc[j,"latitude"]), float(lat_lon_office.loc[j,"longitude"])))
                distance_matrix.append(temp)
            return distance_matrix


        # Define the distance matrix
        distance_matrix = np.array(distance_matrix(lat_lon_office))
        

        # Define a function to calculate the total distance of a route
        def get_total_distance(route):
            total_distance = 0
            for i in range(len(route)-1):
                total_distance += distance_matrix[route[i], route[i+1]]
            return total_distance

        # Generate all possible permutations of stops except for the starting point
        permutations = itertools.permutations(range(1, len(stops)))

        # Calculate the total distance of each permutation and select the one with the minimum distance
        min_distance = float('inf')
        optimal_route = None

        for perm in permutations:
            # Add the starting point to the beginning of the permutation
            route_indices = [0] + list(perm)
            route = [stops[i] for i in route_indices]
            # Calculate the total distance of the route
            total_distance = get_total_distance(route_indices)
            # Update the minimum distance and optimal route if necessary
            if total_distance < min_distance:
                min_distance = total_distance
                optimal_route = route

        final_route.append(optimal_route)
        final_min_distance=final_min_distance+min_distance

    lat_lon_office_temp=pd.DataFrame(columns=df.columns)
    lat_lon_office_all=pd.DataFrame(columns=df.columns)
    lat_lon_office_all=lat_lon_office_all[["latitude","longitude","officeName","AtDeliveryRequirement"]]


    # Quartile >=75%
    for i in range(len(df)):
        if df.loc[i,"requirement%"]>=np.quantile(df["requirement%"],0.75):
            lat_lon_office_temp.loc[i]=list(df.loc[i])

    lat_lon_office_temp.reset_index(drop=True,inplace=True)
    lat_lon_office_temp=lat_lon_office_temp[["latitude","longitude","officeName","AtDeliveryRequirement"]]
    lat_lon_office_temp.loc[len(lat_lon_office_temp.index)]=[startingLatitude,startingLongitude,startingPoint,0]
    lat_lon_office_all=pd.concat([lat_lon_office_all,lat_lon_office_temp])

    find_route(lat_lon_office_temp)
    flat_final_route=list(np.concatenate(final_route).flat)

    # Quartile >=50% and <75%
    lat_lon_office_temp=pd.DataFrame(columns=df.columns)

    for i in range(len(df)):
        if df.loc[i,"requirement%"]>=np.quantile(df["requirement%"],0.50) and df.loc[i,"requirement%"]<np.quantile(df["requirement%"],0.75):
            lat_lon_office_temp.loc[i]=list(df.loc[i])

    lat_lon_office_temp.reset_index(drop=True,inplace=True)
    lat_lon_office_temp=lat_lon_office_temp[["latitude","longitude","officeName","AtDeliveryRequirement"]]
    lat_lon_office_temp.loc[len(lat_lon_office_temp.index)]=df[["latitude","longitude","officeName","AtDeliveryRequirement"]].loc[(df["officeName"]==flat_final_route[len(flat_final_route)-1])].values.tolist()[0] if len(flat_final_route)>1 else [startingLatitude,startingLongitude,startingPoint,0]
    lat_lon_office_all=pd.concat([lat_lon_office_all,lat_lon_office_temp])

    find_route(lat_lon_office_temp)
    flat_final_route=list(np.concatenate(final_route).flat)

    # Quartile <50%
    lat_lon_office_temp=pd.DataFrame(columns=df.columns)

    for i in range(len(df)):
        if df.loc[i,"requirement%"]<np.quantile(df["requirement%"],0.50):
            lat_lon_office_temp.loc[i]=list(df.loc[i])

    lat_lon_office_temp.reset_index(drop=True,inplace=True)
    lat_lon_office_temp=lat_lon_office_temp[["latitude","longitude","officeName","AtDeliveryRequirement"]]
    lat_lon_office_temp.loc[len(lat_lon_office_temp.index)]=df[["latitude","longitude","officeName","AtDeliveryRequirement"]].loc[(df["officeName"]==flat_final_route[len(flat_final_route)-1])].values.tolist()[0] if len(flat_final_route)>1 else [startingLatitude,startingLongitude,startingPoint,0]
    lat_lon_office_all=pd.concat([lat_lon_office_all,lat_lon_office_temp])

    find_route(lat_lon_office_temp)
    flat_final_route=list(np.concatenate(final_route).flat)

    final_route=list(np.concatenate(final_route).flat)

    re_final_route = []
    [re_final_route.append(x) for x in final_route if x not in re_final_route]
    final_min_distance+=df.loc[df["officeName"]==re_final_route[-1]]["distanceAwayFromStartingPoint"].values[0]
    re_final_route.append(startingPoint)

    new_df=pd.DataFrame(columns=lat_lon_office_all.columns)
    for i in range(len(re_final_route)):
        re_final_route[i]=lat_lon_office_all.loc[lat_lon_office_all["officeName"]==re_final_route[i]]
        re_final_route[i]=re_final_route[i].values[0]
        new_df.loc[i]=list(re_final_route[i])


    return new_df.to_dict(orient="records"),final_min_distance
