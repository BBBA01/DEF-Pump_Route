import pandas as pd
import numpy as np
from controllers.DistanceAway.distanceaway import haversine
import itertools

def Route_plan_without_priority(df,startingPoint,startingLatitude,startingLongitude):

    distance_matrix = []
    lat_lon_office=df[["latitude","longitude","officeName","AtDeliveryRequirement"]]
    lat_lon_office.loc[len(lat_lon_office.index)]=[startingLatitude,startingLongitude,startingPoint,0]
    lat_lon_office.reset_index(drop=True,inplace=True)


    for i in range(len(lat_lon_office)-1,-1,-1):
        temp=[]
        for j in range(len(lat_lon_office)-1,-1,-1):
            temp.append(haversine(float(lat_lon_office.loc[i,"latitude"]), float(lat_lon_office.loc[i,"longitude"]),float(lat_lon_office.loc[j,"latitude"]), float(lat_lon_office.loc[j,"longitude"])))
        distance_matrix.append(temp)


    stops=list(lat_lon_office["officeName"][::-1])
    # Define the distance matrix
    distance_matrix = np.array(distance_matrix)

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

    min_distance+=df.loc[df["officeName"]==optimal_route[-1]]["distanceAwayFromStartingPoint"].values[0]
    optimal_route.append(startingPoint)

    # optimal_route add latitude and longitude
    new_df=pd.DataFrame(columns=lat_lon_office.columns)
    for i in range(len(optimal_route)):
        optimal_route[i]=lat_lon_office.loc[lat_lon_office["officeName"]==optimal_route[i]]
        optimal_route[i]=optimal_route[i].values[0]
        new_df.loc[i]=list(optimal_route[i])

    #change the datatype of latitude and longitude to float in new_df dataframe
    new_df["latitude"]=new_df["latitude"].astype(float)
    new_df["longitude"]=new_df["longitude"].astype(float)
       
    return new_df.to_dict(orient='records') ,min_distance
