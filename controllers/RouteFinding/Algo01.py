import pandas as pd
# import googlemaps
from controllers.DistanceAway.distanceaway import haversine

# Replace 'YOUR_API_KEY' with your actual Google Maps API key
# API_KEY = 'AIzaSyDN32TA762x19KhBZX91X4uNcmdGAhAlrQ'

# Initialize the Google Maps client
# gmaps = googlemaps.Client(key=API_KEY)


def Route_plan_without_priority(df,startingPoint,startingPointId,startingLatitude,startingLongitude):

    distance_matrix = []
    lat_lon_office=df[["latitude","longitude","officeName","atDeliveryRequirement","officeId","totalCapacity","currentStock","availableQuantity"]]
    lat_lon_office.loc[len(lat_lon_office.index)]=[startingLatitude,startingLongitude,startingPoint,0,startingPointId,0,0,0]
    lat_lon_office.reset_index(drop=True,inplace=True)
    distance_matrix = []

    for i in range(len(lat_lon_office)-1,-1,-1):
        temp=[]
        for j in range(len(lat_lon_office)-1,-1,-1):
            temp.append(haversine(float(lat_lon_office.loc[i,"latitude"]), float(lat_lon_office.loc[i,"longitude"]),float(lat_lon_office.loc[j,"latitude"]), float(lat_lon_office.loc[j,"longitude"])))
        distance_matrix.append(temp)

    #Reverse a dataframe
    lat_lon_office=lat_lon_office[::-1]

    # Extract the origins and destinations for the distance matrix from the DataFrame
    # origins = lat_lon_office[['latitude', 'longitude']].values.tolist()
    # destinations = lat_lon_office[['latitude', 'longitude']].values.tolist()

    # Make a request to the Google Distance Matrix API
    # matrix = gmaps.distance_matrix(
    #     origins=origins,
    #     destinations=destinations,
    #     mode="driving",
    #     units="metric"
    # )

    # # Extract the distance values from the matrix
    # rows = matrix["rows"]
    # distance_matrix = []
    
    # for row in rows:
    #     elements = row["elements"]
    #     row_distances = [element["distance"]["value"] / 1000 for element in elements]
    #     distance_matrix.append(row_distances)
    
    # distance_matrix_df = pd.DataFrame(distance_matrix, columns=lat_lon_office['officeName'], index=lat_lon_office['officeName'])
    distance_matrix_df2 = pd.DataFrame(distance_matrix, columns=lat_lon_office['officeName'], index=lat_lon_office['officeName'])
    optimal_route=[]
    min_distance=-1

    for x in range(len(distance_matrix_df2)-1):
        distance_matrix_df=distance_matrix_df2.copy()
        distance_matrix_df.drop(columns=startingPoint,inplace=True)
        temp_route=[startingPoint]
        start=distance_matrix_df.columns.values[x]
        temp_route.append(start)
        temp_distance=distance_matrix_df.iloc[0,x]
        distance_matrix_df.drop(columns=start,inplace=True)
        for i in range(len(distance_matrix_df)-2):
            temp_route.append(distance_matrix_df.loc[start].idxmin())
            temp_distance+=distance_matrix_df.loc[start].min()
            start=distance_matrix_df.loc[start].idxmin()
            distance_matrix_df.drop(columns=start,inplace=True)

        temp_distance+=distance_matrix_df2.loc[startingPoint,temp_route[-1]]
        temp_route.append(startingPoint)

        if min_distance>temp_distance or min_distance==-1:
            min_distance=temp_distance
            optimal_route=temp_route

    # optimal_route add latitude and longitude

    new_df=pd.DataFrame(columns=lat_lon_office.columns)
    for i in range(len(optimal_route)):
        optimal_route[i]=lat_lon_office.loc[lat_lon_office["officeName"]==optimal_route[i]]
        optimal_route[i]=optimal_route[i].values[0]
        new_df.loc[i]=list(optimal_route[i])
       
    return new_df.to_dict(orient='records') ,min_distance
    # # Get the list of locations excluding the start/end point
    # locations = distance_matrix_df.columns.tolist()
    # locations.remove(startingPoint)

    # # Generate all possible permutations of the locations
    # permutations = list(itertools.permutations(locations))

    # # Calculate the total distance for each permutation
    # total_distances = []
    # for perm in permutations:
    #     perm = list(perm)
    #     perm.insert(0, startingPoint)
    #     perm.append(startingPoint)
    #     total_distance = sum(distance_matrix_df.loc[perm[i]][perm[i+1]] for i in range(len(perm)-1))
    #     total_distances.append(total_distance)

    # # Find the index of the shortest route
    # shortest_route_index = np.argmin(total_distances)

    # # Get the shortest route
    # shortest_route = list(permutations[shortest_route_index])
    # shortest_route.insert(0, startingPoint)
    # shortest_route.append(startingPoint)

    # # Get the dataframe of the shortest route that contains latitude,longitude in proper order and allow duplicates
    # shortest_route_df = pd.DataFrame(columns=lat_lon_office.columns)
    # for i in range(len(shortest_route)):
    #     shortest_route_df = pd.concat([shortest_route_df,lat_lon_office[lat_lon_office['officeName'] == shortest_route[i]]])
    #     shortest_route_df.reset_index(drop=True, inplace=True)

       
    # return shortest_route_df.to_dict('records'),total_distances[shortest_route_index]