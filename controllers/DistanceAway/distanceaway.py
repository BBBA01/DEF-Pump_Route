from math import radians, sin, cos, sqrt, atan2

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers

    # Convert latitude and longitude to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Calculate the differences between the latitudes and longitudes
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Apply the Haversine formula
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c

    return distance


def DistanceAwayFromStartingPoint(df,lat1,lon1):
    distance=[]
    for i in range(len(df)):
        distance.append(haversine(lat1, lon1, float(df.loc[i,"latitude"]), float(df.loc[i,"longitude"])))
    df = df.assign(distanceAwayFromStartingPoint=distance)
    return df
