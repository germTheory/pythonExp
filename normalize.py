# FOR TESTING
# user1Info = [(2, 55.589200000000005, 86.13520000000001, 0), (2, 56.352700000000006, 86.66810000000001, 3), (2, 57.24620000000001, 86.79490000000001, 6), (2, 57.77470000000001, 87.48580000000001, 9), (2, 58.76540000000001, 87.5584, 12), (2, 59.495900000000006, 87.5738, 15), (2, 59.9742, 88.3344, 18), (2, 60.8667, 89.28750000000001, 21), (2, 61.1311, 89.82530000000001, 24), (2, 61.4363, 90.29540000000001, 27), (2, 61.8477, 90.58340000000001, 30), (2, 62.719, 90.82390000000001, 33), (2, 63.4465, 91.14970000000001, 36), (2, 64.2729, 92.0241, 39), (2, 64.9303, 92.0744, 42), (2, 65.61070000000001, 92.2632, 45), (2, 65.74270000000001, 93.0257, 48), (2, 66.71390000000001, 93.2251, 51), (2, 67.3375, 93.4426, 54), (2, 67.86980000000001, 94.4059, 57), (2, 68.04970000000002, 95.1175, 60), (2, 68.58890000000001, 95.2843, 63), (2, 68.91000000000001, 95.5215, 66), (2, 69.82350000000001, 96.4665, 69), (2, 70.1723, 97.10419999999999, 72), (2, 71.0129, 97.5371, 75), (2, 71.5949, 97.8959, 78), (2, 72.0593, 98.4712, 81), (2, 72.19949999999999, 98.99289999999999, 84), (2, 72.97329999999998, 99.5427, 87), (2, 73.52809999999998, 99.7894, 90), (2, 74.26309999999998, 100.1282, 93), (2, 74.47119999999998, 100.346, 96), (2, 75.28709999999998, 100.5443, 99)]
# user2Info = [(1, 24.9913, 76.0798, 0), (1, 25.5221, 76.5248, 2), (1, 25.8493, 76.8998, 4), (1, 26.6996, 77.7672, 6), (1, 27.167, 78.3833, 8), (1, 28.139200000000002, 78.44900000000001, 10), (1, 28.481, 78.74340000000001, 12), (1, 28.678900000000002, 79.26630000000002, 14), (1, 29.482300000000002, 79.99440000000001, 16), (1, 30.442400000000003, 80.54570000000001, 18), (1, 30.540100000000002, 80.87550000000002, 20), (1, 30.7266, 81.29740000000001, 22), (1, 31.206200000000003, 81.85710000000002, 24), (1, 32.196000000000005, 82.18050000000002, 26), (1, 32.9091, 82.53010000000002, 28), (1, 33.037800000000004, 83.11130000000001, 30), (1, 34.0369, 83.43020000000001, 32), (1, 34.814800000000005, 83.71950000000001, 34), (1, 35.2567, 84.382, 36), (1, 35.793400000000005, 84.589, 38), (1, 36.16910000000001, 85.0035, 40), (1, 36.325500000000005, 85.8523, 42), (1, 36.32920000000001, 86.8207, 44), (1, 37.28360000000001, 86.8394, 46), (1, 37.39600000000001, 87.45909999999999, 48), (1, 38.26080000000001, 88.38449999999999, 50), (1, 38.75630000000001, 89.27669999999999, 52), (1, 38.86470000000001, 89.49879999999999, 54), (1, 39.40100000000001, 90.30269999999999, 56), (1, 40.12380000000001, 90.75339999999998, 58), (1, 40.38810000000001, 91.12079999999999, 60), (1, 40.99080000000001, 91.73629999999999, 62), (1, 41.305400000000006, 92.48109999999998, 64), (1, 42.20360000000001, 92.65509999999999, 66), (1, 42.53990000000001, 92.67869999999999, 68), (1, 42.97620000000001, 93.61699999999999, 70), (1, 43.133800000000015, 94.43499999999999, 72), (1, 43.722200000000015, 95.12489999999998, 74), (1, 44.56220000000002, 95.79299999999998, 76), (1, 44.66190000000002, 96.73209999999997, 78), (1, 44.92970000000002, 97.14599999999997, 80), (1, 45.70440000000002, 97.80189999999997, 82), (1, 45.78740000000002, 98.46949999999997, 84), (1, 46.29550000000002, 98.62219999999996, 86), (1, 46.63160000000002, 99.17749999999997, 88), (1, 47.20590000000002, 99.51989999999996, 90), (1, 47.48470000000002, 100.50479999999996, 92), (1, 47.78310000000002, 100.66859999999996, 94), (1, 48.34610000000002, 101.39289999999995, 96), (1, 48.39280000000002, 102.08709999999995, 98)]

# this function finds the actual latitude and longitude coordinates between two points ie 0--x--------0
def interpolate(coords1, coords2, targetTime):
    # coords 1/2 looks something like this: (userid, latitude, longitude, time)
    # we calculate the solution as follows:
    # 1. Calculate alpha -> (target time - coords1 time) / (coords2 time - coords1 time)
    # 2. solve for latitude: coords1 lat + alpha(coords2 lat - coords1 lat)
    return (coords1[1] + (((targetTime - coords1[3]) / (coords2[3] - coords1[3])) * (coords2[1] - coords1[1])), coords1[2] + (((targetTime - coords1[3]) / (coords2[3] - coords1[3])) * (coords2[2] - coords1[2])) )

# Function will search an array of times for an interval between which our times match
# All Times stored in unix time
def approxBinarySearch(tupleArray, targetTime, lo, hi = None):
    if hi is None:
        hi = len(tupleArray)
    if targetTime > tupleArray[len(tupleArray ) - 1][3] or targetTime < tupleArray[0][3]:
        return ()
    while lo < hi:
        mid = (lo + hi)//2
        midval = tupleArray[mid][3]
        if mid + 1 != len(tupleArray):
            nextval = tupleArray[mid + 1][3]
        else:
            nextval = None
        if mid != 0:
            prevval = tupleArray[mid - 1][3]
        else:
            prevval = None
        # does midval match our target?
        if targetTime == midval:
            return [mid]
        # test if the target fits between previous and current
        elif prevval < targetTime < midval:
            return [mid - 1, mid]
        # test if the target fits between current and next
        elif midval < targetTime < nextval:
            return [mid, mid + 1]
        elif midval < targetTime:
            lo = mid + 1
        elif midval > targetTime: 
            hi = mid
        else:
            return [mid]
    return ()

# Function takes a variety of different timed data and returns data points 
# userData: a set of position data for a specific user(sorted by time)
# numOfPositions: the number of iterations we want to compare
# timePeriod: amount of time to analyze, in seconds
def normalize(userData, numOfIterations, startTime, endTime):
    data = []
    timePeriod = endTime - startTime
    times = []
    segment = timePeriod / numOfIterations
    current = startTime
    while current <= timePeriod:
        times.append(current)
        current += segment
    for time in times:
        surroundingIndices = approxBinarySearch(userData, time, 0) # returns a list containing the indices of the surrounding two times, returns one index if there is a match
        if len(surroundingIndices) == 1:
            # no interpolation necessary
            data.append(userData[surroundingIndices[0]])
        elif len(surroundingIndices) == 2:
            newLatLong = interpolate(userData[surroundingIndices[0]], userData[surroundingIndices[1]], time) # create a tuple of the interpolated latitude and longitude
            newTuple = (userData[0][0], newLatLong[0], newLatLong[1], time)
            data.append(newTuple)
        else:
            break # break if we are unable to access data for the given time
    return data

# FOR TESTING
# updatedUser1Info = normalize(user1Info, 100, 0, 100)
# print "UPDATED USER1 INFO"
# print updatedUser1Info
# updatedUser2Info = normalize(user2Info, 100, 0, 100)
# print "UPDATED USER2 INFO"
# print updatedUser2Info
