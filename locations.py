
import sys
import numpy as np
from pyspark import SparkContext
from random import randrange
import math
# import scipi.interpolate as sp
import pylab

ebolaid = 1
myid = 2
population = 10
iterations = 100
threshold = .5
startTime = 1415318400 # Beginning of November 7, 2014
endTime = 1415404800 # End of November 7, 2014

# this function finds the actual latitude and longitude coordinates between two points ie 0--x--------0
def interpolate(coords1, coords2, targetTime):
    # coords 1/2 looks something like this: (userid, latitude, longitude, time)
    # we calculate the solution as follows:
    # 1. Calculate alpha -> (target time - coords1 time) / (coords2 time - coords1 time)
    # 2. solve for latitude: coords1 lat + alpha(coords2 lat - coords1 lat)
    return (coords1[1] + (((targetTime - coords1[3]) / (coords2[3] - coords1[3])) * (coords2[1] - coords1[1])), coords1[2] + (((targetTime - coords1[3]) / (coords2[3] - coords1[3])) * (coords2[2] - coords1[2])) )

def simulatePositions():
    positions = []
    # Instantiate 100 people
    for person in range(population):
        lat = randrange(90000)/1000000.
        lng = randrange(180000)/1000000.
        positions[len(positions):] = [ (person, lat, lng, 0) ]
    arr = []
    for time in range(iterations):
        for userId in range(population):
            positions[userId]=(
                positions[userId][0],
                positions[userId][1] + randrange(1000)/100000.,
                positions[userId][2] + randrange(1000)/100000.,
                time)
            lat = positions[userId][1]
            lng = positions[userId][2]
            arr[len(arr):] = [(positions[userId][0], lat, lng, time)]
    return arr


# Function will search an array of times for an interval between which our times match
# All Times stored in unix time
def approxBinarySearch(tupleArray, targetTime, lo, hi=None):
    if hi is None:
        hi = len(tupleArray)
    if targetTime > tupleArray[len(tupleArray - 1)][3] or targetTime < tupleArray[0][3]:
        return ()
    while lo < hi:
        mid = (lo + hi)//2
        midval = tupleArray[mid][3]
        nextval = tupleArray[mid + 1][3]
        prevval = tupleArray[mid - 1][3]
        # does midval match our target?
        if targetTime == midval:
            return (mid)
        # test if the target fits between previous and current
        elif prevval < targetTime < midval:
            return (mid - 1, mid)
        # test if the target fits between current and next
        elif midval < targetTime < nextval:
            return (mid, mid + 1)
        elif midval < targetTime:
            lo = mid + 1
        elif midval > targetTime: 
            hi = mid
        else:
            return mid
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
        times[ len(times) ] = current
        current += segment
    for time in times
        surroundingIndices = approxBinarySearch(userData, time) # returns a tuple containing the indices of the surrounding two times, returns one index if there is a match
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


sc = SparkContext(appName="PythonLocations")

# create an RDD object to store users. This will be replaced with a SQL query as we develop this service
users = sc.parallelize(simulatePositions())

# capture data points of only the two people we are concerned with
infectedUser = users.filter( lambda tup: tup[0] == ebolaid ).collect()
comparedUser = users.filter( lambda tup: tup[0] == userid ).collect()

# normalize data to have consistent data points to compare
userData = normalize(infectedUser, 500, startTime, endTime)
res = users.map(lambda position: (position[0], math.sqrt((position[1] - infectedUser[position[3]][1])**2 + (position[2] - infectedUser[position[3]][2])**2),position[3]) )
userData = res.filter( lambda tup: tup[0]==myid )
x = userData.map( lambda position: position[2] ).collect();
y = userData.map( lambda position: position[1] ).collect();
# use pylab to plot x and y
pylab.plot( x, y )
pylab.axhline( threshold, 0, iterations )
# show the plot on the screen
pylab.show()

result = userData.reduce(lambda a,b:(myid,a[1]+b[1]))
print (result[0], result[1] / len(infectedUser));
print 'Finished'

