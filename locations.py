
import sys
import numpy as np
# from pyspark import SparkContext
from random import randrange
import math
import normalize as norm
import pylab
from geopy.distance import vincenty

ebolaid = 1
myid = 2
population = 10
iterations = 100
threshold = .05
startTime = 1415318400 # Beginning of November 7, 2014
endTime = 1415404800 # End of November 7, 2014

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
            idx = 0;
            positions[userId]=(
                positions[userId][0],
                positions[userId][1] + randrange(1000)/100000.,
                positions[userId][2] + randrange(1000)/100000.,
                time)
            lat = positions[userId][1]
            lng = positions[userId][2]
            arr[len(arr):] = [(positions[userId][0], lat, lng, time, idx)]
    return arr


# sc = SparkContext(appName="PythonLocations")

# create an RDD object to store users. This will be replaced with a SQL query as we develop this service
# users = sc.parallelize(simulatePositions())
userData = simulatePositions();
# capture data points of only the two people we are concerned with
infectedUser = userData.filter( lambda tup: tup[0] == ebolaid );
comparedUser = userData.filter( lambda tup: tup[0] == myid ).collect();


# # normalize data to have consistent data points to compare
# # userData = norm.normalize(infectedUser, 500, startTime, endTime)
# res = users.map(lambda position: (position[0], math.sqrt((position[1] - infectedUser[position[3]][1])**2 + (position[2] - infectedUser[position[3]][2])**2),position[3]) )
# userData = res.filter( lambda tup: tup[0]==myid )
x = infectedUser.map(lambda position: position[3])
y = []
for i in userData:
	y.append()
# use pylab to plot x and y
pylab.plot( x, y )
pylab.axhline( threshold, 0, iterations )
# show the plot on the screen
pylab.show()

result = userData.reduce(lambda a,b:(myid,a[1]+b[1]))
print (result[0], result[1] / len(infectedUser));
print 'Finished'

