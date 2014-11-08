
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


# Function will search an array of times for an approximate match
# All Times stored in unix time
# def approxBinarySearch():


# Function takes a variety of different timed data and returns data points 
# userData: a set of position data for a specific user(sorted by time)
# numOfPositions: the number of iterations we want to compare
# timePeriod: amount of time to analyze, in seconds
# def normalize(userData, numOfIterations, timePeriod){
#     data = []

#     times = []
#     segment = timePeriod / numOfIterations
#     current = 0
#     while (current <= timePeriod):
#         times[len(times):] = current
#         current += segment
        
# }

userData = normalize(infectedUser, 50, 86400)

sc = SparkContext(appName="PythonLocations")

# create an RDD object to store users. This will be replaced with a SQL query as we develop this app
users = sc.parallelize(simulatePositions())
infectedUser = users.filter(lambda tup: tup[0] == ebolaid).collect()
res = users.map(lambda position: (position[0], math.sqrt((position[1] - infectedUser[position[3]][1])**2 + (position[2] - infectedUser[position[3]][2])**2),position[3]) )
userData = res.filter(lambda tup: tup[0]==myid)
x = userData.map(lambda position: position[2]).collect();
y = userData.map(lambda position: position[1]).collect();
# use pylab to plot x and y
pylab.plot(x, y)
pylab.axhline(threshold, 0, iterations)
# show the plot on the screen
pylab.show()

result = userData.reduce(lambda a,b:(myid,a[1]+b[1]))
print (result[0], result[1] / len(infectedUser));
print 'Finished'

