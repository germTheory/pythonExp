import sys
import datetime
import pytz
import numpy as np
import math
import normalize as norm
import psycopg2
import geopy
import scipy
from math import exp
from scipy.integrate import simps
from geopy.distance import vincenty
from random import randrange

# To be command line arguments
startDateTime = datetime.datetime(2014, 11, 7, 0, 0, 0).replace(tzinfo=pytz.UTC)
endDateTime = datetime.datetime(2014, 11, 8, 0, 0, 0).replace(tzinfo=pytz.UTC)
diseaseName = "Ebola"
threshold = 750 #feet
contagiousness = 10
timeInterval = 120

def parseData(allData, infectedUserData, interval):
    # iterate through data, assigning it to dictionaries, one for infected people and another for regular users
    # each item in data looks like: (id, latitude, longitude, created_at, updated_at, user_id)
    userLocs = {}
    infectedLocs = {}
    for userEntry in allData:
        currentTime = userEntry[locTime]
        lat = userEntry[locLat]
        long = userEntry[locLong]
        userId = userEntry[locUserId]
        # if already in our data structure, we can assign new location coordinates
        if userId in userLocs:
            userLocs[userId].append( { 'coords': (lat, long), 'time': currentTime } )
        elif userId in infectedLocs:
            infectedLocs[userId].append( { 'coords': (lat, long), 'time': currentTime } )
        # if not, check if they are infected and then add them to the apropriate table
        else:
            if userId in infectedUserData:
                infectedLocs[userId] = [ { 'coords': (lat, long), 'time': currentTime } ]
            else:
                userLocs[userId] = [ { 'coords': (lat, long), 'time': currentTime } ]
    times = []
    current = startDateTime
    interval = datetime.timedelta(0, interval)
    while current <= endDateTime:
        times.append(current)
        current += interval
    for userEntry in userLocs:
        # sort user data and then normalize it for 2 minute intervals
        userLocs[userEntry] = norm.normalize(sorted(userLocs[userEntry], key=lambda k: k['time']), times)
        print("Processed User %s" % (userEntry))
    print('ALL USERS PROCESSED')
    for infectedEntry in infectedLocs:
        infectedLocs[infectedEntry] = norm.normalize(sorted(infectedLocs[infectedEntry], key=lambda k: k['time']), times)
        print("Processed Infected %s" % (infectedEntry))
    print('ALL INFECTED PRE-PROCESSED...now for the hard part.')
    return (userLocs, infectedLocs, len(times))

def mapcount(count, contagiousness, threshold):
    # because math is fun.  See here for what this looks like: http://bit.ly/1u7DQuj
    # y = 1 - exp(-bx), where b = con/(thresh*300) and x = count
    return 1 - math.exp((-1)*(contagiousness/(threshold * (7000/timeInterval) )) * count)




# used for convenience in case the schema is changed later, saves indexes of location values
locId = 0
locUserId = 6
locLat = 1
locLong = 2
locTime = 3
# ebolaid = 1
# myid = 2
# population = 10
# iterations = 100
# threshold = .05

try:
    conn = psycopg2.connect("dbname='kmeurer' user='kevinmeurer' host='localhost' password=''")
    print("Connected to Database")
except:
    print("Unable to connect to the database.")

# find disease Id and relevant information about it
cur = conn.cursor()
cur.execute('SELECT * FROM diseases WHERE name = %s', (diseaseName,))
diseaseData = cur.fetchall()[0] # get the first item in the returned function
diseaseId = diseaseData[0]


# find users that have been confirmed infected with the disease
cur.execute('SELECT * FROM user_diseases WHERE disease_id = %s', (diseaseId,))
infectedList = [entry[2] for entry in cur.fetchall()]
infectedUsers = {}
for user in infectedList:
    infectedUsers[user] = {} # this will eventually store disease information about the user
print(infectedUsers)
# get all location data during the time we're concened with. created after start and before end
cur.execute('SELECT * FROM locations WHERE date > %s AND date < %s', (startDateTime, endDateTime))
data = cur.fetchall()
# print(data)
# parse and normalize data
mainData = parseData(data, infectedUsers, timeInterval)
results = {}
numOfTimes = mainData[2]
infectedLocs = mainData[1]
userLocs = mainData[0]
for userId in userLocs:
    print('Calculating index for user %s of %s' % (userId, len(userLocs)) )
    user = userLocs[userId]
    count = 0;
    for infectedId in infectedLocs:
        infected = infectedLocs[infectedId]
        distances = [] # to be our y values
        for idx, val in enumerate(user):
            print("VAL IS %s" % val)
            if val == None or infected[idx] == None:
                distances.append(None)
            else:
                distances.append(vincenty(val['coords'], infected[idx]['coords']).feet)
        belowthreshold = None
        belowdata = []
        for idx, distance in enumerate(distances):
            if  distance == None or (belowthreshold == None and distance > threshold):
                continue
            # check if we're at the end
            elif (belowthreshold == True and distance > threshold) or (belowthreshold == True and idx == len(distances) - 1):
                # TODO: calculate distance
                count += (threshold * (len(belowdata) - 1)) - simps(belowdata, dx = 1)
                belowdata = []
                belowthreshold = None
            elif belowthreshold == None and distance < threshold:
                belowthreshold = True
                belowdata.append(distance)
            elif belowthreshold == True and distance < threshold:
                belowdata.append(distance)
            else:
                continue
    results[userId] = mapcount(count, contagiousness, threshold)

print(results)
