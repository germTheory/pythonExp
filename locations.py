import sys
import datetime
import numpy as np
import math
import normalize as norm
# import pylab
import psycopg2
import geopy
from geopy.distance import vincenty
from random import randrange

# To be command line arguments
startDateTime = datetime.datetime(2013, 11, 8, 0, 0, 0)
endDateTime = datetime.datetime(2014, 12, 8, 0, 0, 0)
diseaseName = "Ebola"


# used for convenience in case the schema is changed later, saves indexes of location values
locId = 0
locUserId = 5
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
infectedList = [entry[1] for entry in cur.fetchall()]
infectedUsers = {}
for user in infectedList:
	infectedUsers[user] = {} # this will eventually store disease information about the user


# get all location data during the time we're concened with. created after start and before end
cur.execute('SELECT * FROM locations WHERE created_at > %s AND created_at < %s', (startDateTime, endDateTime))
data = cur.fetchall()


def parseData(allData, infectedUserData):
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
	for userEntry in userLocs:
		userEntry = sorted(userEntry, key=lambda k: k['time'])
	for infectedEntry in infectedLocs:
		infectedLocs[infectedEntry] = sorted(infectedLocs[infectedEntry], key=lambda k: k['time'])
	return (userLocs, infectedLocs)

parsed = parseData(data, infectedUsers)
print("USER PARSED")
print(parsed[0])
print("INFECTED PARSED")
print(parsed[1])



# def simulatePositions():
#     positions = []
#     # Instantiate 100 people
#     for person in range(population):
#         lat = randrange(90000)/1000000.
#         lng = randrange(180000)/1000000.
#         positions[len(positions):] = [ (person, lat, lng, 0) ]
#     arr = []
#     for time in range(iterations):
#         for userId in range(population):
#             idx = 0;
#             positions[userId]=(
#                 positions[userId][0],
#                 positions[userId][1] + randrange(1000)/100000.,
#                 positions[userId][2] + randrange(1000)/100000.,
#                 time)
#             lat = positions[userId][1]
#             lng = positions[userId][2]
#             arr[len(arr):] = [(positions[userId][0], lat, lng, time, idx)]
#     return arr


# userData = simulatePositions();
# # capture data points of only the two people we are concerned with
# infectedUser = userData.filter( lambda tup: tup[0] == ebolaid );
# comparedUser = userData.filter( lambda tup: tup[0] == myid ).collect();


# # normalize data to have consistent data points to compare
# # userData = res.filter( lambda tup: tup[0]==myid )
# x = infectedUser.map(lambda position: position[3])
# y = []
# for i in userData:
# 	y.append()
# # use pylab to plot x and y
# pylab.plot( x, y )
# pylab.axhline( threshold, 0, iterations )
# # show the plot on the screen
# pylab.show()

# result = userData.reduce(lambda a,b:(myid,a[1]+b[1]))
# print (result[0], result[1] / len(infectedUser));
# print 'Finished'
