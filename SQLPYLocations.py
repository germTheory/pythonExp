#!/usr/bin/env python3.4
import sys
import datetime
import pytz
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
diseaseName = "Ebola"
radius = 150 #feet
contagiousness = 5
timeInterval = 180
modifier = radius * 10

try:
    conn = psycopg2.connect("dbname='germtracker' user='sasup' host='localhost' password=''")
    # conn = psycopg2.connect("dbname='d21pkb4ibembvk' user='uer1d0n84nrcv8' host='ec2-54-243-218-135.compute-1.amazonaws.com' password='p81tjh25nn1g288d13fmt54kro3'")
    print("Connected to Database")
except:
    print("Unable to connect to the database.")

# find disease Id and relevant information about it
cur = conn.cursor()
cur.execute('SELECT * FROM diseases WHERE name = %s', (diseaseName,))
diseaseData = cur.fetchall()[0] # get the first item in the returned function
diseaseId = diseaseData[0]

# find users that have been confirmed infected with the disease
infectedUsers = {}
usersInProximity = {}
cur.execute('SELECT locations.user_id, locations.latitude, locations.longitude, locations.date FROM user_diseases, locations WHERE locations.user_id =user_diseases.user_id AND user_diseases.disease_id = %s AND date > %s AND date < %s ORDER BY user_id ASC;', (diseaseId, startDateTime, endDateTime,));
data = cur.fetchall()
#(14, 37.7890480964603, -122.424191045913, datetime.datetime(2014, 11, 6, 16, 20, 2, 387000, tzinfo=psycopg2.tz.FixedOffsetTimezone(offset=-480, name=None)))
for entry in data:
  userId = entry[0]
  lat = entry[1]
  lon = entry[2]
  currentTime = entry[3].replace(tzinfo=pytz.UTC)
  if userId in infectedUsers:
    infectedUsers[userId].append( { 'coords': (lat, lon), 'time': currentTime } )
  else:
    infectedUsers[userId] = [ { 'coords': (lat, lon), 'time': currentTime } ]
  # find all users who have been in the vicinity of the infected user
  curU = conn.cursor()
  curU.execute('SELECT user_id, latitude, longitude, date FROM locations WHERE ((EXTRACT(EPOCH FROM date - %s))::Integer) < 500 AND earth_distance(ll_to_earth( %s, %s), ll_to_earth( latitude, longitude)) < %s ORDER BY date ASC', (currentTime, lat, lon, radius,))
  dataUser = curU.fetchall()
  if dataUser:
    for entryU in dataUser:
      userIdP = entryU[0]
      userLat = entryU[1]
      userLon = entryU[2]
      userTime = entryU[3].replace(tzinfo=pytz.UTC)
      if userIdP in usersInProximity:
        usersInProximity[userIdP].append( { 'coords': (userLat, userLon), 'time': userTime } )
      else:
        usersInProximity[userIdP] = [ { 'coords': (userLat, userLon), 'time': userTime } ]

print(usersInProximity)
  

conn.commit()
cur.close()
curU.close()
conn.close()
