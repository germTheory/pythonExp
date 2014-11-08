
import sys
import numpy as np
from pyspark import SparkContext
from random import randrange
import math
import pylab as pl

ebolaid = 1;
myid = 2;
population = 10
generations = 100

def simulatePositions():
    p = []
    # 100 people
    for i in range(population):
        lat = randrange(90000)/1000.
        lng = randrange(180000)/1000.
        p[len(p):] = [(i,lat,lng,0)]
    l = []
    for t in range(generations):
        for j in range(population):
            p[j]=(
                p[j][0],
                p[j][1] + randrange(1000)/1000.,
                p[j][2] + randrange(1000)/1000.,
                t)
            lat = p[j][1]
            lng = p[j][2]
            l[len(l):] = [(p[j][0],lat,lng,t)]
    return l

sc = SparkContext(appName="PythonLocations")
lis = sc.parallelize(simulatePositions());
ebolaguy = lis.filter(lambda tup: tup[0]==ebolaid).collect()
res = lis.map(lambda p: (p[0],math.sqrt((p[1]-ebolaguy[p[3]][1])**2+(p[2]-ebolaguy[p[3]][2])**2),p[3]) )
mydata = res.filter(lambda tup: tup[0]==myid)
x = mydata.map(lambda p: p[2]).collect();
y = mydata.map(lambda p: p[1]).collect();
# use pylab to plot x and y
pl.plot(x, y)
# show the plot on the screen
pl.show()

result = mydata.reduce(lambda a,b:(myid,a[1]+b[1]))
print (result[0],result[1]/len(ebolaguy));
print 'Finished'

