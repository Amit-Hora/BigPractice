import random
import sys
import math
import time
from threading import Thread

radius = 10000  # Choose your own radius
radiusInDegrees = radius / 111300
r = radiusInDegrees
x0 = 40.84
y0 = -73.87

for j in range(1,50):
    outfile = open("C:\\Users\\ahora2\\SparkProjects\\PySparkPOC\\randomfiles\\randomLatLong_NY"+str(j)+".txt", "w")
    #outfile.write("latitude,longitude" + '\n')
    for i in range(1, 4):  # Choose number of Lat Long to be generated

        u = float(random.uniform(0.0, 1.0))
        v = float(random.uniform(0.0, 1.0))

        w = r * math.sqrt(u)
        t = 2 * math.pi * v
        x = w * math.cos(t)
        y = w * math.sin(t)

        xLat = x + x0
        yLong = y + y0
        outfile.write(str(xLat) + "," + str(yLong) + '\n')
    time.sleep(5)