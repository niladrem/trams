import sys
import logging
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import count, max
import findspark
import math
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
import pandas as pd
import pyspark
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql.functions import count, max, mean
from pyspark.sql import functions as F
import seaborn as sns
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window

def get_team(lat1, lon1, stopsT):
    distS = stopsT.copy()
    distS['dist'] = distS.apply (lambda row: calculateDistance(lat1, lon1, row['lat'], row['lon']), axis=1)
    return distS[distS.dist == distS.dist.min()].iat[0,0]
def calculateDistance(lat1, lon1, lat2, lon2):
        earthR = 6371e3

        fi1 = lat1 * math.pi / 180
        fi2 = lat2 * math.pi / 180
        deltaFi = (lat2 - lat1) * math.pi / 180
        lambda_ = (lon2 - lon1) * math.pi / 180
        haversine = math.pow(math.sin(deltaFi / 2), 2) + math.cos(fi1) * math.cos(fi2) * math.pow(math.sin(lambda_ / 2), 2)
        c = 2 * math.atan2(math.sqrt(haversine), math.sqrt(1 - haversine))
        return earthR * c

calcDistPS = udf(calculateDistance, DoubleType())

findspark.init()
sc = SparkContext("local[2]", "First App")


from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(1)
dt = datetime.strftime(yesterday, '%Y-%m-%d')

if len(sys.argv) > 1:
    dt = sys.argv[1]


from pyspark.sql import HiveContext
hive_context = HiveContext(sc)
trams = hive_context.table("trams")
trams = trams.filter(trams.day == dt)
trams = trams.withColumn("time", trams["time"].cast("double"))

import happybase

stops = hive_context.table("stops")

w = Window().partitionBy("lines", "vehiclenumber", "brigade").orderBy(col("lines"), col("vehiclenumber"), col("time"))
# w = Window().partitionBy("lines").orderBy(col("lines"), col("vehiclenumber"), col("time"))
tramsPrev = trams.select("*", lag("lat", default = 0).over(w).alias("latPrev")).na.drop()
tramsPrev = tramsPrev.select("*", lag("lon", default = 0).over(w).alias("lonPrev")).na.drop()
tramsPrev = tramsPrev.select("*", lag("time", default = 0).over(w).alias("timePrev")).na.drop()
tramsPrev = tramsPrev.filter(tramsPrev.timePrev != 0)

tramsMoving = tramsPrev.withColumn("distance", calcDistPS("lat", "lon", "latPrev", "lonPrev"))
tramsMoving = tramsMoving.withColumn("timeDiff", (tramsMoving.time - tramsMoving.timePrev)/1000)
tramsMoving = tramsMoving.withColumn("velocity", tramsMoving.distance / tramsMoving.timeDiff)

stopsAgg = stops.groupBy("nazwa_zespolu").agg(mean("szer_geo").alias('lat_stops'),
                                              mean("dlug_geo").alias('lon_stops'),
                                             F.min("zespol").alias('zespol'))
stopsAggTram = stopsAgg.filter((stopsAgg.lat_stops > 52.15) & (stopsAgg.lat_stops < 52.35) & (stopsAgg.lon_stops > 20.9) & (stopsAgg.lon_stops < 21.1))

joined = tramsMoving.crossJoin(stopsAggTram)

joined = joined.withColumn("distance_to_team", calcDistPS("lat", "lon", "lat_stops", "lon_stops"))
joined_min = joined.groupBy("lines", "lon", "lat", "vehiclenumber", "brigade", "time", "day").agg(F.min("distance_to_team").alias("distance_to_team"))
joined_names = joined.select(joined.lines, joined.lon, joined.lat, joined.vehiclenumber, joined.brigade,
                             joined.time, joined.day,
                             joined.nazwa_zespolu, joined.distance_to_team, joined.velocity,
                             joined.timeDiff, joined.zespol, joined.distance)
cond = [joined_min.lines == joined_names.lines, joined_min.lon == joined_names.lon, joined_min.lat == joined_names.lat,
        joined_min.vehiclenumber == joined_names.vehiclenumber, joined_min.brigade == joined_names.brigade,
        joined_min.time == joined_names.time, joined_min.day == joined_names.day, joined_min.distance_to_team == joined_names.distance_to_team]
joined_final = joined_min.join(joined_names, cond, "left")
joined_final.show()
joined_final_Pd = joined_final.toPandas()

cols = {
    "infoTram": ["vehiclenumber","brigade"],
    "infoTeam": ["zespol","nazwa_zespolu"],
    "stats": ["velocity","lon","lat",
                         "distance", "time", "timeDiff"]
}

df = joined_final_Pd
df = df.loc[:,~df.columns.duplicated()]

flag = True

conn = happybase.Connection(host='localhost', port=9090)
conn.open()

while(flag):
    flag = False
    try:
        table = conn.table("tramsStats")
        for idx, row in df.iterrows():
            tmp = {}
            for family, family_cols in cols.items():
                for col in family_cols:
                    colIns = col
                    ins = str(row[col])
                    tmp["{}:{}".format(family, colIns)] = ins
            print(str(row['lines']))
            print(tmp)
            table.put(str(row['lines']), tmp)
        conn.close()
    except Exception as err:
        print(err)
        flag = True
