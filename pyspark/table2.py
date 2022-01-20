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
from pyspark.sql.functions import *

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

from pyspark.sql import HiveContext
hive_context = HiveContext(sc)
trams = hive_context.table("trams")

from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(1)
dt = datetime.strftime(yesterday, '%Y-%m-%d')

if len(sys.argv) > 1:
    dt = sys.argv[1]


trams = trams.filter(trams.day == dt)
trams = trams.withColumn("time", trams["time"].cast("double"))

import happybase

conn = happybase.Connection('localhost',port=9090)
conn.open()

w = Window().partitionBy("lines", "vehiclenumber", "brigade").orderBy(col("lines"), col("vehiclenumber"), col("time"))
# w = Window().partitionBy("lines").orderBy(col("lines"), col("vehiclenumber"), col("time"))
tramsPrev = trams.select("*", lag("lat", default = 0).over(w).alias("latPrev")).na.drop()
tramsPrev = tramsPrev.select("*", lag("lon", default = 0).over(w).alias("lonPrev")).na.drop()
tramsPrev = tramsPrev.select("*", lag("time", default = 0).over(w).alias("timePrev")).na.drop()
tramsPrev = tramsPrev.filter(tramsPrev.timePrev != 0)

tramsMoving = tramsPrev.withColumn("distance", calcDistPS("lat", "lon", "latPrev", "lonPrev"))
tramsMoving = tramsMoving.withColumn("timeDiff", (tramsMoving.time - tramsMoving.timePrev)/1000)
tramsMoving = tramsMoving.withColumn("velocity", tramsMoving.distance / tramsMoving.timeDiff)

tramsAgg = tramsMoving.groupBy("lines").agg(F.mean("velocity").alias('mean_velocity'),
                                           F.count("brigade").alias('number_of_brigades'),
                                           F.min("time").alias('min_time'),
                                           F.max("time").alias('max_time'),
                                           F.min("day").alias('day'))
tramsAgg = tramsAgg.withColumn("min_time", F.to_utc_timestamp(F.from_unixtime(F.col("min_time")/1000,'yyyy-MM-dd HH:mm:ss'),'EST'))\
        .withColumn("max_time", F.to_utc_timestamp(F.from_unixtime(F.col("max_time")/1000,
                                                                   'yyyy-MM-dd HH:mm:ss'),'EST'))

df = tramsAgg.toPandas()

cols = {
    "stats": ["mean_velocity","number_of_brigades"],
    "dayStats": ["min_time","max_time", "day"]
}

df = df.loc[:,~df.columns.duplicated()]

flag = True

while(flag):
    flag = False
    try:
        conn = happybase.Connection(host='localhost', port=9090)
        conn.open()
        table = conn.table("linesStats")
        for idx, row in df.iterrows():
            tmp = {}
            for family, family_cols in cols.items():
                for col in family_cols:
                    colIns = col
                    ins = str(row[col])
                    tmp["{}:{}".format(family, colIns)] = ins
            table.put(str(row['lines']), tmp)
        conn.close()
    except Exception as err:
        print(err)
        flag = True
