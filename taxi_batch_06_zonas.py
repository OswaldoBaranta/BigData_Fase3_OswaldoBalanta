import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TaxiZonas").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

lim = spark.read.parquet("hdfs://localhost:9000/Tarea3/taxi_limpio")
zonas = (spark.read.option("header", True)
         .csv("file:///home/hadoop/datos/taxi/taxi_zone_lookup.csv")
         .withColumn("LocationID", F.col("LocationID").cast("int")))

top = (lim.groupBy("PULocationID")
          .agg(F.count("*").alias("viajes"),
               F.round(F.avg("fare_amount"), 2).alias("tarifa_prom"))
          .orderBy(F.desc("viajes")).limit(10)
          .join(zonas, F.col("PULocationID") == zonas["LocationID"], "left")
          .select("PULocationID", "Borough", "Zone", "viajes", "tarifa_prom")
          .orderBy(F.desc("viajes")))

top.show(10, truncate=False)

pdf = top.toPandas()
salida = os.path.expanduser("~/graficos")
os.makedirs(salida, exist_ok=True)
plt.figure(figsize=(10, 5.5))
plt.barh(pdf["Zone"][::-1], pdf["viajes"][::-1] / 1000, color="#ED7D31")
plt.xlabel("Viajes (miles)")
plt.title("Diez zonas de recogida con más viajes")
plt.tight_layout()
plt.savefig(salida + "/4_top_zonas_con_nombre.png", dpi=150)
plt.close()
print("Gráfico guardado en", salida)
