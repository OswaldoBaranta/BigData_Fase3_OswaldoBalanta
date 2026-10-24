import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TaxiGraficos").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
lim = spark.read.parquet("hdfs://localhost:9000/Tarea3/taxi_limpio")

salida = os.path.expanduser("~/graficos")
os.makedirs(salida, exist_ok=True)

# 1. Demanda y tarifa por milla según la hora
h = (lim.groupBy("hora")
        .agg(F.count("*").alias("viajes"), F.avg("tarifa_por_milla").alias("tarifa_milla"))
        .orderBy("hora").toPandas())
fig, ax1 = plt.subplots(figsize=(10, 5))
ax1.bar(h["hora"], h["viajes"] / 1000, color="#5B9BD5")
ax1.set_xlabel("Hora de recogida")
ax1.set_ylabel("Viajes (miles)")
ax1.set_xticks(range(24))
ax2 = ax1.twinx()
ax2.plot(h["hora"], h["tarifa_milla"], color="#C00000", marker="o")
ax2.set_ylabel("Tarifa promedio por milla (USD)")
plt.title("Demanda y tarifa por milla según la hora del día")
plt.tight_layout()
plt.savefig(salida + "/1_demanda_tarifa_por_hora.png", dpi=150)
plt.close()

# 2. Viajes por día de la semana
d = (lim.groupBy(F.dayofweek("tpep_pickup_datetime").alias("dia"))
        .agg(F.count("*").alias("viajes")).orderBy("dia").toPandas())
nombres = ["Dom", "Lun", "Mar", "Mié", "Jue", "Vie", "Sáb"]
plt.figure(figsize=(8, 5))
plt.bar([nombres[i - 1] for i in d["dia"]], d["viajes"] / 1000, color="#70AD47")
plt.ylabel("Viajes (miles)")
plt.title("Viajes por día de la semana")
plt.tight_layout()
plt.savefig(salida + "/2_viajes_por_dia.png", dpi=150)
plt.close()

# 3. Diez zonas con más viajes
z = (lim.groupBy("PULocationID").agg(F.count("*").alias("viajes"))
        .orderBy(F.desc("viajes")).limit(10).toPandas())
plt.figure(figsize=(9, 5))
plt.bar(z["PULocationID"].astype(str), z["viajes"] / 1000, color="#ED7D31")
plt.xlabel("Zona de recogida (PULocationID)")
plt.ylabel("Viajes (miles)")
plt.title("Diez zonas de recogida con más viajes")
plt.tight_layout()
plt.savefig(salida + "/3_top_zonas.png", dpi=150)
plt.close()

print("Gráficos guardados en", salida)
