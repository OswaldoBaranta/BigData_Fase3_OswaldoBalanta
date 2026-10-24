from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TaxiBatchLimpieza").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://localhost:9000/Tarea3/taxi/")
total = df.count()

dur = (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60

limpio = (df
    .filter(F.col("tpep_pickup_datetime") >= "2025-09-01")
    .filter(F.col("tpep_pickup_datetime") < "2025-12-01")
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    .filter(F.col("trip_distance").between(0.1, 100))
    .filter((F.col("fare_amount") > 0) & (F.col("fare_amount") <= 500))
    .filter(F.col("tip_amount") >= 0)
    .filter(F.col("total_amount") > 0)
    .withColumn("hora", F.hour("tpep_pickup_datetime"))
    .withColumn("duracion_min", dur)
    .filter(F.col("duracion_min").between(1, 180))
    .withColumn("tarifa_por_milla", F.col("fare_amount") / F.col("trip_distance"))
    .filter(F.col("tarifa_por_milla").between(1, 100)))

pct = F.when((F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
             F.col("tip_amount") / F.col("fare_amount") * 100)
limpio = limpio.withColumn("propina_pct", F.when(pct <= 100, pct))

limpio.write.mode("overwrite").parquet("hdfs://localhost:9000/Tarea3/taxi_limpio")
lim = spark.read.parquet("hdfs://localhost:9000/Tarea3/taxi_limpio")
n = lim.count()

print("=== LIMPIEZA ===")
print("Registros originales:", total)
print("Registros limpios:", n)
print("Descartados:", total - n, "(", round((total - n) / total * 100, 2), "% )")

print("=== ESTADISTICAS TRAS LA LIMPIEZA ===")
lim.select("trip_distance", "fare_amount", "tip_amount", "duracion_min",
           "tarifa_por_milla", "propina_pct").summary().show()
