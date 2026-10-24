from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TaxiDiagnostico").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://localhost:9000/Tarea3/taxi/")

dur = (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60

reglas = {
    "fuera_del_periodo":       (F.col("tpep_pickup_datetime") >= "2025-09-01") & (F.col("tpep_pickup_datetime") < "2025-12-01"),
    "bajada_no_posterior":     F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"),
    "distancia_fuera_0.1_100": F.col("trip_distance").between(0.1, 100),
    "tarifa_fuera_0_500":      (F.col("fare_amount") > 0) & (F.col("fare_amount") <= 500),
    "propina_negativa":        F.col("tip_amount") >= 0,
    "total_no_positivo":       F.col("total_amount") > 0,
    "pasajeros_fuera_1_6":     F.col("passenger_count").between(1, 6),
    "duracion_fuera_1_180":    dur.between(1, 180),
}

cols = [F.sum((~F.coalesce(c, F.lit(False))).cast("int")).alias(n) for n, c in reglas.items()]
cols += [F.sum(F.col("passenger_count").isNull().cast("int")).alias("pasajeros_nulos"),
         F.sum((F.col("passenger_count") == 0).cast("int")).alias("pasajeros_cero"),
         F.count("*").alias("total")]

fila = df.agg(*cols).collect()[0].asDict()
total = fila.pop("total")

print("=== REGISTROS QUE VIOLAN CADA REGLA ===")
for k, v in sorted(fila.items(), key=lambda x: -x[1]):
    print(f"{k:28s} {v:>10d}  {v/total*100:6.2f} %")
print("Total:", total)

print("=== PAGO (payment_type) ===")
df.groupBy("payment_type").count().orderBy(F.desc("count")).show()
