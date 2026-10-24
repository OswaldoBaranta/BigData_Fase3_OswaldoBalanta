from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TaxiEDA").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

lim = spark.read.parquet("hdfs://localhost:9000/Tarea3/taxi_limpio")

print("=== VIAJES, TARIFA POR MILLA Y PROPINA POR HORA ===")
(lim.groupBy("hora")
    .agg(F.count("*").alias("viajes"),
         F.round(F.avg("tarifa_por_milla"), 2).alias("tarifa_milla_prom"),
         F.round(F.avg("propina_pct"), 2).alias("propina_pct_prom"))
    .orderBy("hora").show(24, truncate=False))

print("=== VIAJES POR DIA DE LA SEMANA (1=domingo) ===")
(lim.groupBy(F.dayofweek("tpep_pickup_datetime").alias("dia_semana"))
    .agg(F.count("*").alias("viajes"),
         F.round(F.avg("fare_amount"), 2).alias("tarifa_prom"))
    .orderBy("dia_semana").show())

print("=== TOP 10 ZONAS DE RECOGIDA ===")
(lim.groupBy("PULocationID")
    .agg(F.count("*").alias("viajes"),
         F.round(F.avg("fare_amount"), 2).alias("tarifa_prom"))
    .orderBy(F.desc("viajes")).show(10))

print("=== POR TIPO DE PAGO ===")
(lim.groupBy("payment_type")
    .agg(F.count("*").alias("viajes"),
         F.round(F.avg("tip_amount"), 2).alias("propina_prom"))
    .orderBy(F.desc("viajes")).show())
