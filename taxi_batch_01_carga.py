from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TaxiBatchCarga").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://localhost:9000/Tarea3/taxi/")

print("=== ESQUEMA ===")
df.printSchema()

print("=== NUMERO DE FILAS ===")
print(df.count())

print("=== PRIMERAS FILAS ===")
df.select("tpep_pickup_datetime", "trip_distance", "fare_amount",
          "tip_amount", "total_amount", "passenger_count").show(5, truncate=False)

print("=== ESTADISTICAS ===")
df.select("trip_distance", "fare_amount", "tip_amount", "total_amount").summary().show()
