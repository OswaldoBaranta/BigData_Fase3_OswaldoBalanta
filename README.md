# Tarea 3: Procesamiento de datos con Apache Spark (Big Data, UNAD 202016911)

Análisis en batch de los viajes de taxi amarillo de Nueva York (septiembre a noviembre de 2025) con PySpark, y procesamiento en tiempo real con Apache Kafka y Spark Streaming.

## Problema

¿Cómo varían la demanda, la tarifa por milla y la propina según la hora del día, el día de la semana, la zona de recogida y el tipo de pago?

## Datos

- Fuente: NYC Taxi & Limousine Commission (TLC), Trip Record Data (datos públicos).
- Archivos: `yellow_tripdata_2025-09.parquet`, `yellow_tripdata_2025-10.parquet` y `yellow_tripdata_2025-11.parquet`.
- Descarga: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_AAAA-MM.parquet`
- Volumen: 12.861.158 registros (209 MB en Parquet); 11.159.159 tras la limpieza.

## Entorno

- Ubuntu en una máquina virtual de VirtualBox
- Hadoop 3.3.x (HDFS y YARN), Spark 3.5.8 (Scala 2.12.18), Java 11, Python 3.13
- Kafka 3.9.2 (con ZooKeeper) y `kafka-python` 3.0.11
- Librerías de Python para los gráficos: `pandas` y `matplotlib`

## Procesamiento en batch (carpeta `batch/`)

1. Descargar los tres archivos a `~/datos/taxi` y cargarlos a HDFS:

   ```bash
   hdfs dfs -mkdir -p /Tarea3/taxi
   hdfs dfs -put -f ~/datos/taxi/*.parquet /Tarea3/taxi/
   ```

2. Ejecutar los scripts en orden con `spark-submit`:

   | Script | Qué hace |
   |---|---|
   | `taxi_batch_01_carga.py` | Lee los Parquet, muestra esquema, conteo, primeras filas y estadísticas |
   | `taxi_batch_03_diagnostico.py` | Mide cuántos registros viola cada regla de calidad |
   | `taxi_batch_02_limpieza.py` | Aplica la limpieza, crea columnas derivadas y escribe `/Tarea3/taxi_limpio` |
   | `taxi_batch_04_eda.py` | Agrega por hora, día de la semana, zona y tipo de pago |
   | `taxi_batch_05_graficos.py` | Genera tres gráficos en `~/graficos` |
   | `taxi_batch_06_zonas.py` | Une el resultado con la tabla de zonas de la TLC y grafica las diez zonas con nombre |

   ```bash
   spark-submit batch/taxi_batch_01_carga.py
   spark-submit batch/taxi_batch_03_diagnostico.py
   spark-submit batch/taxi_batch_02_limpieza.py
   spark-submit batch/taxi_batch_04_eda.py
   spark-submit batch/taxi_batch_05_graficos.py
   wget -P ~/datos/taxi https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
   spark-submit batch/taxi_batch_06_zonas.py
   ```

La carpeta `resultados/` contiene los gráficos obtenidos.

## Tiempo real (carpeta `streaming/`)

Se ejecuta en terminales separadas y en este orden: ZooKeeper, broker de Kafka, creación del topic, productor y consumidor.

```bash
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties &
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties &
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
python3 streaming/kafka_producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 --conf spark.sql.shuffle.partitions=4 streaming/spark_streaming_consumer.py
```

El paquete `spark-sql-kafka-0-10_2.12` debe coincidir con la versión de Spark instalada.

## Autores

Oswaldo Balanta
