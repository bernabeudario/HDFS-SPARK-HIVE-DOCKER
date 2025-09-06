from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Iniciar la sesion de Spark con soporte para Hive
spark = SparkSession.builder \
    .appName("Spark Hive") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 1) Crear la base de datos y la tabla en Hive si no existen
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.zonas (
        id INT,
        zona STRING
    )
    STORED AS PARQUET
""")

# 2) Crear un DataFrame de ejemplo
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("zona", StringType(), True)
])

data = [
    (1, "Norte"),
    (2, "Sur"),
    (3, "Este"),
    (4, "Oeste"),
    (5, "Centro")
]

df_zonas = spark.createDataFrame(data, schema)

# 3) Cargar los datos del DataFrame Spark en la tabla Hive
df_zonas.write.mode("overwrite").insertInto("bronze.zonas")

spark.stop()
