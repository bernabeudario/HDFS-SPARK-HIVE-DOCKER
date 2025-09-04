from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == "__main__":
    # Iniciar la sesion de Spark
    spark = SparkSession.builder \
        .appName("Almacenar en HDFS") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Numero de registros a generar
    num_records = 50000000

    # Ruta en HDFS
    hdfs_path = "/tmp/archivo_desde_spark"

    # 1) Generar el DataFrame
    dataframe = spark.range(num_records) \
        .withColumn("nombre", expr("concat('Registro ', id)"))

    # 2) Almacenar en HDFS
    dataframe.write.mode("overwrite").parquet(hdfs_path)

    # 3) Recuperar desde HDFS
    spark.read.parquet(hdfs_path).show(10)
    print("=" * 30)

    spark.stop()
