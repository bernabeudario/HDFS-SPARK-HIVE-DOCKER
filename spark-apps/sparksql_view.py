from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Iniciar la sesion de Spark
    spark = SparkSession.builder \
        .appName("SparkSQL View") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # 1) Generar el DataFrame
    sample_data = [
        ("Registro 1", 1),
        ("Registro 2", 2),
        ("Registro 3", 3)
    ]
    columns = ["nombre", "id"]
    spark_df = spark.createDataFrame(sample_data, columns)

    # 2) Crear vista temporal
    spark_df.createOrReplaceTempView("v_registros")

    # 3) Consultar la vista 'v_registros' 
    spark.sql("SELECT * FROM v_registros WHERE id > 1 ORDER BY nombre DESC").show()
    print("=" * 30)


    spark.stop()
