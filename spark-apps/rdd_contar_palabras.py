from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Iniciar la sesion de Spark
    spark = SparkSession.builder \
        .appName("Contar Palabras: 3 Cores") \
        .master("spark://spark-master:7077") \
        .config("spark.cores.max", "3") \
        .getOrCreate()

    # Crear un RDD a partir de un archivo de texto con contenido: A B C B C C
    file_path = "file:///opt/spark-apps/texto_ejemplo.txt"
    lines = spark.sparkContext.textFile(file_path)

    # Realizar las transformaciones
    word_counts = lines.flatMap(lambda line: line.split(" ")) \
                    .map(lambda word: (word, 1)) \
                    .reduceByKey(lambda a, b: a + b)

    # flatMap: ['A', 'B', 'C', 'B', 'C', 'C']
    # map: [('A', 1), ('B', 1), ('C', 1), ('B', 1), ('C', 1), ('C', 1)]
    # reduceByKey: [('A', 1), ('B', 2), ('C', 3)]

    # Realizar la accion collect() que ejecuta el Job y devuelve el resultado al Spark Driver
    print(word_counts.collect())
    print("=" * 30)

    spark.stop()