from pyspark.sql import SparkSession

# Iniciar sesión de Spark
spark = SparkSession.builder \
    .appName("Local Spark Session") \
    .getOrCreate()

# Ruta al archivo CSV
csv_path1 = "bronze/load_raw_bronze.csv"

# Cargar el archivo CSV
df = spark.read.csv(csv_path1, header=True, inferSchema=True)

# Crear la base de datos si no existe
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Registrar el DataFrame como una tabla temporal en el esquema 'bronze'
df.createOrReplaceTempView("five9_ivr")
spark.sql("USE bronze")

# Ejecutar el comando DESCRIBE
esquema_tabla = spark.sql("DESCRIBE five9_ivr")
esquema_tabla.show()


# Haood


from pyspark.sql import SparkSession

# Iniciar sesión de Spark
spark = SparkSession.builder \
    .appName("Local Spark Session") \
    .getOrCreate()

# Ruta al archivo CSV
csv_path1 = "bronze/load_raw_bronze.csv"

# Cargar el archivo CSV
df = spark.read.csv(csv_path1, header=True, inferSchema=True)

# Crear la base de datos (esquema) si no existe
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Registrar el DataFrame como una tabla temporal en el esquema 'bronze'
df.createOrReplaceTempView("five9_ivr_temp")

# Guardar el DataFrame como tabla en el esquema 'bronze'
spark.sql("CREATE TABLE bronze.five9_ivr USING parquet AS SELECT * FROM five9_ivr_temp")

# Ejecutar el comando DESCRIBE
esquema_tabla = spark.sql("DESCRIBE bronze.five9_ivr")
esquema_tabla.show()

