from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import unicodedata

spark = SparkSession.builder \
    .appName("AnalisisSiniestrosBogota") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

ruta = "hdfs://localhost:9000/Tarea3/siniestros_bogota.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(ruta)

print("\n==============================================")
print("     ANALISIS DE SINIESTROS VIALES BOGOTA")
print("==============================================")

print("\n--- ESQUEMA DEL DATASET ---\n")
df.printSchema()

print("\n--- PRIMEROS 10 REGISTROS ---\n")
df.show(10, truncate=False)

total_registros = df.count()
total_columnas = len(df.columns)

print("\n--- INFORMACION GENERAL ---\n")
print("Total de registros:", total_registros)
print("Total de columnas:", total_columnas)

print("\nColumnas encontradas:")
for columna in df.columns:
    print("-", columna)

print("\n--- VALORES NULOS POR COLUMNA ---\n")

nulos = df.select([
    F.sum(
        F.when(
            F.col(c).isNull() |
            (F.trim(F.col(c).cast("string")) == ""),
            1
        ).otherwise(0)
    ).alias(c)
    for c in df.columns
])

nulos.show(truncate=False)

df_limpio = df.dropDuplicates()

registros_limpios = df_limpio.count()
duplicados = total_registros - registros_limpios

print("\n--- LIMPIEZA DE DATOS ---\n")
print("Registros originales:", total_registros)
print("Registros duplicados:", duplicados)
print("Registros despues de la limpieza:", registros_limpios)

def normalizar(texto):
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return texto.upper().strip()

columnas_normalizadas = {
    normalizar(c): c for c in df_limpio.columns
}

if "LOCALIDAD" in columnas_normalizadas:
    columna_localidad = columnas_normalizadas["LOCALIDAD"]

    print("\n--- 10 LOCALIDADES CON MAS SINIESTROS ---\n")

    resultado_localidad = df_limpio \
        .filter(F.col(columna_localidad).isNotNull()) \
        .groupBy(columna_localidad) \
        .count() \
        .orderBy(F.desc("count"))

    resultado_localidad.show(10, truncate=False)
else:
    print("\nNo se encontro la columna LOCALIDAD.")

if "GRAVEDAD" in columnas_normalizadas:
    columna_gravedad = columnas_normalizadas["GRAVEDAD"]

    print("\n--- SINIESTROS SEGUN GRAVEDAD ---\n")

    resultado_gravedad = df_limpio \
        .filter(F.col(columna_gravedad).isNotNull()) \
        .groupBy(columna_gravedad) \
        .count() \
        .orderBy(F.desc("count"))

    resultado_gravedad.show(truncate=False)
else:
    print("\nNo se encontro la columna GRAVEDAD.")

if "CLASE_ACC" in columnas_normalizadas:
    columna_clase = columnas_normalizadas["CLASE_ACC"]

    print("\n--- SINIESTROS SEGUN CLASE DE ACCIDENTE ---\n")

    resultado_clase = df_limpio \
        .filter(F.col(columna_clase).isNotNull()) \
        .groupBy(columna_clase) \
        .count() \
        .orderBy(F.desc("count"))

    resultado_clase.show(truncate=False)
else:
    print("\nNo se encontro la columna CLASE_ACC.")

columna_anio = None
for posible in ["AÑO", "ANO", "ANO_OCURRENCIA_ACC"]:
    if normalizar(posible) in columnas_normalizadas:
        columna_anio = columnas_normalizadas[normalizar(posible)]
        break

if columna_anio:
    print("\n--- SINIESTROS POR AÑO ---\n")

    resultado_anio = df_limpio \
        .filter(F.col(columna_anio).isNotNull()) \
        .groupBy(columna_anio) \
        .count() \
        .orderBy(columna_anio)

    resultado_anio.show(30, truncate=False)
else:
    print("\nNo se encontro una columna correspondiente al año.")

print("\n--- RESUMEN ESTADISTICO ---\n")
df_limpio.summary().show(truncate=False)

print("\n==============================================")
print("     PROCESAMIENTO BATCH FINALIZADO")
print("==============================================\n")

spark.stop()
