# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Test Spark") \
    .getOrCreate()

# Verificar el nivel de log
spark.sparkContext.setLogLevel("ERROR")

# Hacer una simple operación (crear DataFrame y mostrarlo)
data = [("Juan", 1), ("Ana", 2), ("Carlos", 3)]
df = spark.createDataFrame(data, ["Nombre", "Valor"])

df.show()

# Detener la sesión de Spark
spark.stop()

