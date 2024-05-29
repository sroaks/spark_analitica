from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date, year, month, dayofmonth, date_format, weekofyear, quarter, concat_ws, lit

# Crear la sesión de Spark
spark = SparkSession.builder.appName("ComprasVentas").getOrCreate()

# Cargar los archivos Excel como DataFrames de PySpark
compras_totales = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("ct_py.xlsx")
compras_semanales = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("cs_py.xlsx")

# Modificar campos antes de guardar
compras_semanales = compras_semanales.withColumn("PC.UNIT", col("TOTAL") / col("CANTIDAD"))
compras_semanales = compras_semanales.withColumn("AÑO", year(to_date(col("FECHA"), 'dd/MM/yyyy')))
compras_semanales = compras_semanales.withColumn("DÍA", dayofmonth(to_date(col("FECHA"), 'dd/MM/yyyy')))
compras_semanales = compras_semanales.withColumn("MES", month(to_date(col("FECHA"), 'dd/MM/yyyy')))
compras_semanales = compras_semanales.withColumn("FECHA", to_date(col("FECHA"), 'dd/MM/yyyy'))
compras_semanales = compras_semanales.withColumn("MES.TEXTO", date_format(col("FECHA"), 'MMMM'))

# Guardar los cambios en Excel
compras_semanales.write.format("com.crealytics.spark.excel").option("header", "true").mode("overwrite").save("cs_py.xlsx")

# Volver a cargar el archivo actualizado
compras_semanales = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("cs_py.xlsx")

# Combinar los DataFrames de compras
compras_totales = compras_totales.union(compras_semanales)

# Guardar el DataFrame combinado
compras_totales.write.format("com.crealytics.spark.excel").option("header", "true").mode("overwrite").save("ct_py.xlsx")

# Cargar de nuevo el archivo combinado
compras_totales = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("ct_py.xlsx")

# Ordenar el DataFrame por REFERENCIA y FECHA
ct_py = compras_totales.orderBy(["REFERENCIA", "FECHA"])

# Obtener el último precio de compra para cada referencia de producto
from pyspark.sql.window import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy("REFERENCIA").orderBy(F.desc("FECHA"))
upc = ct_py.withColumn("row_num", F.row_number().over(window_spec)).filter(col("row_num") == 1).select("REFERENCIA", "PC.UNIT")

# Guardar el DataFrame resultante en Excel
ruta_guardar = 'F:/VALIJA DIGITAL/GRS/analisis/VSC/upc.xlsx'
upc.write.format("com.crealytics.spark.excel").option("header", "true").mode("overwrite").save(ruta_guardar)

# Cargar los archivos de clientes y comerciales
clientes = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("CLIENTES.xlsx")
comerciales = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("comerciales.xlsx")

# Cargar el archivo de ventas semanales
ventas_semanales = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("vs_py.xlsx")

# Concatenar SERIE-NFAC
ventas_semanales = ventas_semanales.withColumn("FACTURA", concat_ws("-", col("SERIE").cast("string"), col("NFAC").cast("string")))

# Modificar campos de fecha
ventas_semanales = ventas_semanales.withColumn("FECHA", to_date(col("FECHA"), 'dd/MM/yyyy'))
ventas_semanales = ventas_semanales.withColumn("DIA", dayofmonth(col("FECHA")))
ventas_semanales = ventas_semanales.withColumn("MES", month(col("FECHA")))
ventas_semanales = ventas_semanales.withColumn("MES.TXT", date_format(col("FECHA"), 'MMMM'))
ventas_semanales = ventas_semanales.withColumn("AÑO", year(col("FECHA")))
ventas_semanales = ventas_semanales.withColumn("D.SEM", date_format(col("FECHA"), 'EEEE'))
ventas_semanales = ventas_semanales.withColumn("SEMANA", weekofyear(col("FECHA")))
ventas_semanales = ventas_semanales.withColumn("TRIMESTRE", quarter(col("FECHA")))

# Left join con clientes para obtener R.SOCIAL y renombrar a CLIENTE
ventas_semanales = ventas_semanales.join(clientes.select("COD.CLIENTE", "R.SOCIAL"), on="COD.CLIENTE", how="left")
ventas_semanales = ventas_semanales.withColumnRenamed("R.SOCIAL", "CLIENTE")

# Left join con clientes para obtener NOMBRE COMERCIAL y renombrar a COMERCIAL
ventas_semanales = ventas_semanales.join(clientes.select("COD.CLIENTE", "NOMBRE COMERCIAL"), on="COD.CLIENTE", how="left")
ventas_semanales = ventas_semanales.withColumnRenamed("NOMBRE COMERCIAL", "COMERCIAL")

# Left join con comerciales para obtener VENDIDO POR
ventas_semanales = ventas_semanales.join(comerciales.select(col("COD").alias("COD_COMERCIAL"), "VENDIDO POR"), ventas_semanales["COD.COMERCIAL"] == comerciales["COD"], how="left")
ventas_semanales = ventas_semanales.drop("COD")

# Actualizar valores NaN en 'VENDIDO POR' con 'LF'
ventas_semanales = ventas_semanales.fillna({'VENDIDO POR': 'LF'})

# Cargar el archivo de upc
upc = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("upc.xlsx")

# Renombrar columnas de upc y hacer el join con ventas_semanales
upc = upc.withColumnRenamed("REFERENCIA", "ID_REF").withColumnRenamed("PC.UNIT", "PRECIO_U")
ventas_semanales = ventas_semanales.join(upc.select("ID_REF", "PRECIO_U"), ventas_semanales["REFERENCIA"] == upc["ID_REF"], how="left")
ventas_semanales = ventas_semanales.drop("ID_REF").withColumnRenamed("PRECIO_U", "PC.UNIT")

# Actualizar valores NaN en 'PC.UNIT' con 'PVP/2' para la familia 45
ventas_semanales = ventas_semanales.withColumn("PC.UNIT", expr("CASE WHEN FAMILIA == 45 AND PC.UNIT IS NULL THEN PVP / 2 ELSE PC.UNIT END"))

# Filtrar datos de la familia 45 para verificación
filtro_familia_45 = ventas_semanales.filter(col("FAMILIA") == 45).select("PVP", "PC.UNIT")
filtro_familia_45.show()

# Cambios de formato y cálculo de columnas adicionales
ventas_semanales = ventas_semanales.withColumn("PC.UNIT", col("PC.UNIT").cast("float"))
ventas_semanales = ventas_semanales.withColumn("CANTIDAD", col("CANTIDAD").cast("float"))
ventas_semanales = ventas_semanales.withColumn("TTL", col("TTL").cast("float"))
ventas_semanales = ventas_semanales.withColumn("PC.TTL", col("PC.UNIT") * col("CANTIDAD"))
ventas_semanales = ventas_semanales.withColumn("BENEFICIO", col("TTL") - col("PC.TTL"))
ventas_semanales = ventas_semanales.withColumn("M", round((col("TTL") / col("PC.TTL") - 1) * 100, 2))

# Guardar ventas_semanales en Excel
ruta_guardar_vs = 'F:/VALIJA DIGITAL/GRS/analisis/VSC/vs_py.xlsx'
ventas_semanales.write.format("com.crealytics.spark.excel").option("header", "true").mode("overwrite").save(ruta_guardar_vs)

# Cargar el archivo de ventas totales y combinar con ventas semanales
ventas_totales = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("vt_py.xlsx")
ventas_totales = ventas_totales.union(ventas_semanales)

# Guardar el DataFrame combinado en Excel
ruta_guardar_vt = 'F:/VALIJA DIGITAL/GRS/analisis/VSC/prueba.xlsx'
ventas_totales.write.format("com.crealytics.spark.excel").option

