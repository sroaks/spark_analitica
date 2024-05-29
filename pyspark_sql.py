from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("PySpark SQL Server Integration") \
    .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:9.2.1.jre8") \
    .getOrCreate()

# Configurar la conexión a SQL Server
server = 'PRA_GENERAL\SQLEXPRESS'
database = 'Prueba DB'
url = f"jdbc:sqlserver://{server};databaseName={database};integratedSecurity=true;"

# Nombre de la tabla en la base de datos
tabla_sql = 'dbo.hola_desde_pyspark'

# Eliminar la tabla si existe
drop_table_query = f"IF OBJECT_ID('{tabla_sql}', 'U') IS NOT NULL DROP TABLE {tabla_sql};"

# Ejecutar la consulta de eliminación usando pyodbc
import pyodbc
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes')
cursor = conn.cursor()
cursor.execute(drop_table_query)
conn.commit()
cursor.close()
conn.close()

# Cargar los datos desde el archivo Excel
df_pandas = pd.read_excel('prueba_sql.xlsx')

# Convertir el DataFrame de pandas a Spark DataFrame
schema = StructType([
    StructField('REFERENCIA', StringType(), True),
    StructField('DESCRIPCION', StringType(), True),
    StructField('COD.CLIENTE', FloatType(), True),
    StructField('CANTIDAD', FloatType(), True),
    StructField('PVP', FloatType(), True),
    StructField('DTO.V', FloatType(), True),
    StructField('TTL', FloatType(), True),
    StructField('SERIE', StringType(), True),
    StructField('NFAC', FloatType(), True),
    StructField('FECHA', TimestampType(), True),
    StructField('COD.COMERCIAL', FloatType(), True),
    StructField('FAMILIA', StringType(), True),
    StructField('FACTURA', StringType(), True),
    StructField('DIA', FloatType(), True),
    StructField('MES', FloatType(), True),
    StructField('MES.TXT', StringType(), True),
    StructField('AÑO', FloatType(), True),
    StructField('D.SEM', StringType(), True),
    StructField('SEMANA', FloatType(), True),
    StructField('TRIMESTRE', FloatType(), True),
    StructField('CLIENTE', StringType(), True),
    StructField('COMERCIAL', StringType(), True),
    StructField('VENDIDO POR', StringType(), True),
    StructField('PC.UNIT', FloatType(), True),
    StructField('PC.TTL', FloatType(), True),
    StructField('BENEFICIO', FloatType(), True),
    StructField('MARGEN', FloatType(), True)
])

spark_df = spark.createDataFrame(df_pandas, schema=schema)

# Guardar el DataFrame en SQL Server
spark_df.write \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", tabla_sql) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("overwrite") \
    .save()
