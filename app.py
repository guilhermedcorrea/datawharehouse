import os
from pyspark.sql.session import SparkSession
import os



os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-19"
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'
os.environ['HADOOP_HOME'] =  r'C:\Users\Guilherme\Documents\datawharehouse\spark-3.3.1-bin-hadoop3\hadoop\bin\winutils.exe'

import findspark
findspark.init(r'C:\Users\Guilherme\Documents\datawharehouse\spark-3.3.1-bin-hadoop3')

appName = "Spark - Setting Log Level"
master = "local"


# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .config("spark.driver.extraClassPath", r"C:\Users\Guilherme\Documents\datawharehouse\postgresql-42.5.1.jar")\
    .config("spark.eventLog.enabled","false")\
    .config("spark.executor.extraJavaOptions","=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")\
    .config("spark.driver.extraJavaOptions","-Dlog4jspark.root.logger=WARN,console")\
    .master(master) \
    .getOrCreate()


df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/datawarehouse") \
    .option("dbtable", "pentaho.r_user") \
    .option("user", "postgres") \
    .option("password", "123") \
    .option("driver", "org.postgresql.Driver") \
    .load()



spark.sparkContext.setLogLevel('WARN')
df.createOrReplaceTempView("insurance_df")

x = spark.sql("SELECT LOGIN FROM insurance_df WHERE LOGIN = 'admin'")

print(x.show())

