# import mysql.connector
#
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.master("local").appName("db Connector").getOrCreate()
#
# # Establish a connection
# conn = mysql.connector.connect(user='root', database='ingest',
#                                password='root',
#                                host="localhost",
#                                port=3306)


from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("Word Count")\
    .master("local[1]")\
    .config("spark.driver.extraClassPath", "C:\BALAKUMAR\LocalFiles\SparkProjects\mysql connector jar\mysql-connector-java-8.0.29.jar")\
    .getOrCreate()

# without partitions

# dataframe_mysql = spark.read\
#     .format("jdbc")\
#     .option("url", "jdbc:mysql://localhost/ingest")\
#     .option("driver", "com.mysql.jdbc.Driver")\
#     .option("dbtable", "flight2").option("user", "root")\
#     .option("password", "root").load()

# with partitions

dataframe_mysql = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:mysql://localhost/ingest")\
    .option("driver", "com.mysql.jdbc.Driver")\
    .option("dbtable", "flight2").option("user", "root")\
    .option("partitionColumn", "ID")\
    .option("numPartitions", 3)\
    .option("lowerBound", 0)\
    .option("upperBound", 6)\
    .option("password", "root").load()



print(dataframe_mysql.columns)

dataframe_mysql.show()

input("press any key to stop")