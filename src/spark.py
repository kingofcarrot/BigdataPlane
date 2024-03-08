from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("itinerariesDataProcessing") \
    .getOrCreate()

# 读取CSV文件
df = spark.read.csv("/media/scdx/E2/wp/yelp/itineraries.csv", header=True, inferSchema=True)
# df.show()

# 机场信息表 与城市信息表需要外部数据
# 提取起飞和降落机场的三字码
starting_airports = df.select(col("startingAirport").alias("airport_code")).distinct()
destination_airports = df.select(col("destinationAirport").alias("airport_code")).distinct()
# 合并两个列表并去重
unique_airports = starting_airports.union(destination_airports).distinct()
# 显示结果
unique_airports.show()


# 提取航空公司二字代码和名称
unique_airlines = df.select(
    col("segmentsAirlineCode").alias("airline_code"),
    col("segmentsAirlineName").alias("airline_name")
).distinct()

# 显示结果
unique_airlines.show()

# 提取飞机型号
unique_aircraft_types = df.select(
    col("segmentsEquipmentDescription").alias("aircraft_type")
).distinct()

# 显示结果
unique_aircraft_types.show()



