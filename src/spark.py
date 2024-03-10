# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit
#
# # 初始化SparkSession
# spark = SparkSession.builder \
#     .appName("itinerariesDataProcessing") \
#     .getOrCreate()
#
# # 读取CSV文件
# df = spark.read.csv("/media/scdx/E2/wp/yelp/itineraries.csv", header=True, inferSchema=True)
#
#
# # 机场信息表 与城市信息表需要外部数据
# # 提取起飞和降落机场的三字码
# starting_airports = df.select(col("startingAirport").alias("airport_code")).distinct()
# destination_airports = df.select(col("destinationAirport").alias("airport_code")).distinct()
# # 合并两个列表并去重
# unique_airports = starting_airports.union(destination_airports).distinct()
# # 显示结果
# unique_airports.show()
#
#
# # 提取航空公司二字代码和名称
# unique_airlines = df.select(
#     col("segmentsAirlineCode").alias("airline_code"),
#     col("segmentsAirlineName").alias("airline_name")
# ).distinct()
#
# # 显示结果
# unique_airlines.show()
#
# # 提取飞机型号
# unique_aircraft_types = df.select(
#     col("segmentsEquipmentDescription").alias("aircraft_type")
# ).distinct()
#
# # 显示结果
# unique_aircraft_types.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, concat_ws,arrays_zip, explode_outer

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("itinerariesDataProcessing") \
    .getOrCreate()

# 读取CSV文件
df = spark.read.csv("/media/scdx/E2/wp/yelp/itineraries.csv", header=True, inferSchema=True)

# # 处理分段航程信息，拆分分段的机场三字码、航空公司二字码、航空公司名称和飞机型号
# split_segments = df.withColumn("segmentsArrivalAirportCode", explode(split(col("segmentsArrivalAirportCode"), "\|\|"))) \
#                    .withColumn("segmentsDepartureAirportCode", explode(split(col("segmentsDepartureAirportCode"), "\|\|"))) \
#                    .withColumn("segmentsAirlineCode", explode(split(col("segmentsAirlineCode"), "\|\|"))) \
#                    .withColumn("segmentsAirlineName", explode(split(col("segmentsAirlineName"), "\|\|"))) \
#                    .withColumn("segmentsEquipmentDescription", explode(split(col("segmentsEquipmentDescription"), "\|\|")))

# # 提取唯一的机场三字码
# unique_airports = split_segments.select("segmentsArrivalAirportCode").distinct() \
#                  .union(split_segments.select("segmentsDepartureAirportCode").distinct()).distinct()

# # 提取唯一的航空公司二字代码和名称
# unique_airlines = split_segments.select("segmentsAirlineCode", "segmentsAirlineName").distinct()

# # 提取唯一的飞机型号
# unique_aircraft_types = split_segments.select("segmentsEquipmentDescription").distinct()

# 显示结果
# print("Unique Airports:")
# unique_airports.show(truncate=False)

# print("Unique Airlines:")
# unique_airlines.show(truncate=False)

# print("Unique Aircraft Types:")
# unique_aircraft_types.show(truncate=False)

# 指定保存文件的目录
output_dir = "/media/scdx/D/wp/Yelp/Bigdata/data/"

# # 保存唯一机场三字码为CSV
# unique_airports.coalesce(1).write.option("header", "true").csv(output_dir + "unique_airports.csv", mode="overwrite")

# 保存唯一航空公司二字码和名称为CSV
# unique_airlines.coalesce(1).write.option("header", "true").csv(output_dir + "unique_airlines.csv", mode="overwrite")

# # 保存唯一飞机型号为CSV
# unique_aircraft_types.coalesce(1).write.option("header", "true").csv(output_dir + "unique_aircraft_types.csv", mode="overwrite")


# 首先，将每段航程的航空公司二字码和名称拆分成数组
df = df.withColumn("segmentsAirlineCodeArray", split(col("segmentsAirlineCode"), "\|\|")) \
       .withColumn("segmentsAirlineNameArray", split(col("segmentsAirlineName"), "\|\|"))

# 使用arrays_zip函数将二字码和名称组合成结构化数组
df = df.withColumn("airlineInfo", arrays_zip("segmentsAirlineCodeArray", "segmentsAirlineNameArray"))

# 使用explode_outer展开结构化数组，并保留每个元素的结构
df = df.withColumn("explodedAirlineInfo", explode_outer("airlineInfo"))

# 提取结构化数组中的二字码和名称
df = df.withColumn("airlineCode", col("explodedAirlineInfo.segmentsAirlineCodeArray")) \
       .withColumn("airlineName", col("explodedAirlineInfo.segmentsAirlineNameArray"))

# 选择需要的列，并去重
df_unique_airlines = df.select("airlineCode", "airlineName").distinct()

df_unique_airlines.show(truncate=False)
# 保存唯一航空公司二字码和名称为CSV
df_unique_airlines.show(truncate=False)
df_unique_airlines.coalesce(1).write.option("header", "true").csv(output_dir + "unique_airlines.csv", mode="overwrite")
