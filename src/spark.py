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
from pyspark.sql.functions import col, explode, split

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


# 假设df是你的DataFrame
# 分别处理航空公司二字码和航空公司名称，确保它们在相同行上对齐
airline_codes = df.select(explode(split(col("segmentsAirlineCode"), "\|\|")).alias("airline_code"))
airline_names = df.select(explode(split(col("segmentsAirlineName"), "\|\|")).alias("airline_name"))

# 假设每段航程的二字码和名称顺序是一致的，我们需要一个方法来保证行对齐
# 一种简单的策略是添加行号作为新列，然后按行号join两个DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number

# 为airline_codes和airline_names添加行号
windowSpec  = Window.orderBy(monotonically_increasing_id())
airline_codes = airline_codes.withColumn("row_num", row_number().over(windowSpec))
airline_names = airline_names.withColumn("row_num", row_number().over(windowSpec))

# 根据行号join
airline_info = airline_codes.join(airline_names, "row_num").select("airline_code", "airline_name")

# 去除重复的行
airline_info = airline_info.distinct()

# 保存唯一航空公司二字码和名称为CSV
airline_info.show(truncate=False)
airline_info.coalesce(1).write.option("header", "true").csv(output_dir + "unique_airlines.csv", mode="overwrite")
