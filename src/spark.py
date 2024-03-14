import mysql.connector
import pandas as pd
from pyspark.sql.functions import col, lit, when, concat_ws
from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql import SparkSession


# 初始化SparkSession
spark = SparkSession.builder \
    .appName("itinerariesDataProcessing") \
    .config("spark.jars", "/media/scdx/D/wp/Yelp/mysql-connector-java-8.0.23.jar") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "4g") \
    .getOrCreate()

# 读取CSV文件
df = spark.read.csv("/media/scdx/E2/wp/yelp/itineraries.csv", header=True, inferSchema=True)

# 数据库配置
db_config = {
    'user': 'root',
    'password': '123456789@SCU',
    'host': '8.137.118.233',
    'port': '3306',
    'database': 'flight_db',
}

#  机场信息表 与城市信息表需要外部数据
#  提取起飞和降落机场的三字码
# starting_airports = df.select(col("startingAirport").alias("airport_code")).distinct()
# destination_airports = df.select(col("destinationAirport").alias("airport_code")).distinct()
#  合并两个列表并去重
# unique_airports = starting_airports.union(destination_airports).distinct()
#  显示结果
# unique_airports.show()
#  提取航空公司二字代码和名称
# unique_airlines = df.select(
#     col("segmentsAirlineCode").alias("airline_code"),
#     col("segmentsAirlineName").alias("airline_name")
# ).distinct()
# 显示结果
# unique_airlines.show()
# # 提取飞机型号
# unique_aircraft_types = df.select(
#     col("segmentsEquipmentDescription").alias("aircraft_type")
# ).distinct()
# 显示结果
# unique_aircraft_types.show()
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

# 指定保存文件的目录
# output_dir = "/media/scdx/D/wp/Yelp/Bigdata/data/"

# ********************airline_info********************
# # 保存唯一机场三字码为CSV
# unique_airports.coalesce(1).write.option("header", "true").csv(output_dir + "unique_airports.csv", mode="overwrite")

# 保存唯一航空公司二字码和名称为CSV
# unique_airlines.coalesce(1).write.option("header", "true").csv(output_dir + "unique_airlines.csv", mode="overwrite")

# # 保存唯一飞机型号为CSV
# unique_aircraft_types.coalesce(1).write.option("header", "true").csv(output_dir + "unique_aircraft_types.csv", mode="overwrite")


# # 首先，将每段航程的航空公司二字码和名称拆分成数组
# df = df.withColumn("segmentsAirlineCodeArray", split(col("segmentsAirlineCode"), "\|\|")) \
#        .withColumn("segmentsAirlineNameArray", split(col("segmentsAirlineName"), "\|\|"))
#
# # 使用arrays_zip函数将二字码和名称组合成结构化数组
# df = df.withColumn("airlineInfo", arrays_zip("segmentsAirlineCodeArray", "segmentsAirlineNameArray"))
#
# # 使用explode_outer展开结构化数组，并保留每个元素的结构
# df = df.withColumn("explodedAirlineInfo", explode_outer("airlineInfo"))
#
# # 提取结构化数组中的二字码和名称
# df = df.withColumn("airlineCode", col("explodedAirlineInfo.segmentsAirlineCodeArray")) \
#        .withColumn("airlineName", col("explodedAirlineInfo.segmentsAirlineNameArray"))
#
# # 选择需要的列，并去重
# df_unique_airlines = df.select("airlineCode", "airlineName").distinct()
#
# df_unique_airlines.show(truncate=False)
# # 保存唯一航空公司二字码和名称为CSV
# df_unique_airlines.show(truncate=False)
# df_unique_airlines.coalesce(1).write.option("header", "true").csv(output_dir + "unique_airlines.csv", mode="overwrite")


# ********************predict_price_info********************
# 确定前20个热门机场（以起飞和到达的总次数计）
# top_airports = df.select("startingAirport").union(df.select("destinationAirport")) \
#                  .groupBy("startingAirport") \
#                  .count() \
#                  .orderBy(F.desc("count")) \
#                  .limit(100) \
#                  .collect()
# top_airports_list = [row["startingAirport"] for row in top_airports]
# # 筛选前20个机场之间的航程记录
# filtered_df = df.filter(
#     df.startingAirport.isin(top_airports_list) &
#     df.destinationAirport.isin(top_airports_list)
# )
#
# # 使用窗口函数按起飞机场、到达机场和出发日期分组，计算每组的最低票价
# windowSpec = Window.partitionBy("startingAirport", "destinationAirport", "flightDate").orderBy("totalFare")
# min_fare_df = filtered_df.withColumn("row_number", F.row_number().over(windowSpec)) \
#                          .filter(F.col("row_number") == 1) \
#                          .drop("row_number")
#
# # 转换列名和数据类型以匹配SQL表结构
# final_df = min_fare_df.select(
#     F.col("startingAirport").alias("start_airport"),
#     F.col("destinationAirport").alias("dest_airport"),
#     F.col("totalFare").cast("string").alias("price"),
#     F.col("flightDate").alias("date")
# )
#
# # 插入数据到MySQL数据库
# final_df.write \
#     .format("jdbc") \
#     .option("url", "jdbc:mysql://8.137.118.233:3306/flight_db") \
#     .option("dbtable", "predict_price_info") \
#     .option("user", "root") \
#     .option("password", "root") \
#     .option("driver", "com.mysql.cj.jdbc.Driver") \
#     .mode("append") \
#     .save()
#
# spark.stop()

# 处理数据
processed_df = df.withColumn("is_not_stop", when(col("isNonStop") == "true", lit(1)).otherwise(lit(0))) \
    .withColumn("travel_duration", col("travelDuration")) \
    .withColumn("departure_date", col("flightDate")) \
    .withColumn("arrival_date", col("segmentsArrivalTimeRaw")) \
    .withColumn("segment_departure_time", concat_ws("||", "segmentsDepartureTimeRaw")) \
    .withColumn("segment_arrival_time", concat_ws("||", "segmentsArrivalTimeRaw")) \
    .withColumn("segment_departure_airport", concat_ws("||", "segmentsDepartureAirportCode")) \
    .withColumn("segment_arrival_airport", concat_ws("||", "segmentsArrivalAirportCode")) \
    .withColumn("segment_aircraft_type", concat_ws("||", "segmentsEquipmentDescription")) \
    .withColumn("segment_distance", concat_ws("||", "segmentsDistance"))

# 选择与数据库表匹配的列，确保没有重复的列被选取
final_df = processed_df.select(
    col("startingAirport").alias("start_airport"),
    col("destinationAirport").alias("dest_airport"),
    col("travel_duration"),
    col("departure_date"),
    col("totalFare").alias("total_fare"),
    col("totalTravelDistance").alias("total_distance"),
    col("is_not_stop"),
    col("arrival_date"),
    col("segment_departure_time"),
    col("segment_arrival_time"),
    col("segment_departure_airport"),
    col("segment_arrival_airport"),
    col("segment_aircraft_type"),
    col("segment_distance")
).distinct()  # 添加.distinct()以去除可能的重复记录


def insert_into_database(data):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        insert_query = """INSERT INTO flight_info (start_airport, dest_airport, travel_duration, departure_date, total_fare, total_distance, is_not_stop, arrival_date, segment_departure_time, segment_arrival_time, segment_departure_airport, segment_arrival_airport, segment_aircraft_type, segment_distance) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(insert_query, data)
        conn.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")
        print(f"Problematic data: {data}")  # 打印有问题的数据行
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# 给每行数据分配一个唯一的ID
final_df_with_id = final_df.withColumn("row_id", monotonically_increasing_id())

# 获取DataFrame的总行数
total_rows = final_df_with_id.count()

# 定义每批数据的大小
batch_size = 1000


# 分批处理并写入数据库
for start_row in range(0, total_rows, batch_size):
    end_row = start_row + batch_size
    # 使用row_id选择当前批次的数据
    batch_df = final_df_with_id.filter((col("row_id") >= start_row) & (col("row_id") < end_row))
    pandas_df = batch_df.toPandas()
    # 处理NaN值
    pandas_df = pandas_df.where(pd.notnull(pandas_df), None)

    # 将批次数据转换为列表，准备插入数据库
    data_to_insert = pandas_df.drop(columns=["row_id"]).to_dict(orient="records")

    # 逐行插入数据库
    for data in data_to_insert:
        insert_data_tuple = tuple(data.values())
        insert_into_database(insert_data_tuple)




