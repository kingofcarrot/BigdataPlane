import mysql.connector
from mysql.connector import Error
import random
from datetime import datetime, timedelta

# 数据库配置
db_config = {
    'user': 'root',
    'password': '123456789@SCU',
    'host': '8.137.118.233',
    'port': '3306',
    'database': 'flight_db',
}

# 连接数据库
try:
    conn = mysql.connector.connect(**db_config)
    if conn.is_connected():
        print('Successfully connected to the database')
except Error as e:
    print(f"Error: {e}")
    exit()

cursor = conn.cursor()

# 假定用户类型
user_types = ["商务旅行者", "旅行发烧友", "家庭度假者"]

def generate_user_data(user_type):
    """
    为不同类型的用户生成数据，并插入到数据库中。
    """
    # 用户基本信息生成
    name = "User_" + user_type + str(random.randint(1, 1000))
    age = random.randint(25, 50) if user_type == "商务旅行者" else random.randint(18, 60)
    gender = random.choice(["男", "女"])
    email = name.lower() + "@example.com"
    user_type_specific = user_type
    user_name = name
    password = "password123"

    # 用户偏好生成
    price_sensitivity = "不敏感" if user_type == "商务旅行者" else "中等"
    preferred_airlines = "Delta" if user_type == "商务旅行者" else "任意"
    stopover_preference = "直飞" if user_type == "商务旅行者" else "不限"
    travel_class = "商务舱" if user_type == "商务旅行者" else "经济舱"
    flight_time_preference = "早班机" if user_type == "商务旅行者" else "不限"
    airport_preference = "首选"
    aircraft_type_preference = "Boeing 737-800" if user_type == "商务旅行者" else "任意"
    flight_duration_preference = "长途" if user_type == "商务旅行者" else "不限"
    travel_distance_preference = "长距离" if user_type == "商务旅行者" else "不限"

    # 插入用户基本信息和用户偏好
    user_info_query = """
    INSERT INTO user_info (name, age, gender, email, user_type, user_name, password)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(user_info_query, (name, age, gender, email, user_type_specific, user_name, password))
    user_id = cursor.lastrowid

    user_preferences_query = """
    INSERT INTO user_preference (user_id, price_sensitivity, preferred_airlines, stopover_preference,
    travel_class, flight_time_preference, airport_preference, aircraft_type_preference,
    flight_duration_preference, travel_distance_preference)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(user_preferences_query, (user_id, price_sensitivity, preferred_airlines, stopover_preference,
                                            travel_class, flight_time_preference, airport_preference,
                                            aircraft_type_preference, flight_duration_preference,
                                            travel_distance_preference))

    # 生成飞行记录
    for _ in range(random.randint(5, 15)):  # 假设每个用户有5到15条飞行记录
        flight_date = (datetime.now() - timedelta(days=random.randint(1, 365))).date()
        starting_airport = "ATL"
        destination_airport = "BOS"
        travel_duration = "PT2H29M"
        is_non_stop = True if user_type == "商务旅行者" else random.choice([True, False])
        base_fare = random.uniform(200, 1000)
        total_fare = base_fare * 1.2
        segments_departure_time_epoch = int(datetime.now().timestamp())
        segments_arrival_time_epoch = segments_departure_time_epoch + 7200  # 假设飞行时间为2小时
        segments_airline_name = preferred_airlines if preferred_airlines != "任意" else "Delta"
        segments_aircraft_description = aircraft_type_preference if aircraft_type_preference != "任意" else "Airbus A321"
        segments_duration_in_seconds = 7200  # 假设飞行时间为2小时
        segments_distance = random.randint(500, 1500) # 假设飞行距离在500到1500英里之间

    user_flight_records_query = """
            INSERT INTO user_flight_records (user_id, flight_date, starting_airport, destination_airport,
            travel_duration, is_non_stop, base_fare, total_fare, segments_departure_time_epoch,
            segments_arrival_time_epoch, segments_airline_name, segments_aircraft_description,
            segments_duration_in_seconds, segments_distance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
    cursor.execute(user_flight_records_query, (user_id, flight_date, starting_airport, destination_airport,
                                                   travel_duration, is_non_stop, base_fare, total_fare,
                                                   segments_departure_time_epoch, segments_arrival_time_epoch,
                                                   segments_airline_name, segments_aircraft_description,
                                                   segments_duration_in_seconds, segments_distance))

    # 提交到数据库
    conn.commit()

user_types = ["商务旅行者", "旅行发烧友", "家庭度假者"]
for user_type in user_types:
    for _ in range(10):  # 每种类型生成10个用户及其数据
        generate_user_data(user_type)

cursor.close()
conn.close()