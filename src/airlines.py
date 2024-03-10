import csv

import mysql.connector

# 数据库连接配置，请根据实际情况调整
db_config = {
    'user': 'root',
    'password': 'root',
    'host': '8.137.118.233',
    'port': '3306',
    'database': 'flight_db',
}

# 航空联盟信息
alliances = [
    ('Star Alliance'),  # 星空联盟
    ('One World'),  # 寰宇一家
    ('SkyTeam'),  # 天合联盟
]

try:
    # 连接数据库
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # SQL插入语句
    insert_query = "INSERT INTO Airline_Alliance_Info (alliance_name) VALUES (%s)"

    # 执行插入操作
    for alliance in alliances:
        cursor.execute(insert_query, (alliance,))

    # 提交事务
    conn.commit()
    print(f"成功插入{cursor.rowcount}条航空联盟信息。")
except mysql.connector.Error as e:
    print(f"数据库错误: {e}")
finally:
    if conn.is_connected():
        cursor.close()
        conn.close()


# 飞机型号
def insert_aircraft_models_from_file(file_path):
    try:
        # 连接数据库
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # 插入SQL语句
        insert_query = """
            INSERT INTO aircraft_type_info (aircraft_type_name, manufacturer)
            VALUES (%s, %s)
        """

        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                model = row[0]  # 假设型号在每行的第一个位置
                manufacturer = model.split()[0]  # 提取制造商
                # 执行插入操作
                cursor.execute(insert_query, (model, manufacturer))

        # 提交事务
        conn.commit()
        print("成功插入飞机型号及其制造商信息。")
    except mysql.connector.Error as e:
        print(f"数据库错误: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


# 调用函数，开始插入数据
insert_aircraft_models_from_file('/media/scdx/D/wp/Yelp/Bigdata/data/unique_aircraft_types.csv/part-00000-a10a1e37-f36b-49f0-91ad-a1ffabce2cf8-c000.csv')



# 航空公司与联盟的映射
airline_alliance_mapping = {
    "JetBlue Airways": 'SkyTeam',
    "Key Lime Air": None,
    "Frontier Airlines": None,
    "Boutique Air": None,
    "Alaska Airlines": "OneWorld",
    "Sun Country Airlines": None,
    "Spirit Airlines": None,
    "Contour Airlines": None,
    "American Airlines": "OneWorld",
    "United": "Star Alliance",
    "Cape Air": None,
    "Delta": "SkyTeam",
    "Southern Airways Express": None,
    "Hawaiian Airlines": None,
    "Silver Airways": None
}

airline_country_mapping = {
    "JetBlue Airways": "United States",
    "Key Lime Air": "United States",
    "Frontier Airlines": "United States",
    "Boutique Air": "United States",
    "Alaska Airlines": "United States",
    "Sun Country Airlines": "United States",
    "Spirit Airlines": "United States",
    "Contour Airlines": "United States",
    "American Airlines": "United States",
    "United": "United States",
    "Cape Air": "United States",
    "Delta": "United States",
    "Southern Airways Express": "United States",
    "Hawaiian Airlines": "United States",
    "Silver Airways": "United States",
    # 添加更多航空公司和国家映射
}

# 航空联盟ID映射（假设这些ID已经存在于数据库的Airline_Alliance_Info表中）
alliance_id_mapping = {
    "Star Alliance": 1,
    "OneWorld": 2,
    "SkyTeam": 3
}

def insert_airlines_from_file(file_path):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                airline_name = row["airlineName"]
                country = airline_country_mapping.get(airline_name, "Unknown")  # 如果找不到，则默认为"Unknown"
                alliance_name = airline_alliance_mapping.get(airline_name)
                alliance_id = alliance_id_mapping.get(alliance_name, None)  # 如果找不到联盟，则默认为None

                insert_query = """
                    INSERT INTO airline_info (airline_name, country, alliance_id)
                    VALUES (%s, %s, %s)
                """
                cursor.execute(insert_query, (airline_name, country, alliance_id))

        conn.commit()
        print("航空公司信息及所在国家插入完成。")
    except mysql.connector.Error as e:
        print(f"数据库错误: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# 文件路径
file_path = '/media/scdx/D/wp/Yelp/Bigdata/data/unique_airlines.csv/part-00000-6d0fc7d2-cced-4a5c-9a8e-6cf90a64cc7a-c000.csv'
insert_airlines_from_file(file_path)