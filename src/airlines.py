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