import requests
import mysql.connector

from mysql.connector import Error
API_URL = "http://test.api.amadeus.com"
API_KEY = "ctAGVNMkePDqhe38vFxgrZcgYWWNzMy9"
AIRPORT_INFO_URL = 'https://test.api.amadeus.com/v1/reference-data/locations/airports'
API_ENDPOINT = 'https://test.api.amadeus.com/v1/reference-data/locations'
client_secret ='3FixNf3N2fvQKLMj'

# 数据库配置
db_config = {
    'user': 'root',
    'password': 'root',
    'host': '8.137.118.233',
    'port':'3306',
    'database': 'flight_db',
}


def get_access_token(client_id, client_secret):
    """获取访问令牌"""
    url = "https://test.api.amadeus.com/v1/security/oauth2/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        print(f"Failed to get access token: {response.text}")
        return None

access_token = get_access_token(API_KEY, client_secret)
if not access_token:
    print("Failed to get access token.")

def fetch_airport_info(airport_code):
    """Fetch airport information using Amadeus API."""
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'keyword': airport_code, 'subType': 'AIRPORT'}
    response = requests.get(API_ENDPOINT, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get('data', [])
        if data:
            # 取列表中的第一个机场信息
            return data[0]
    else:
        print(f"Failed to fetch information for airport {airport_code}: {response.status_code}")
    return None



# # 查询参数
# params = {
#     'keyword': 'SFO',  # 示例关键词，你可以改为其他机场代码
#     'subType': 'AIRPORT',  # 查询类型为机场
# }
#
# # 请求头
# headers = {
#     'Authorization': f'Bearer {access_token}'
# }
#
# # 获取
# def fetch_locations(keyword):
#     """Fetch locations matching a given keyword from Amadeus API."""
#     # 更新查询参数中的关键词
#     params['keyword'] = keyword
#     response = requests.get(API_ENDPOINT, headers=headers, params=params)
#     if response.status_code == 200:
#         return response.json().get('data')
#     else:
#         print(f"Failed to fetch information for keyword {keyword}: {response.status_code}")
#         return None
#
# # 插入机场信息到数据库
# def insert_airport_info_to_db(airport_data):
#     try:
#         conn = mysql.connector.connect(**db_config)
#         cursor = conn.cursor()
#         insert_query = """INSERT INTO Airport_Info (airport_code, airport_name, city_name, latitude, longitude)
#                                   VALUES (%s, %s, %s, %s, %s)"""
#         for airport in airport_data:
#             # 假设每个airport字典中包含code, name, cityName, geoCode纬度, geoCode经度
#             data = (
#                 airport.get('iataCode'),  # 使用IATA代码作为airport_code
#                 airport.get('name'),  # 机场名称
#                 airport.get('address', {}).get('cityName'),  # 所在城市
#                 airport.get('geoCode', {}).get('latitude'),  # 纬度
#                 airport.get('geoCode', {}).get('longitude'),  # 经度
#             )
#             cursor.execute(insert_query, data)
#         conn.commit()
#         print(f"Successfully inserted {cursor.rowcount} rows.")
#     except mysql.connector.Error as e:
#         print(f"Error: {e}")
#     finally:
#         if conn.is_connected():
#             cursor.close()
#             conn.close()
#
#
# # 查询的机场代码列表
# airport_codes = ['OAK', 'LGA', 'BOS', 'EWR', 'DEN', 'IAD', 'CLT', 'MIA', 'DFW', 'SFO', 'ATL', 'ORD', 'DTW', 'LAX', 'JFK', 'PHL']
# for code in airport_codes:
#     info = fetch_locations(code)
#     if info:
#         for airport in info:
#             print(f"Airport Code: {airport.get('iataCode')}, Name: {airport.get('name')}, City: {airport.get('address', {}).get('cityName')}, Country: {airport.get('address', {}).get('countryName')}")
#     else:
#         print(f"No information found for airport {code}")
def get_or_create_default_city_id(cursor):
    """获取默认城市的city_id，如果不存在则创建一个默认城市并返回其city_id"""
    default_city_name = "Default City"
    default_city_code = "DEF"  # 为默认城市指定一个假设的city_code
    try:
        # 尝试查找默认城市的city_id
        query = "SELECT city_id FROM City_Info WHERE city_name = %s"
        cursor.execute(query, (default_city_name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            # 默认城市不存在，插入默认城市并返回其city_id
            # 确保包括city_code在内的所有必需字段都被正确赋值
            insert_query = "INSERT INTO City_Info (city_name, city_code) VALUES (%s, %s)"
            cursor.execute(insert_query, (default_city_name, default_city_code))
            return cursor.lastrowid  # 返回插入的默认城市的city_id
    except Error as e:
        print(f"Error: {e}")
        return None

def insert_airport_info_to_db(airport_info):
    """Insert fetched airport data into MySQL database."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        insert_query = """
                INSERT INTO Airport_Info (airport_code, airport_name, city_id, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s)
            """
        default_city_id = get_or_create_default_city_id(cursor)
        if default_city_id is None:
            print("Failed to obtain default city_id.")
            return
        data = (
            airport_info.get('iataCode', 'N/A'),  # 机场三字码，没有则默认'N/A'
            airport_info.get('name', 'Unknown'),  # 机场名称，默认'Unknown'
            default_city_id,  # 关联的城市ID，这里使用假设值
            airport_info.get('geoCode', {}).get('latitude', 0),  # 纬度，默认0
            airport_info.get('geoCode', {}).get('longitude', 0),  # 经度，默认0
        )
        cursor.execute(insert_query, data)
        conn.commit()  # 提交事务
        print("Airport information inserted successfully.")
    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# 主执行函数
def main():
    airport_codes = ['OAK', 'LGA', 'BOS', 'EWR', 'DEN', 'IAD', 'CLT', 'MIA', 'DFW', 'SFO', 'ATL', 'ORD', 'DTW', 'LAX', 'JFK', 'PHL']
    for code in airport_codes:
        airport_info = fetch_airport_info(code)
        if airport_info:
            insert_airport_info_to_db(airport_info)
        else:
            print(f"No information found for airport {code}")

if __name__ == "__main__":
    main()