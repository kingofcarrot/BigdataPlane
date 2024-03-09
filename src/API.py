import requests
import mysql.connector
import csv
from mysql.connector import Error

API_URL = "http://test.api.amadeus.com"
API_KEY = "1msvGi376OElHTLJqCVdKEGgrxuG6Vlc"
API_SECRET = "ac2v1IywgF5FPSGv"
AIRPORT_INFO_URL = 'https://test.api.amadeus.com/v1/reference-data/locations/airports'
API_ENDPOINT = 'https://test.api.amadeus.com/v1/reference-data/locations'
API_CITY_ENDPOINT = 'https://test.api.amadeus.com/v1/reference-data/locations/cities'


# 数据库配置
db_config = {
    'user': 'root',
    'password': 'root',
    'host': '8.137.118.233',
    'port': '3306',
    'database': 'flight_db',
}


# 获取token
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
        # 成功获取访问令牌
        access_token = response.json().get('access_token')
        print("Access Token:", access_token)
        return access_token
    else:
        # 获取访问令牌失败
        print(f"Failed to get access token: {response.text}")
        return None

access_token = get_access_token(API_KEY, API_SECRET)

# 获取机场信息
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


# 获取城市信息
def fetch_city_info(city_keyword):
    """Fetch city information based on a keyword using Amadeus API."""
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {
        'keyword': city_keyword,  # 城市名称关键词
        'subType': 'CITY',  # 指定查询子类型为城市
    }
    response = requests.get(f"https://test.api.amadeus.com/v1/reference-data/locations", headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get('data', [])
        if data:
            # 假设我们只关心列表中的第一个匹配城市
            city_info = data[0]
            return {
                'name': city_info['name'],
                'iataCode': city_info.get('iataCode', ''),
                'countryCode': city_info['address'].get('countryCode', ''),
                'latitude': str(city_info['geoCode']['latitude']),
                'longitude': str(city_info['geoCode']['longitude']),
            }
    else:
        print(f"Failed to fetch information for city {city_keyword}: {response.status_code}, {response.text}")
    return None


def insert_city_if_not_exists(city_info, cursor):
    """如果城市不存在于City_Info表中，则插入城市并返回city_id"""
    select_query = "SELECT city_id FROM City_Info WHERE city_name = %s"
    cursor.execute(select_query, (city_info['name'],))
    result = cursor.fetchone()
    if result:
        return result[0]  # 城市已存在，返回city_id
    else:
        # 城市不存在，插入新城市
        insert_query = """
            INSERT INTO City_Info (city_name, city_code, latitude, longitude)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
        city_info['name'], city_info.get('iataCode'), city_info.get('latitude'), city_info.get('longitude')))
        return cursor.lastrowid  # 返回新插入城市的city_id


def insert_airport_info(airport_info, city_id, latitude, longitude, cursor):

    insert_query = """
        INSERT INTO Airport_Info (airport_code, airport_name, city_id, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        airport_info['iataCode'],  # 机场代码
        airport_info['name'],  # 机场名称
        city_id,  # 关联的城市ID
        latitude,  # 城市的纬度
        longitude,  # 城市的经度
    ))

# def insert_airport_info_to_db(airport_info):
#     """Insert fetched airport data into MySQL database."""
#     try:
#         conn = mysql.connector.connect(**db_config)
#         cursor = conn.cursor()
#         insert_query = """
#                 INSERT INTO Airport_Info (airport_code, airport_name, city_id, latitude, longitude)
#                 VALUES (%s, %s, %s, %s, %s)
#             """
#         default_city_id = get_or_create_default_city_id(cursor)
#         if default_city_id is None:
#             print("Failed to obtain default city_id.")
#             return
#         data = (
#             airport_info.get('iataCode', 'N/A'),  # 机场三字码，没有则默认'N/A'
#             airport_info.get('name', 'Unknown'),  # 机场名称，默认'Unknown'
#             default_city_id,  # 关联的城市ID，这里使用假设值
#             airport_info.get('geoCode', {}).get('latitude', 0),  # 纬度，默认0
#             airport_info.get('geoCode', {}).get('longitude', 0),  # 经度，默认0
#         )
#         cursor.execute(insert_query, data)
#         conn.commit()  # 提交事务
#         print("Airport information inserted successfully.")
#     except Error as e:
#         print(f"Error: {e}")
#     finally:
#         if conn.is_connected():
#             cursor.close()
#             conn.close()

# 主执行函数
def load_airport_codes_from_csv(file_path):
    """从CSV文件加载机场三字码"""
    airport_codes = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # 跳过表头
        for row in reader:
            airport_codes.append(row[0])  # 假设机场代码在第一列
    return airport_codes


def main():
    file_path = '/media/scdx/D/wp/Yelp/Bigdata/data/unique_airports.csv/part-00000-dad9bad5-c1dd-4000-9b2c-fc32ecde24b8-c000.csv'
    airport_codes = load_airport_codes_from_csv(file_path)
    # airport_codes = ['OAK', 'LGA', 'BOS', 'EWR', 'DEN', 'IAD', 'CLT', 'MIA', 'DFW', 'SFO', 'ATL', 'ORD', 'DTW', 'LAX',
    #                  'JFK', 'PHL']
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    for code in airport_codes:
        airport_info = fetch_airport_info(code)  # 获取机场信息
        if airport_info:
            city_info = fetch_city_info(airport_info['address']['cityName'])  # 获取城市信息
            if city_info:
                city_id = insert_city_if_not_exists(city_info, cursor)  # 插入城市信息（如果需要）
                insert_airport_info(airport_info, city_id, city_info['latitude'], city_info['longitude'],cursor)  # 插入机场信息
                print(f"Inserted airport and city info for {code}.")
            else:
                print(f"Failed to fetch city info for airport {code}.")
        else:
            print(f"Failed to fetch airport info for {code}.")

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
