import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy import text
import torch  # 确保导入了torch


db_config = {
    'user': 'root',
    'password': '123456789@SCU',
    'host': '8.137.118.233',
    'port': '3306',
    'database': 'flight_db',
}

class FlightDataset:
    def __init__(self, db_config, mode='train', user_id=None, candidate_tickets_df=None):
        self.db_config = db_config
        self.mode = mode
        self.user_id = user_id
        self.candidate_tickets_df = candidate_tickets_df
        self.encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')  # 初始化 OneHotEncoder
        self.load_data()
        self.preprocess_data()
        self.encode_data()


    def load_data(self):
        # 使用 SQLAlchemy 创建数据库连接
        database_uri = "mysql+mysqlconnector://root:123456789%40SCU@8.137.118.233:3306/flight_db"
        engine = create_engine(database_uri)

        # 读取用户偏好数据
        if self.mode == 'eval' and self.user_id is not None:
            query = f"SELECT * FROM user_preference WHERE user_id = {self.user_id}"
            self.user_preferences = pd.read_sql(query, engine)
        else:
            self.user_preferences = pd.read_sql("SELECT * FROM user_preference", engine)

        # 读取用户飞行记录数据
        self.flight_records = pd.DataFrame()
        if self.mode == 'train':
            self.flight_records = pd.read_sql("SELECT * FROM user_flight_records", engine)

        if self.mode == 'eval' and self.candidate_tickets_df is not None:
            self.flight_info = self.candidate_tickets_df
        else:
            self.flight_info = pd.DataFrame()
        engine.dispose()

    def preprocess_data(self):
        # 确保所有的文本字段被转换为字符串类型
        # 注意：需要在每个数据集上分别应用
        def convert_to_str(df, categorical_cols):
            for col in categorical_cols:
                df[col] = df[col].astype(str)

        if not self.user_preferences.empty:
            user_pref_categorical_cols = self.user_preferences.select_dtypes(include=['object', 'bool']).columns
            convert_to_str(self.user_preferences, user_pref_categorical_cols)

        if not self.flight_records.empty:
            flight_records_categorical_cols = self.flight_records.select_dtypes(include=['object', 'bool']).columns
            convert_to_str(self.flight_records, flight_records_categorical_cols)

        if not self.flight_info.empty:
            flight_info_categorical_cols = self.flight_info.select_dtypes(include=['object', 'bool']).columns
            convert_to_str(self.flight_info, flight_info_categorical_cols)

    def encode_data(self):
        user_pref_fields = ['price_sensitivity', 'preferred_airlines', 'stopover_preference', 'travel_class',
                            'flight_time_preference', 'airport_preference', 'aircraft_type_preference',
                            'flight_duration_preference', 'travel_distance_preference']

        flight_record_fields = ['flight_date', 'starting_airport', 'destination_airport', 'travel_duration',
                                'is_non_stop', 'total_fare','segments_departure_time_epoch',
                                'segments_arrival_time_epoch','segments_aircraft_description','segments_distance']

        flight_info_fields = ['departureDate','startAirport','destAirport','travelDuration','notStop',
                              'totalFare','segmentDepartureTime','segmentArrivalTime','segmentAircraftType','segmentDistance',]

        # 根据模式确定使用的DataFrame
        if self.mode == 'train':
            data_to_fit = self.flight_records[flight_record_fields]
        elif self.mode == 'eval':
            data_to_fit = self.candidate_tickets_df[flight_info_fields]

        self.user_pref_encoder = OneHotEncoder(sparse=False, handle_unknown='ignore').fit(self.user_preferences[user_pref_fields])

        self.user_preferences_encoded = self.user_pref_encoder.transform(self.user_preferences[user_pref_fields])
        user_pref_target_length = 17
        if self.user_preferences_encoded.shape[1] < user_pref_target_length:
            original_encoded = self.user_preferences_encoded
            padded_encoded = np.zeros((1, user_pref_target_length))
            padded_encoded[:, :original_encoded.shape[1]] = original_encoded
            self.user_preferences_encoded = padded_encoded

        # 编码飞行记录或候选机票信息
        self.flight_info_encoder = OneHotEncoder(sparse=False, handle_unknown='ignore').fit(data_to_fit)
        if self.mode == 'train':
            self.flight_records_encoded = self.flight_info_encoder.transform(self.flight_records[flight_record_fields])
        elif self.mode == 'eval':
            self.candidate_tickets_encoded = self.flight_info_encoder.transform(self.candidate_tickets_df[flight_info_fields])

    def __getitem__(self, idx):
        if self.mode == 'train':
            user_pref = self.user_preferences_encoded[idx]
            flight_records = self.flight_records_encoded[idx]
        elif self.mode == 'eval':
            user_pref = self.user_preferences_encoded[0]
            flight_records = self.candidate_tickets_encoded[idx]# 使用正确的属性名

        if flight_records.shape[0] < 108:
            original_encoded = flight_records
            padded_encoded = np.zeros(108)
            padded_encoded[:original_encoded.shape[0]] = original_encoded
            flight_records = padded_encoded

        return torch.tensor(user_pref, dtype=torch.float32), torch.tensor(flight_records, dtype=torch.float32)

    def __len__(self):
            return len(self.user_preferences_encoded)