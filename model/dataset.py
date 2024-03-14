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
    def __init__(self, db_config, mode='train'):
        self.db_config = db_config
        self.mode = mode
        self.encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')  # 初始化 OneHotEncoder
        self.load_data()
        self.preprocess_data()
        self.encode_data()

    def load_data(self):
        # 使用 SQLAlchemy 创建数据库连接
        database_uri = "mysql+mysqlconnector://root:123456789%40SCU@8.137.118.233:3306/flight_db"
        engine = create_engine(database_uri)

        # 读取用户偏好数据
        self.user_preferences = pd.read_sql("SELECT * FROM user_preference", engine)
        # 读取用户飞行记录数据
        self.flight_records = pd.read_sql("SELECT * FROM user_flight_records", engine)

        if self.mode == 'eval':
            # 如果处于评估模式，读取航班信息数据
            self.flight_info = pd.read_sql("SELECT * FROM flight_info", engine)
        else:
            # 如果不是评估模式，初始化一个空的DataFrame
            self.flight_info = pd.DataFrame()

        engine.dispose()  # 关闭数据库连接

    def preprocess_data(self):
        # 确保所有的文本字段被转换为字符串类型
        # 注意：需要在每个数据集上分别应用
        def convert_to_str(df, categorical_cols):
            for col in categorical_cols:
                df[col] = df[col].astype(str)

        user_pref_categorical_cols = self.user_preferences.select_dtypes(include=['object', 'bool']).columns
        convert_to_str(self.user_preferences, user_pref_categorical_cols)

        if self.mode == 'train':
            flight_records_categorical_cols = self.flight_records.select_dtypes(include=['object', 'bool']).columns
            convert_to_str(self.flight_records, flight_records_categorical_cols)
        elif self.mode == 'eval':
            flight_info_categorical_cols = self.flight_info.select_dtypes(include=['object', 'bool']).columns
            convert_to_str(self.flight_info, flight_info_categorical_cols)

    def encode_data(self):
        # 生成所有可能特征列的并集
        all_columns = set(self.user_preferences.columns) | set(self.flight_records.columns) | set(
            self.flight_info.columns)

        # 对于每个DataFrame，添加缺失的列，设置为""表示空值
        for df in [self.user_preferences, self.flight_records, self.flight_info]:
            for col in all_columns:
                if col not in df.columns:
                    df[col] = ""

        # 更新categorical_cols，因为已经添加了缺失的列
        categorical_cols = list(all_columns.difference(['preference_id', 'user_id', 'record_id', 'flight_id']))

        # 将所有文本字段转换为字符串类型，确保独热编码可以正确应用
        for df in [self.user_preferences, self.flight_records, self.flight_info]:
            df[categorical_cols] = df[categorical_cols].astype(str)

        # 初始化OneHotEncoder，并使用所有数据的并集进行fit，这确保了编码器能够识别所有可能的类别
        self.encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')
        all_data = pd.concat([self.user_preferences[categorical_cols],
                              self.flight_records[categorical_cols],
                              self.flight_info[categorical_cols]], ignore_index=True)
        self.encoder.fit(all_data)

        # 分别对每个DataFrame的相关列进行transform，得到独热编码的结果
        self.user_preferences_encoded = self.encoder.transform(self.user_preferences[categorical_cols])
        self.flight_records_encoded = self.encoder.transform(self.flight_records[categorical_cols])

        # 检查flight_info是否为空，这是为了处理在非评估模式下可能不存在flight_info的情况
        if not self.flight_info.empty:
            self.flight_info_encoded = self.encoder.transform(self.flight_info[categorical_cols])
        else:
            # 为flight_info_encoded提供一个默认值，例如一个空数组，确保后续操作不会因为此变量是None而出错
            self.flight_info_encoded = np.array([]).reshape(0, len(categorical_cols))  # 确保空数组的形状与编码器输出一致

    def __getitem__(self, idx):
        user_pref = self.user_preferences_encoded[idx]
        user_id = self.user_preferences.iloc[idx]['user_id']

        if self.mode == 'train':
            matching_records_indices = self.flight_records['user_id'] == user_id
            flight_records = self.flight_records_encoded[matching_records_indices]
        elif self.mode == 'eval':
            flight_records = self.flight_info_encoded

        # 将numpy数组转换为PyTorch tensor
        user_pref = torch.tensor(user_pref, dtype=torch.float32)
        flight_records = torch.tensor(flight_records, dtype=torch.float32)

        return user_pref, flight_records

    def __len__(self):
            return len(self.user_preferences_encoded)