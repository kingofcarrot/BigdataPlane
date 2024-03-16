from dataset import FlightDataset
from model import FlightRecommendationModel
import pandas as pd
import torch
from flask import Flask, request, jsonify
import logging
from logging.handlers import RotatingFileHandler
app = Flask(__name__)

# 假定数据库配置和模型路径
db_config = {
    'user': 'root',
    'password': '123456789@SCU',
    'host': '8.137.118.233',
    'port': '3306',
    'database': 'flight_db',
}

model_path = '/media/scdx/D/wp/Yelp/Bigdata/model/flight_recommendation_model.pth'

# 模型的输入维度可能需要根据实际情况进行调整
user_pref_input_dim = 17  # 用户偏好输入特征维度
flight_records_input_dim = 108  # 飞行记录输入特征维度
hidden_dim = 64  # 隐藏层维度

# 加载模型
model = FlightRecommendationModel(user_pref_input_dim, flight_records_input_dim, hidden_dim)
checkpoint = torch.load(model_path, map_location=torch.device('cpu'))
model.load_state_dict(checkpoint)
model.eval()


@app.route('/evaluate', methods=['POST'])
def evaluate():
    # 获取请求数据
    request_data = request.json

    # 从请求数据中提取用户ID和候选机票
    user_id = request_data.get('user_id')
    candidate_tickets = request_data.get('candidate_tickets')
    candidate_tickets_df = pd.DataFrame(candidate_tickets)

    # 实例化数据集
    dataset = FlightDataset(db_config=db_config, mode='eval', user_id=user_id, candidate_tickets_df=candidate_tickets_df)

    # 初始化一个空列表来存储排序后的候选机票
    sorted_tickets = []

    # 使用模型进行评估
    for i in range(len(dataset) + 1):
        # 提取单个候选机票和用户偏好
        user_pref, flight_option = dataset[i]

        # 使用模型进行评估
        with torch.no_grad():
            score = model(user_pref.unsqueeze(0), flight_option.unsqueeze(0)).item()

        # 将候选机票及其得分添加到排序列表中
        sorted_tickets.append((candidate_tickets[i], score))

    # 根据得分对候选机票进行排序
    sorted_tickets = sorted(sorted_tickets, key=lambda x: x[1], reverse=True)

    # 提取排序后的候选机票
    sorted_tickets = [ticket[0] for ticket in sorted_tickets]

    # 返回排序后的候选机票
    return jsonify(sorted_tickets)

@app.errorhandler(403)
def forbidden(error):
    return jsonify({"message": "Access forbidden"}), 403


if __name__ == '__main__':
    # handler = RotatingFileHandler('flask_app.log', maxBytes=10000, backupCount=3)
    # handler.setLevel(logging.INFO)
    # app.logger.addHandler(handler)
    app.run(debug=True, host='0.0.0.0', port=5000)
