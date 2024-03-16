import torch
import torch.optim as optim
from torch.utils.data import DataLoader
from dataset import FlightDataset
from model import FlightRecommendationModel

# 数据库配置
db_config = {
    'user': 'root',
    'password': '123456789@SCU',
    'host': '8.137.118.233',
    'port': '3306',
    'database': 'flight_db',
}
user_pref_input_dim = 17  # 用户偏好输入特征维度
flight_records_input_dim = 108  # 飞行记录输入特征维度
hidden_dim = 64  # 隐藏层维度
batch_size = 32
learning_rate = 0.001
epochs = 30

# 设备配置
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 加载数据集
train_dataset = FlightDataset(db_config=db_config, mode='train')
train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

# 初始化模型和优化器
model = FlightRecommendationModel(user_pref_input_dim, flight_records_input_dim, hidden_dim).to(device)
optimizer = optim.Adam(model.parameters(), lr=learning_rate)

def train(model, device, train_loader, optimizer, epochs):
    model.train()
    for epoch in range(epochs):
        total_loss = 0
        for user_pref, flight_records in train_loader:
            user_pref = user_pref.to(device).float()
            flight_records = flight_records.to(device).float()

            optimizer.zero_grad()
            similarity = model(user_pref, flight_records)
            # 损失函数是为了最大化相似度
            loss = 1.0 - similarity.mean()  # 意图最大化相似度
            loss.backward()
            optimizer.step()

            total_loss += loss.item()
        print(f'Epoch {epoch+1}, Average Loss: {total_loss / len(train_loader)}')

# 训练模型
train(model, device, train_loader, optimizer, epochs)

# 保存模型参数
torch.save(model.state_dict(), "flight_recommendation_model.pth")
