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

# 设定训练参数
batch_size = 32
learning_rate = 0.001
epochs = 10
input_dim = 358
hidden_dim = 64
output_dim = 1

# 设备配置
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 加载数据集
train_dataset = FlightDataset(db_config=db_config, mode='train')
train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

# 初始化模型和优化器
model = FlightRecommendationModel(input_dim, hidden_dim, output_dim).to(device)
optimizer = optim.Adam(model.parameters(), lr=learning_rate)
criterion = torch.nn.BCELoss()

def train(model, device, train_loader, optimizer, criterion):
    model.train()
    for batch_idx, (user_pref, flight_records) in enumerate(train_loader):
        # 假设 user_pref 和 flight_records 都是numpy数组，需要转换为torch张量
        user_pref = torch.tensor(user_pref).to(device).float()
        flight_records = torch.tensor(flight_records).to(device).float()

        # 确保flight_records是二维的，如果是三维数据，使用view方法展平
        if flight_records.dim() == 3:
            flight_records = flight_records.view(flight_records.size(0), -1)

        # 确保user_pref和flight_records维度相同
        if user_pref.size(1) != flight_records.size(1):
            print("Feature dimension mismatch: user_pref and flight_records should have the same number of features.")
            return

        optimizer.zero_grad()
        output = model(user_pref, flight_records)
        labels = torch.ones_like(output)  # 假设所有样本都是正样本
        loss = criterion(output, labels)
        loss.backward()
        optimizer.step()

# 训练循环
for epoch in range(epochs):
    model.train()
    train(model, device, train_loader, optimizer, criterion)
    total_loss = 0
    for user_pref, flight_records in train_loader:
        user_pref = user_pref.to(device)
        flight_records = flight_records.squeeze(1).to(device)  # 使用squeeze调整形状
        optimizer.zero_grad()
        output = model(user_pref, flight_records)
        loss = criterion(output, torch.ones_like(output))  # 假设所有样本都是正样本
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    print(f"Epoch {epoch+1}, Loss: {total_loss/len(train_loader)}")
