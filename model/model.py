import torch.nn as nn

# 修改模型定义
class FlightRecommendationModel(nn.Module):
    def __init__(self, user_pref_input_dim, flight_records_input_dim, hidden_dim):
        super(FlightRecommendationModel, self).__init__()
        self.user_pref_fc = nn.Linear(user_pref_input_dim, hidden_dim)
        self.flight_records_fc = nn.Linear(flight_records_input_dim, hidden_dim)
        self.cos = nn.CosineSimilarity(dim=1, eps=1e-6)

    def forward(self, user_pref, flight_records):
        user_pref_hidden = self.user_pref_fc(user_pref)
        flight_records_hidden = self.flight_records_fc(flight_records)
        similarity = self.cos(user_pref_hidden, flight_records_hidden)
        return similarity

# 修改训练函数
def train(model, device, train_loader, optimizer):
    model.train()
    total_loss = 0
    for batch_idx, (user_pref, flight_records) in enumerate(train_loader):
        user_pref, flight_records = user_pref.to(device), flight_records.to(device)
        optimizer.zero_grad()
        similarity = model(user_pref, flight_records)
        loss = 1.0 - similarity.mean()  # Maximize similarity
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    print(f"Average Loss: {total_loss / len(train_loader)}")


