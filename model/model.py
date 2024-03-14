import torch
import torch.nn as nn
import torch.optim as optim

class FlightRecommendationModel(nn.Module):
    def __init__(self, input_dim, hidden_dim, output_dim):
        super(FlightRecommendationModel, self).__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_dim, output_dim)
        self.sigmoid = nn.Sigmoid()

    def forward(self, user_pref_features, flight_features):
        # Combine user preferences and flight features
        combined_features = torch.cat((user_pref_features, flight_features), dim=1)
        hidden = self.fc1(combined_features)
        hidden = self.relu(hidden)
        output = self.fc2(hidden)
        output = self.sigmoid(output)
        return output

def train(model, device, train_loader, optimizer, criterion):
    model.train()
    for batch_idx, (user_pref_features, flight_features, labels) in enumerate(train_loader):
        user_pref_features, flight_features, labels = user_pref_features.to(device), flight_features.to(device), labels.to(device)
        optimizer.zero_grad()
        output = model(user_pref_features, flight_features)
        loss = criterion(output, labels)
        loss.backward()
        optimizer.step()

def evaluate(model, device, test_loader):
    model.eval()
    total_loss = 0
    with torch.no_grad():
        for user_pref_features, flight_features, labels in test_loader:
            user_pref_features, flight_features, labels = user_pref_features.to(device), flight_features.to(device), labels.to(device)
            output = model(user_pref_features, flight_features)
            total_loss += criterion(output, labels).item()
    return total_loss / len(test_loader)