import pandas as pd
import numpy as np
# import akshare as ak
import streamlit as st
import torch
import torch.nn as nn
import torch.functional as F
import matplotlib.pyplot as plt
from torch.utils.data import TensorDataset, DataLoader

# pd.set_option('display.max_columns', None)
# pd.set_option('display.unicode.ambiguous_as_wide', True)
# pd.set_option('display.unicode.east_asian_width', True)
# pd.set_option('display.max_rows', None)
# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="600006", period="daily", start_date="20170301", end_date='20241102', adjust="qfq")
# print(stock_zh_a_hist_df)

device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using {device} device")


def generate_sine_wave_data(seq_length, num_samples):
    x = np.linspace(0, 22 * np.pi, num_samples + seq_length + 1)
    y = np.sin(x)

    data = []
    label = []
    for i in range(num_samples):
        data.append(y[i: i + seq_length])
        label.append(y[i+1: i+seq_length+1])
    return np.array(data), np.array(label)

data, label = generate_sine_wave_data(100, 1000)
plt.plot(data[0])
plt.plot(label[0])
plt.show()
data, label = data[:, :, np.newaxis], label[:, :, np.newaxis]

train_data_tensor = torch.from_numpy(data[:800]).float()
test_data_tensor = torch.from_numpy(data[800:]).float()
train_label_tensor = torch.from_numpy(label[:800]).float()
test_label_tensor = torch.from_numpy(label[800:]).float()

trainDataset = TensorDataset(train_data_tensor, train_label_tensor)
testDataset = TensorDataset(test_data_tensor, test_label_tensor)

trainDataloader = DataLoader(trainDataset, batch_size=100, shuffle=True, pin_memory=True)
testDataloader = DataLoader(testDataset, batch_size=100, pin_memory=True)







def train_loop(dataloader, model, loss_fn, optimizer, device):
    size = len(dataloader.dataset)
    model.train()
    for batch, (X, y) in enumerate(dataloader):
        X, y = X.to(device), y.to(device)

        outputs, _ = model(X)
        loss = loss_fn(outputs, y)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        # if batch % 10 == 0:
        #     loss, current = loss.item(), batch * len(X)
        #     print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


def test_loop(dataloader, model, loss_fn, device):
    model.eval()
    size = len(dataloader.dataset)
    num_batches = len(dataloader)
    test_loss = 0.0

    with torch.no_grad():
        for X, y in dataloader:
            X, y = X.to(device), y.to(device)
            output, _ = model(X)
            test_loss += loss_fn(output, y).item()

    test_loss /= num_batches
    print(f"Test Error: Avg loss: {test_loss:>8f}")


input_size = 1
hidden_size = 8
output_size = 1
learning_rate = 1e-4

loss_fn = nn.MSELoss()
model = Model(input_size, hidden_size, output_size).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)

epochs = 2000
for t in range(epochs):
    if t % 50 == 0:
        print(f"Epoch {t + 1}-------------------------------", end=" ")
        test_loop(testDataloader, model, loss_fn, device=device)
    train_loop(trainDataloader, model, loss_fn, optimizer, device=device)


print("Done!")

x = np.linspace(0, 8 * np.pi, 10000)
y = np.sin(x)

sampleData = torch.from_numpy(y[np.newaxis, :, np.newaxis]).float()
plt.plot(np.arange(0, 10000), sampleData[0, :, 0])

with torch.no_grad():
    output, _ = model(sampleData.to(device))
    output = torch.squeeze(output)
output = output.cpu().numpy()
plt.plot(np.arange(10000, 20000), output)
plt.show()



