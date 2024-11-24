import torch
import torch.nn as nn


class LSTMModel(nn.Module):
    def __init__(self, input_size, hidden_size, output_size, num_layers=1):
        super(LSTMModel, self).__init__()
        self.rnn = torch.nn.LSTM(input_size, hidden_size, num_layers=num_layers, batch_first=True)
        self.linear = torch.nn.Linear(hidden_size, output_size)

    def forward(self, input_seq, h0=None):
        output, hn = self.rnn(input_seq, h0)
        output = self.linear(output)
        return output, hn

    def predict(self, input_seq, seq_length):
        h0 = None
        outputs = []
        with torch.no_grad():
            for _ in range(seq_length):
                output, h0 = self.forward(input_seq, h0)
                outputs.append(output[:, -1, :])
                input_seq = output[:, -1:, :]
        return torch.squeeze(torch.stack(outputs, dim=1))

class Model(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(Model, self).__init__()
        self.rnn = nn.RNN(input_size, hidden_size, num_layers=1, batch_first=True)
        self.linear = torch.nn.Sequential(
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, output_size)
        )

    def forward(self, input_seq, h0=None):
        output, hn = self.rnn(input_seq, h0)
        output = self.linear(output)
        return output, hn

    def predict(self, input_seq, seq_length):
        h0 = None
        outputs = []
        with torch.no_grad():
            for _ in range(seq_length):
                output, h0 = self.forward(input_seq, h0)
                outputs.append(output[:, -1, :])
                input_seq = output[:, -1:, :]
        return torch.squeeze(torch.stack(outputs, dim=1))