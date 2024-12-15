import pandas as pd
from torch.utils.data import Dataset
from torch.utils.data import DataLoader
from sklearn.preprocessing import LabelEncoder

class MyDataset(Dataset):
    def __init__(self, df):
        self.df = df
        df['f_code'] = LabelEncoder().fit_transform(df['f_code'])

    def __len__(self):
        return len(self.df['f_code'])

    def __getitem__(self, index):
        return self.df.to_dict(orient='records')[index]


data = {
    'f_code': ['300200', '200901', '600812'],
    'f_max_price_over_1_month': [[1.2, 3.4, 2.2], [3.2, 3.5, 1.1], [0.2, 5.5, 7.2]],
    'l_increase_rate': [0.1, 0.2, 0.3]
}

dataset = MyDataset(pd.DataFrame(data))
dataloader = DataLoader(dataset, batch_size=2, shuffle=True)

for i in dataloader:
    print(i['f_max_price_over_1_month'])
    print(i['f_max_price_over_1_month'].dtype)
    break