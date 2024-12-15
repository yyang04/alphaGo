import pandas as pd


def common_stock(x:str):
    prefixes = ['300', '688', '43', '83', '87']
    return not any(x.startswith(p) for p in prefixes)


class BaseFeature:
    def __init__(self):
        self.dims = []

    def process(self):
        df = pd.read_parquet('stock_history.parquet')
        df['dt'] = pd.to_datetime(df['dt'])
        df.set_index('dt', inplace=True)
        filter_df = df[df['code'].apply(common_stock)]

        # 发行量
        filter_df['insurance_volume'] = filter_df['volume'] / filter_df['turnover_ratio']

        #
