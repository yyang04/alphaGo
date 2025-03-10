from features.base_feature import BaseFeature


class BaseStockFeature(BaseFeature):
    def __init__(self):
        super().__init__()

    def update(self):
        df = self.src_df[0]
        df['max_price_overall'] = df.group


