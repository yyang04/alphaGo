import os
import pandas as pd

class BaseFeature:
    def __init__(self, src_tables, dst_table, dims):
        self.src_tables = src_tables
        self.dst_table = dst_table
        self.dims = dims

        self.src_df = []
        self.base_path = 'data/feature'
        for src_table in src_tables:
            self.src_df.append(pd.read_parquet(f'{self.base_path}/{src_table}.parquet'))


    def update(self):
        pass

    def save_parquet(self):
        pass

    def execute(self):
        pass