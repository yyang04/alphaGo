from concurrent.futures import ThreadPoolExecutor, as_completed
import akshare as ak
import streamlit as st
import pandas as pd
from datetime import datetime
import time


def fetch_history(mode, start_date, end_date):
    def get_history(func, name, start_date, end_date, mode):
        try:
            stock_zh_a_hist_df = func(symbol=name, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if mode == 'concept':
                stock_zh_a_hist_df.loc[:, '板块名称'] = name
        except Exception as _:
            return
        return stock_zh_a_hist_df

    progress_bar = st.progress(0)
    if mode == 'stock':
        stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    elif mode == 'concept':
        stock_zh_a_spot_em_df = ak.stock_board_concept_name_em()
    else:
        return

    stock_zh_a_spot_em_df.dropna(subset=['最新价'], inplace=True)
    task_num = stock_zh_a_spot_em_df.shape[0]
    result = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=10) as executor:
        if mode == 'stock':
            func = ak.stock_zh_a_hist
            attr = '代码'
        else:
            func = ak.stock_board_concept_hist_em
            attr = '板块名称'
        futures = [executor.submit(get_history, func, getattr(row, attr), start_date, end_date, mode) for row in stock_zh_a_spot_em_df.itertuples()]

        for i, future in enumerate(as_completed(futures)):
            df = future.result()
            result.append(df)
            done = len(list(filter(lambda f: f.done(), futures)))
            elapsed_time = time.time() - start_time
            remaining_time = (task_num - (i + 1)) * elapsed_time // (i + 1)
            progress_bar.progress(done / task_num, text=f'Update {mode} data in progress, please wait: progress {done}/{task_num}, remaining_time {remaining_time} seconds')

    if not result:
        return

    combined_df = pd.concat(result, ignore_index=True)
    if mode == 'stock':
        combined_df = combined_df.rename(columns={
                '日期': 'dt',
                '股票代码': 'code',
                '开盘': 'open_price',
                '收盘': 'close_price',
                '最高': 'high_price',
                '最低': 'low_price',
                '成交量': 'volume',
                '成交额': 'turnover',
                '换手率': 'turnover_rate',
                '振幅': 'amplitude',
                '涨跌幅': 'pcp'
            }
        )
    if mode == 'concept':
        combined_df['日期'] = pd.to_datetime(combined_df['日期']).dt.date

    return combined_df


def fetch_concept():
    def get_concept(symbol):
        ret = ak.stock_board_concept_cons_em(symbol=symbol)
        ret.loc[:, '板块名称'] = symbol
        return ret

    progress_bar = st.progress(0)

    stock_board_concept_name_em_df = ak.stock_board_concept_name_em()
    task_num = stock_board_concept_name_em_df.shape[0]
    result = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_concept, getattr(row, '板块名称')) for row in stock_board_concept_name_em_df.itertuples()]

        for i, future in enumerate(as_completed(futures)):
            df = future.result()
            result.append(df)
            done = len(list(filter(lambda f: f.done(), futures)))
            elapsed_time = time.time() - start_time
            remaining_time = (task_num - (i + 1)) * elapsed_time // (i + 1)
            progress_bar.progress(done / task_num, text=f'Update concept daily data in progress, please wait: progress {done}/{task_num}, remaining_time {remaining_time} seconds')

    if not result:
        return

    combined_df = pd.concat(result, ignore_index=True)
    combined_df['日期'] = datetime.now().date()
    return combined_df
