from concurrent.futures import ThreadPoolExecutor, as_completed
import akshare as ak
import requests
import streamlit as st
import pandas as pd
from datetime import datetime
import time


def multithreading_tasks(func, args_list, num_workers=10, enable_progress_bar=False):

    if enable_progress_bar:
        progress_bar = st.progress(0)
        start_time = time.time()
        task_num = len(args_list)

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(func, *args) for args in args_list]
        results = []
        for i, future in enumerate(as_completed(futures)):
            results.append(future.result())

            # calculate remaining time
            if enable_progress_bar:
                done = len(list(filter(lambda f: f.done(), futures)))
                elapsed_time = time.time() - start_time
                remaining_time = (task_num - (i + 1)) * elapsed_time // (i + 1)
                progress_bar.progress(done / task_num, text=f'progress {done}/{task_num}, remaining_time {remaining_time} seconds')
        return results


def fetch_history(mode, start_date, end_date):
    def get_history(func, name, start_date, end_date, mode):
        try:
            stock_zh_a_hist_df = func(symbol=name, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if mode == 'concept':
                stock_zh_a_hist_df.loc[:, '板块名称'] = name
        except Exception as _:
            return
        return stock_zh_a_hist_df

    if mode == 'stock':
        stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
        func = ak.stock_zh_a_hist
        attr = '代码'
    elif mode == 'concept':
        stock_zh_a_spot_em_df = ak.stock_board_concept_name_em()
        func = ak.stock_board_concept_hist_em
        attr = '板块名称'
    else:
        return

    stock_zh_a_spot_em_df.dropna(subset=['最新价'], inplace=True)
    args_list = [(func, getattr(row, attr), start_date, end_date, mode) for row in stock_zh_a_spot_em_df.itertuples()]
    result = multithreading_tasks(get_history, args_list, num_workers=10, enable_progress_bar=True)

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
                '涨跌幅': 'price_change_percentage',
                '涨跌额': 'price_change_amount'
            }
        )

    if mode == 'concept':
        combined_df['日期'] = pd.to_datetime(combined_df['日期']).dt.date

    return combined_df


def fetch_history_daily():
    pass


def fetch_all_symbols():
    ret = []
    for i in range(1, 100):
        url = f'http://money.finance.sina.com.cn/d/api/openapi_proxy.php/?__s=[["hq","hs_a","",0,{i},500]]'
        r = requests.get(url)
        if len(r.json()[0]['items']) == 0:
            break
        for item in r.json()[0]['items']:
            symbol = item[0]
            code = item[1]
            name = item[2]
            quote = {
                'dt': datetime.now().date(),
                'symbol': symbol,
                'code': code,
                'name': name
            }
            ret.append(quote)
    return pd.DataFrame(ret)


def fetch_recent_history(symbols, scale):
    # scale 1/5/10/30/60/120
    # scale 4days 20days 40days 120days 240days 480days

    def get_recent_history(symbol, scale, datalen=1024):
        api = f'https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData?symbol={symbol}&scale={scale}&ma=yes&datalen={datalen}'
        ret = requests.get(api)
        df = pd.DataFrame(ret.json()).rename(columns={
            'day': 'dt',
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price',
        })
        df['symbol'] = symbol
        return df

    args_list = [(symbol, scale) for symbol in symbols]
    result = multithreading_tasks(get_recent_history, args_list, num_workers=10, enable_progress_bar=True)
    combined_df = pd.concat(result, ignore_index=True)
    combined_df['dt'] = pd.to_datetime(combined_df['dt'])
    combined_df['open_price'] = combined_df['open_price'].astype('double')
    combined_df['high_price'] = combined_df['high_price'].astype('double')
    combined_df['low_price'] = combined_df['low_price'].astype('double')
    combined_df['close_price'] = combined_df['close_price'].astype('double')
    combined_df['volume'] = combined_df['volume'].astype('float').astype('int64')
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
