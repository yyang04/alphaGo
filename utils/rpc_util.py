from concurrent.futures import ThreadPoolExecutor, as_completed
import random

import akshare as ak
import requests
import streamlit as st
import pandas as pd
from datetime import datetime
import time

from utils.constants import USER_AGENTS


def common_stock(x:str):
    prefixes = ['300', '688', '43', '83', '87']
    return not any(x.startswith(p) for p in prefixes)


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


def fetch_stock_history(symbol, *args):
    start_date = args[0] if args else '19700101'
    try:
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=symbol, period='daily', start_date=start_date, adjust="qfq")
    except Exception as _:
        print(f'[get_stock_history_error] name: {symbol}')
        return
    return stock_zh_a_hist_df


def fetch_stocks_history(*args):
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    stock_zh_a_spot_em_df.dropna(subset=['最新价'], inplace=True)
    filter_df = stock_zh_a_spot_em_df[stock_zh_a_spot_em_df['代码'].apply(common_stock)]

    args_list = [(getattr(row, '代码'), *args) for row in filter_df.itertuples()]
    result = multithreading_tasks(fetch_stock_history, args_list, num_workers=10, enable_progress_bar=True)

    if not result:
        return

    combined_df = pd.concat(result, ignore_index=True)
    # post_process
    combined_df = combined_df.rename(columns={
        '日期': 'dt',
        '股票代码': 'code',
        '开盘': 'open_price',
        '收盘': 'close_price',
        '最高': 'high_price',
        '最低': 'low_price',
        '成交量': 'volume',
        '成交额': 'amount',
        '换手率': 'turnover_ratio',
        '振幅': 'amplitude',
        '涨跌幅': 'change_percentage',
        '涨跌额': 'price_change'}
    )
    if not combined_df.empty:
        combined_df['dt'] = pd.to_datetime(combined_df['dt']).dt.date
        combined_df.set_index('dt', inplace=True)
    return combined_df


def fetch_global_index_history():
    df = ak.stock_dzjy_sctj()
    del (df['序号'])
    df = df.rename(columns={
        '交易日期': 'dt',
        '上证指数': 'sse_index',
        '上证指数涨跌幅': 'change_percentage_sse',
        '大宗交易成交总额': 'tbt',
        '溢价成交总额': 'ptt',
        '溢价成交总额占比': 'ppt',
        '折价成交总额': 'dtt',
        '折价成交总额占比': 'pdt'
    })
    df['dt'] = pd.to_datetime(df['dt']).dt.date
    df.set_index('dt', inplace=True)
    return df


def fetch_concept_history(symbol, *args):
    # period: [daily, weekly, monthly]
    start_date = args[0] if args else '19700101'
    end_date = datetime.today().strftime('%Y%m%d')
    try:
        stock_board_concept_hist_em_df = ak.stock_board_concept_hist_em(symbol=symbol, period='daily', start_date=start_date, end_date=end_date, adjust="qfq")
        stock_board_concept_hist_em_df.loc[:, '板块名称'] = symbol
    except Exception as _:
        print(f'[get_concept_history_error] name: {symbol}')
        return
    return stock_board_concept_hist_em_df


def fetch_concepts_history(*args):
    df = ak.stock_board_concept_name_em()
    args_list = [(getattr(row, '板块名称'), *args) for row in df.itertuples()]
    result = multithreading_tasks(fetch_concept_history, args_list, num_workers=10, enable_progress_bar=True)

    if not result:
        return

    combined_df = pd.concat(result, ignore_index=True)

    combined_df = combined_df.rename(columns={
        '日期': 'dt',
        '板块名称': 'name',
        '开盘': 'open_price',
        '收盘': 'close_price',
        '最高': 'high_price',
        '最低': 'low_price',
        '成交量': 'volume',
        '成交额': 'amount',
        '换手率': 'turnover_ratio',
        '振幅': 'amplitude',
        '涨跌幅': 'change_percentage',
        '涨跌额': 'price_change'}
    )

    combined_df['dt'] = pd.to_datetime(combined_df['dt']).dt.date
    combined_df.set_index('dt', inplace=True)
    return combined_df


def fetch_stock_concept(symbol):
    ret = ak.stock_board_concept_cons_em(symbol=symbol)
    filter_df = ret[['代码']].copy()
    filter_df.loc[:, '板块名称'] = symbol
    return filter_df


def fetch_stock_concepts(*args):
    df = ak.stock_board_concept_name_em()
    args_list = [(getattr(row, '板块名称'),) for row in df.itertuples()]
    result = multithreading_tasks(fetch_stock_concept, args_list, num_workers=10, enable_progress_bar=True)

    if not result:
        return

    combined_df = pd.concat(result, ignore_index=True)

    combined_df = combined_df.rename(columns={
        '板块名称': 'name',
        '代码': 'code'}
    )

    combined_df['dt'] = datetime.now().date()
    combined_df.set_index('dt', inplace=True)
    return combined_df


def fetch_industry_history(symbol, *args):
    # period: [日k, 周k, 月k]
    start_date = args[0] if args else '19700101'
    end_date = datetime.today().strftime('%Y%m%d')
    try:
        stock_board_concept_hist_em_df = ak.stock_board_industry_hist_em(symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq")
        stock_board_concept_hist_em_df.loc[:, '板块名称'] = symbol
    except Exception as _:
        print(f'[get_industry_history_error] name: {symbol}, start_date: {start_date}, end_date: {end_date}')
        return
    return stock_board_concept_hist_em_df


def fetch_industries_history(*args):
    df = ak.stock_board_industry_name_em()
    args_list = [(getattr(row, '板块名称'), *args) for row in df.itertuples()]
    result = multithreading_tasks(fetch_industry_history, args_list, num_workers=10, enable_progress_bar=True)

    if not result:
        return

    combined_df = pd.concat(result, ignore_index=True)

    combined_df = combined_df.rename(columns={
        '日期': 'dt',
        '板块名称': 'name',
        '开盘': 'open_price',
        '收盘': 'close_price',
        '最高': 'high_price',
        '最低': 'low_price',
        '成交量': 'volume',
        '成交额': 'amount',
        '换手率': 'turnover_ratio',
        '振幅': 'amplitude',
        '涨跌幅': 'change_percentage',
        '涨跌额': 'price_change'}
    )

    combined_df['dt'] = pd.to_datetime(combined_df['dt']).dt.date
    combined_df.set_index('dt', inplace=True)
    return combined_df


def fetch_stock_industry(symbol):
    ret = ak.stock_board_industry_cons_em(symbol=symbol)
    filter_df = ret[['代码']].copy()
    filter_df.loc[:, '板块名称'] = symbol
    return filter_df


def fetch_stocks_industries(*args):
    df = ak.stock_board_industry_name_em()
    args_list = [(getattr(row, '板块名称'), ) for row in df.itertuples()]
    result = multithreading_tasks(fetch_stock_industry, args_list, num_workers=10, enable_progress_bar=True)

    if not result:
        return

    combined_df = pd.concat(result, ignore_index=True)

    combined_df = combined_df.rename(columns={
        '板块名称': 'name',
        '代码': 'code'}
    )
    combined_df['dt'] = datetime.now().date()
    combined_df.set_index('dt', inplace=True)
    return combined_df



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
        time.sleep(random.random() / 12)
        headers = {
            'User-Agent': random.choice(USER_AGENTS)
        }
        api = f'https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData?symbol={symbol}&scale={scale}&ma=yes&datalen={datalen}'
        ret = requests.get(api, headers=headers)
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
    result = multithreading_tasks(get_recent_history, args_list, num_workers=1, enable_progress_bar=True)
    combined_df = pd.concat(result, ignore_index=True)
    combined_df['dt'] = pd.to_datetime(combined_df['dt'])
    combined_df['open_price'] = combined_df['open_price'].astype('double')
    combined_df['high_price'] = combined_df['high_price'].astype('double')
    combined_df['low_price'] = combined_df['low_price'].astype('double')
    combined_df['close_price'] = combined_df['close_price'].astype('double')
    combined_df['volume'] = combined_df['volume'].astype('float').astype('int64')
    return combined_df


