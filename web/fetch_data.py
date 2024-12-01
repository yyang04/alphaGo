import streamlit as st
import pandas as pd
from datetime import datetime
from utils.file_util import register_table, get_table_meta, append_dataframe_to_parquet, get_all_symbols
from utils.rpc_util import fetch_history, fetch_concept, fetch_all_symbols, fetch_recent_history
import os

st.title('获取数据')
con = st.session_state.db_connection
base_path = 'data/raw_data'
table_names = ['stock_symbol', 'stock_history', 'concept_history', 'concept',
               'stock_realtime_1', 'stock_realtime_5', 'stock_realtime_10', 'stock_realtime_30', 'stock_realtime_60',
               'stock_realtime_120']

register_table(con, base_path, table_names)


meta_table_list = []
for table_name in table_names:
    table_meta = get_table_meta(con, base_path, table_name)
    if table_meta is not None:
        meta_table_list.append(table_meta)
meta_table = pd.DataFrame(meta_table_list)
st.dataframe(meta_table)

tab_names = ['stock_symbol', 'stock_history', 'concept_history', 'concept', 'stock_realtime']
tab1, tab2, tab3, tab4, tab5 = st.tabs(tab_names)

with tab1:
    if st.button('更新数据', key=11):
        df = fetch_all_symbols()
        append_dataframe_to_parquet(df, base_path, table_names[0], duplicate_cols=['symbol'])


with tab2:
    start_date = st.date_input('开始日期', key=1)
    end_date = st.date_input('截止日期', datetime.now(), key=2)
    if st.button("更新数据", key=12):
        start_date = start_date.strftime('%Y%m%d')
        end_date = end_date.strftime('%Y%m%d')
        df = fetch_history(mode='stock', start_date=start_date, end_date=end_date)
        append_dataframe_to_parquet(df, base_path, table_names[1], duplicate_cols=['dt', 'code'])

with tab3:
    start_date = st.date_input('开始日期', key=3)
    end_date = st.date_input('截止日期', datetime.now(), key=4)
    if st.button("更新数据", key=13):
        start_date = start_date.strftime('%Y%m%d')
        end_date = end_date.strftime('%Y%m%d')
        df = fetch_history(mode='concept', start_date=start_date, end_date=end_date)
        append_dataframe_to_parquet(df, base_path, table_names[2], duplicate_cols=['日期', '板块'])

with tab4:
    if st.button("更新数据", key=14):
        df = fetch_concept()
        append_dataframe_to_parquet(df, base_path, table_names[3])

with tab5:
    if st.button("更新数据", key=15):
        ret = get_all_symbols(con)
        # scale_list = [1, 5, 10, 30, 60, 120]
        scale_list = [1]
        for scale in scale_list:
            df = fetch_recent_history(symbols=ret, scale=scale)
            append_dataframe_to_parquet(df, base_path, f'stock_realtime_{scale}', duplicate_cols=['dt', 'symbol'])

# history_stats = con.sql("""
#         select 日期, count(*) as cnt
#           from history
#          group by 1
#           order by 1
# """).execute()

# line = (
#     Line()
#     .add_xaxis(history_stats['日期'].tolist())
#     .add_yaxis('数量', history_stats['cnt'].tolist())
#     .set_global_opts(
#         title_opts=opts.TitleOpts(title="Top cloud providers 2018", subtitle="2017-2018 Revenue"),
#         toolbox_opts=opts.ToolboxOpts(),
#     )
# )
#
# st_pyecharts(line)
