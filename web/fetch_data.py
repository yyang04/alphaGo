import streamlit as st
import pandas as pd
from datetime import datetime
from utils.file_util import register_table, get_table_meta, append_dataframe_to_parquet
from utils.rpc_util import fetch_history, fetch_concept
import os

st.title('获取数据')
con = st.session_state.db_connection
base_path = 'data/raw_data'
table_names = ['stock_history', 'concept_history', 'concept']
register_table(con, base_path, table_names)


meta_table_list = []
for table_name in table_names:
    table_meta = get_table_meta(con, base_path, table_name)
    if table_meta is not None:
        meta_table_list.append(table_meta)
meta_table = pd.DataFrame(meta_table_list)
st.dataframe(meta_table)

tab1, tab2, tab3 = st.tabs(table_names)

with tab1:
    start_date = st.date_input('开始日期', key=1)
    end_date = st.date_input('截止日期', datetime.now(), key=2)
    if st.button("更新数据", key=12):
        start_date = start_date.strftime('%Y%m%d')
        end_date = end_date.strftime('%Y%m%d')
        df = fetch_history(mode='stock', start_date=start_date, end_date=end_date)
        append_dataframe_to_parquet(df, base_path, table_names[0], duplicate_cols=['dt', 'code'])

with tab2:
    start_date = st.date_input('开始日期', key=3)
    end_date = st.date_input('截止日期', datetime.now(), key=4)
    if st.button("更新数据", key=13):
        start_date = start_date.strftime('%Y%m%d')
        end_date = end_date.strftime('%Y%m%d')
        df = fetch_history(mode='concept', start_date=start_date, end_date=end_date)
        append_dataframe_to_parquet(df, base_path, table_names[1], duplicate_cols=['日期', '板块'])

with tab3:
    if st.button("更新数据", key=14):
        df = fetch_concept()
        append_dataframe_to_parquet(df, base_path, table_names[2])

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
