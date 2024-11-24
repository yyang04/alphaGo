import streamlit as st
import ibis
import duckdb

from utils.udf_util import *

if 'db_connection' not in st.session_state:
    con = duckdb.connect()
    st.session_state.db_connection = con

    con.create_function("str_magic", str_magic)


pages = [
    st.Page("web/fetch_data.py", title='获取数据'),
    st.Page("web/data_visualization.py", title='数据可视化'),
    st.Page("web/generate_feature.py", title='特征生产'),
    st.Page("web/generate_sample.py", title='样本生成'),
    st.Page("web/model_training.py", title='模型训练'),
    st.Page("web/model_infer.py", title='模型推理')
]




pg = st.navigation(pages)
pg.run()
