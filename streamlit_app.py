import streamlit as st
import duckdb
import tomllib

from utils.udf_util import *

st.set_page_config(layout="wide")

# 设置数据库连接
if 'db_connection' not in st.session_state:
    con = duckdb.connect()
    st.session_state.db_connection = con
    con.create_function("str_magic", str_magic)

# 设置配置
if 'conf' not in st.session_state:
    with open('conf/common.toml', 'rb') as f:
        st.session_state.conf = tomllib.load(f)

# 获取页面
pages = [
    st.Page("web/fetch_data.py", title='获取数据'),
    st.Page("web/data_visualization.py", title='数据探查'),
    st.Page("web/generate_feature.py", title='特征生产'),
    st.Page("web/generate_sample.py", title='样本生成'),
    st.Page("web/model_training.py", title='模型训练'),
    st.Page("web/model_infer.py", title='模型推理')
]

pg = st.navigation(pages)
pg.run()
