import streamlit as st
from pyecharts.charts import Line
from pyecharts import options as opts
from streamlit_echarts import st_pyecharts

from utils.visual_util import draw_charts

st.title("数据探查")

con = st.session_state.db_connection

code = st.text_input('股票代码')

if code:
    df = con.execute(f"""
    select dt, open_price, close_price, low_price, high_price
      from stock_history
    where code = {code}
      order by dt
    """).df()

    draw_charts(df)
