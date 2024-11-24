import streamlit as st
from pyecharts.charts import Line
from pyecharts import options as opts
from streamlit_echarts import st_pyecharts

st.title("股票数据可视化")

con = st.session_state.db_connection

code = st.text_input('股票代码')

if code:
    df = con.execute(f"""
    select dt, close_price
      from stock_history
    where code = {code}
      and dt between date_add(current_date, -40) and current_date
      order by dt
    """).df()

    line = Line()
    line.add_xaxis(df['dt'].tolist())
    line.add_yaxis(df['close_price'].tolist(), df['close_price'].tolist())
    line.set_global_opts(title_opts=opts.TitleOpts(title='个股数据查询'))
    st_pyecharts(line)