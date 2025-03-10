from utils.date_util import get_days_delta
from utils.file_util import register_table, get_table_meta, append_dataframe_to_parquet, get_all_symbols
from utils.rpc_util import *

st.title('获取数据')

con = st.session_state.db_connection
conf = st.session_state.conf

base_path = 'data/raw_data'
table_names = [ent['name'] for ent in conf['tables']]
register_table(con, base_path, table_names)

for index, row in enumerate(conf['tables']):
    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
    table_meta = get_table_meta(con, base_path, row['name'])

    with col1:
        if index == 0:
            st.write('表名')
        st.write(row['desc'])

    with col2:
        if index == 0:
            st.write('数据量')
        st.write(table_meta.get('行数', ''))

    with col3:
        if index == 0:
            st.write('文件大小')
        st.write(table_meta.get('文件大小', ''))

    with col4:
        if index == 0:
            st.write('更新日期')
        st.write(table_meta.get('更新日期', ''))

    with col5:
        if index == 0:
            st.write('更新')

        if st.button("更新数据", key=row['desc']):
            args = []
            start_date = table_meta.get('更新日期', '')
            if start_date:
                start_date = get_days_delta(start_date, 1)
                args.append(start_date)
            df = eval(row['fetch_function'])(*args)
            duplicate_cols = row['key']
            if not df.empty:
                append_dataframe_to_parquet(df, base_path, row.tableName, duplicate_cols=duplicate_cols)