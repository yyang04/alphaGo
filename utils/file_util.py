import os
import pandas as pd


def register_table(con, base_path, table_names):
    for table_name in table_names:
        path = f'{base_path}/{table_name}.parquet'
        if os.path.exists(path):
            con.sql(f"""create or replace table {table_name} as select * from '{path}'""")


def get_file_size(base_path, table_name):
    path = f'{base_path}/{table_name}.parquet'
    if not os.path.exists(path):
        return
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
    file_size_bytes = os.stat(path).st_size
    index = 0
    while file_size_bytes >= 1024 and index < len(units) - 1:
        file_size_bytes /= 1024
        index += 1
    size = f"{file_size_bytes:.2f} {units[index]}"
    return size


def get_table_meta(con, base_path, table_name):
    path = f'{base_path}/{table_name}.parquet'
    if not os.path.exists(path):
        return
    update_date_sql = f'select max(dt) from {table_name}'
    num_rows_sql = f'select count(*) as cnt from {table_name}'

    update_date = con.execute(update_date_sql).df().iloc[0, 0].strftime("%Y%m%d")
    num_rows = con.execute(num_rows_sql).df().iloc[0, 0]
    size = get_file_size(base_path, table_name)
    return {'数据表': table_name, '更新日期': update_date, '行数': num_rows, '文件大小': size}


def append_dataframe_to_parquet(df, base_path, table_name, duplicate_cols=None):
    path = f'{base_path}/{table_name}.parquet'
    if not os.path.exists(path):
        df.to_parquet(path, index=False)
    else:
        old_df = pd.read_parquet(path)
        new_df = pd.concat([old_df, df])
        if duplicate_cols:
            new_df.drop_duplicates(subset=duplicate_cols, keep='first', inplace=True)
        new_df.to_parquet(path, index=False)
        print("Done")


