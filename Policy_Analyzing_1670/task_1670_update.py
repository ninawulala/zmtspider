# 通过两次数量的差集判断是否更新
import os

import pandas as pd

file_name_csv = 'Task_1670_csv'  # _update

csv_list = os.listdir(file_name_csv)
csv_list_update = os.listdir(file_name_csv+'_update')

def get_update():

    for cl in csv_list:
        for cl_p in csv_list_update:
            if cl.split('_')[2].split('.')[0] == cl_p.split('_')[2]:
                df = pd.read_csv(f'./{file_name_csv}/{cl}')
                df_up = pd.read_csv(f'./{file_name_csv}_update/{cl_p}')
                diff = pd.concat(objs=[df_up, df, df]).drop_duplicates(subset=['title'], keep=False)
                len_diff = len(diff)
                print(f'此次有{len_diff}条更新')
                diff.to_csv(f'./{file_name_csv}_update/{cl_p}', index=False)
                up = pd.concat([diff, df])
                up.to_csv(f'./{file_name_csv}/{cl}', index=False)

    print(type(csv_list))
# get_update()

def to_db():
    from sqlalchemy import create_engine
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/kfzmt')
    db_info = {
        'user': 'root',
        'password': 'root',
        'host': 'localhost',
        'port': '3306',
        'database': 'kfzmt',
        # 'table': 'article_1670'
        'table': 'article'
    }
    count = 0
    for ls in csv_list:
        df = pd.read_csv(f'./{file_name_csv}/{ls}')
        df['url'] = ''
        df.to_sql(db_info["table"], con=engine, index=False, if_exists='append')
        print(f'{ls}导入数据库{db_info["database"]}下的表{db_info["table"]}成功{len(df)}条。')
        count += len(df)
    print(f'此次共导入{count}条')

    # for ls in csv_list_update:
    #     df = pd.read_csv(f'./{file_name_csv}_update/{ls}')
    #     df['url'] = ''
    #     df.to_sql(db_info["table"], con=engine, index=False, if_exists='append')
    #     print(f'{ls}导入数据库{db_info["database"]}下的表{db_info["table"]}成功{len(df)}条。')
    #     count += len(df)
    # print(f'此次共导入{count}条')

to_db()

