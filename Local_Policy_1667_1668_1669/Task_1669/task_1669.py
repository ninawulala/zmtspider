import csv
import io
import os
import pathlib
import random
import re
import shelve
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from pathlib import Path

# import sys  # 导入sys模块
# sys.setrecursionlimit(3000)  # 将默认的递归深度修改为3000


class Scrapy:

    def __init__(self):
        self.file_name_csv = 'task_1669_url.csv'
        self.filenames = ['id', 'cate_id', 'title', 'descript', 'file_wh', 'images', 'content', 'createdate', 'status',
                          'sort', 'type',
                          'attach', 'views', 'systemdate', 'url',
                          'attach_editor', 'laiyuan']

    def get_content(self):

        f = open('task_1669_url.csv', 'w', encoding='UTF-8-sig', newline='')
        writer = csv.DictWriter(f, fieldnames=self.filenames)
        writer.writeheader()
        # with shelve.open('data') as db:
            # datas = db['data']

        # 获取省直部门url参数
        params = {
            'deptCode': 'HN',
        }
        response = requests.get('http://wjbb.sft.henan.gov.cn/viewRegulatory.do', params=params,verify=False)
        html = BeautifulSoup(response.text, 'lxml')
        slideLists = html.select('body > div > div.ejLeft > div:nth-child(9) > a.slideList')
        for slideList in slideLists:
            if slideList.get_text().startswith('河南'):
                source = slideList.get_text()
            else:
                source = '河南' + slideList.get_text()
            slide_url = 'http://wjbb.sft.henan.gov.cn' + slideList.attrs['href'][2:]
            # 获取内容等详细信息
            response = requests.get(slide_url)
            html = BeautifulSoup(response.text, "lxml")
            char = html.select('.page')[0].get_text()
            try:
                number = re.findall('页 共(.*?)页', char)[0]
            except:
                continue
            # 分页拿数据
            for i in range(int(number)):
                params = {
                    'offset': str(15 * i),
                }
                response = requests.get(slide_url, params=params, verify=False)
                html = BeautifulSoup(response.text, "lxml")
                pdf_urls = html.select('.serListCon > a')
                for item in pdf_urls:
                    href = item.attrs['href']
                    pdf_url = 'http://wjbb.sft.henan.gov.cn' + href
                    title = item.get_text().strip()
                    date = item.next_sibling.next_sibling.get_text().strip('[]')
                    file_type = pdf_url.split('.')[-1]
                    try:
                        href_s = href.split('HN')[1]
                    except:
                        href_s = href.split('upload')[1]
                    if 'HNM' in href:
                        href_s = href.split('HNM')[1]
                    try:
                        attach = file_type + href_s
                    except:
                        print(href)
                        datee = date.split('-')
                        attach = file_type + '/' + '/'.join(datee) + '/' + href.split('/')[-1]
                        print(title, date, source, pdf_url, attach)
                        time.sleep(4)
                    try:
                        p1 = re.compile(r'[(](.*?)[)]', re.S)  # 最小匹配
                        file_wh = re.findall(p1, title)[0]
                    except:
                        print('*'*10)
                        print(slide_url, title)
                        continue
                    date_list = re.findall(r'\d+', date)
                    date_list_len = len(date_list)
                    if date_list_len == 1:
                        dt = datetime.strptime(date_list[0], '%Y')
                    elif date_list_len == 2:
                        dt = datetime.strptime(date_list[0] + '-' + date_list[1], '%Y-%m')
                    elif date_list_len == 3:
                        dt = datetime.strptime(date_list[0] + '-' + date_list[1] + '-' + date_list[2], '%Y-%m-%d')
                    else:
                        dt = ''
                    try:
                        utc_time = datetime.strptime("1970-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
                        met_time = dt - utc_time
                        createdate = str(met_time.days * 24 * 3600 + met_time.seconds)
                    except:
                        print('*'*10)
                        print(slide_url, date)
                        continue
                    print(title, date, source, pdf_url)
                    rows = [
                        {
                        'id': '',
                        'cate_id': '69',
                        'title': title,
                        'descript': '',
                        'file_wh': file_wh,
                        'images': '',
                        'content': '',
                        'createdate': createdate,  # 创建时间，取爬取文章中的创建时间，转为时间戳格式
                        'status': 1,
                        'sort': 50,
                        'type': 2,
                        'attach': attach,
                        'views': '',
                        'systemdate': '',
                        'url': pdf_url,
                        'attach_editor': '',
                        'laiyuan': source,
                    },
                    ]

                    writer.writerows(rows)

                    # time.sleep(random.uniform(0.5, 0.6))
        f.close()

    def get_pdf(self):
        f = open('task_1669_url.csv', 'r', encoding='utf-8-sig')
        reader = csv.DictReader(f)
        for k, line in enumerate(reader):
            print(k)
            title = line['title']
            if title:
                for i in title:
                    if i in '<>/|:"*?':
                        title = title.replace(i, '')
                pdf_url = line['url']
                attach = line['attach']
                print(title, pdf_url)
                reaponse = requests.get(pdf_url)
                bytes_io = io.BytesIO(reaponse.content)
                dir = fr'./task_1669/{attach}'
                ll = '.' + dir.strip(dir.split("/")[-1])
                pathlib.Path(ll).mkdir(parents=True, exist_ok=True)
                with open(dir, mode='wb') as r:
                    r.write(bytes_io.getvalue())

        f.close()

    def to_db(self):
        import pandas as pd 
        from sqlalchemy import create_engine
        db_info = {
            'user': 'root',
            'password': '123456',
            'host': 'localhost',
            'port': '3306',
            'database': 'kfzmt',
            # 'table': 'article_1669'
            'table': 'article'
        }
        engine = create_engine(f'mysql+pymysql://{db_info["user"]}:{db_info["password"]}@{db_info["host"]}:{db_info["port"]}/{db_info["database"]}')

        # df = pd.read_csv(f'./{self.file_name_csv}')
        df = pd.read_csv(f'./{self.file_name_csv}')
        df['url'] = ''
        df.to_sql(db_info["table"], con=engine, index=False, if_exists='append')
        print(f'{self.file_name_csv}导入数据库{db_info["database"]}下的表{db_info["table"]}成功{len(df)}条。')

    def drop_url(self):
        import pandas as pd
        df = pd.read_csv(f'./task_1669_url.csv')
        df['url'] = ''
        df.to_csv('task_1669.csv', index=False)

if __name__ == '__main__':
    scrapy = Scrapy()
    # scrapy.get_content()
    # scrapy.get_pdf()
    scrapy.to_db()
    # scrapy.drop_url()