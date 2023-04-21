import csv
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from datetime import datetime
from typing import List

import requests
from bs4 import BeautifulSoup
from retrying import retry


class Task:
    def __init__(self):
        self.file_name_csv = 'Task_1667_8_csv'
        # self.file_name_csv = 'Task_1667_8_csv' + '_update'
        self.url = 'https://www.henan.gov.cn/zwgk/fgwj'
        self.retry_times = 1  # 重试次数
        self.fieldnames = ['id', 'cate_id', 'title', 'descript', 'file_wh', 'images', 'content', 'createdate', 'status',
                           'sort', 'type',
                           'attach', 'views', 'systemdate', 'url',
                           'attach_editor', 'laiyuan']

        self.szfl = {
            'code': 'szfl',
            'id':'45000000010115416542055691',
            'cate_id': '69',
            'name': 'task_1667_szfl',
            # 'name': 'task_1667_szfl' + '_update',
            'laiyuan': '河南省人民政府'
        }
        self.yz = {
            'code': 'yz',
            'id':'45000000010115416542079063',
            'cate_id': '69',
            'name': 'task_1668_yz',
            # 'name': 'task_1668_yz' + '_update',
            'laiyuan': '河南省人民政府'
        }
        self.yzb = {
            'code': 'yzb',
            'id':'45000000010115416542055799',
            'cate_id': '69',
            'name': 'task_1668_yzb',
            # 'name': 'task_1668_yzb' + '_update',
            'laiyuan': '河南省人民政府办公厅'
        }
        self.tasks_list = [self.szfl, self.yz, self.yzb]

    def config_init(self, task):
        task_code = task['id']
        url = f'https://searchapi.henan.gov.cn/open/api/external?keywords=&siteId=4500000001&pageNumber=1&pageSize=15&channelMarkId={task_code}'
        total_page = self.get_total_page(url)
        task_name = task['name']

        cp = ConfigParser()
        cp.add_section('db')  # 创建section db
        cp.set('db', 'total_page', str(total_page))  # 添加 total_page 到 db下
        try:
            os.listdir('./config/')
        except:
            os.makedirs('./config/')

        cp.write(open(f"./config/{task_name}_config.ini", 'w'))  # 写入文件并保存
        # 开始爬取数据
        self.get_csv_data(task, total_page)

    def get_csv_data(self, task, page_index):

        try:
            fw = open(f'./{self.file_name_csv}/{task["name"]}.csv', 'w', encoding='UTF8', newline='')
        except:
            os.makedirs(f'./{self.file_name_csv}/')
            fw = open(f'./{self.file_name_csv}/{task["name"]}.csv', 'w', encoding='UTF8', newline='')

        writer = csv.DictWriter(fw, fieldnames=self.fieldnames)
        writer.writeheader()

        task_code = task['id']
        for i in range(int(page_index)):
            print(f'{task["name"]}正在爬取第{i}页')
            url = f'https://searchapi.henan.gov.cn/open/api/external?keywords=&siteId=4500000001&pageNumber={i+1}&pageSize=15&channelMarkId={task_code}'
            response = self.get_res(url)
            resp = response.json()
            data = resp.get('data')
            datas = data.get('datas')
            for k in datas:
                html = BeautifulSoup(self.get_res(k['selfUrl']).text, 'lxml')
                title = html.select('head meta[name="ArticleTitle"]')[0]['content']
                date = html.select('head meta[name="PubDate"]')[0]['content']
                url = html.select('head meta[name="Url"]')[0]['content']
                file_wh = html.select('tr .td-r')[5].get_text()
                print(url)
                # response = self.get_content_res(url)
                # response.encoding = response.apparent_encoding
                # html = BeautifulSoup(response.text, "lxml")
                # print(response.text)
                content = self.get_content(html)
                rows: List[dict] = [
                    {
                        'id': '',
                        'cate_id': task['cate_id'],
                        'title': title,
                        'descript': '',
                        'file_wh': file_wh,
                        'images': '',
                        'content': content,
                        'createdate': self.get_timestamp(date),  # 创建时间，取爬取文章中的创建时间，转为时间戳格式
                        'status': 1,
                        'sort': 50,
                        'type': 1,
                        'attach': '',
                        'views': '',
                        'systemdate': '',
                        'url': url,
                        'attach_editor': '',
                        'laiyuan': task['laiyuan'],
                    },
                ]
                writer.writerows(rows)

    # 发起请求并设置重试次数为10次，最小等待时间为5秒，最大为20秒
    @retry(stop_max_attempt_number=50, wait_random_min=10000, wait_random_max=20000)
    def get_res(self, url):
        requests.packages.urllib3.disable_warnings()
        headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers = headers, verify=False)
        response.encoding = response.apparent_encoding
        return response

    # 发起请求并设置重试次数为10次，最小等待时间为5秒，最大为20秒
    @retry(stop_max_attempt_number=50, wait_random_min=10000, wait_random_max=20000)
    def get_content_res(self, url):
        response = requests.get(url, verify=False)
        response.encoding = response.apparent_encoding

        if response.status_code != 200:
            print(f'请求出现异常，正在进行第{self.retry_times}次重试')
            self.retry_times += 1
        else:
            if self.retry_times != 1:
                print('可以正常请求了，中途休息20秒')
                time.sleep(20)
            self.retry_times = 1

        return response

    # 获取总页数
    def get_total_page(self, url):
        res = self.get_res(url)
        resp = res.json()
        data = resp.get('data')
        # soup = BeautifulSoup(res.text, 'lxml')
        # pagesize = soup.select('#pageDec')[0].attrs['pagesize']
        # pagecount = soup.select('#pageDec')[0].attrs['pagecount']
        # total_page = math.ceil(int(pagecount) / int(pagesize))
        total_page = data.get('totalPage')
        return total_page

    # 日期转时间戳
    @staticmethod
    def get_timestamp(date):

        date_list = re.findall(r'\d+', date)
        date_list_len = len(date_list)
        if date_list_len == 1:
            dt = datetime.strptime(date_list[0], '%Y')
        elif date_list_len == 2:
            dt = datetime.strptime(date_list[0] + '-' + date_list[1], '%Y-%m')
        elif date_list_len == 3:
            dt = datetime.strptime(date_list[0] + '-' + date_list[1] + '-' + date_list[2], '%Y-%m-%d')
        try:
            utc_time = datetime.strptime("1970-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
            met_time = dt - utc_time
            createdate = str(int(met_time.days * 24 * 3600 + met_time.seconds))
        except:
            createdate = '0' * 10

        return createdate

    @staticmethod
    def get_laiyuan(html):
        source = html.select('.article.oneColumn.pub_border > div.pages-date > span')[0].get_text()[
                 3:].strip()
        if source == '府门户网站　www.gov.cn':
            source = html.select('.article.oneColumn.pub_border > div.pages-date > span')[1].get_text()[
                     3:].strip()
        elif source == '中央政府门户网站　www.gov.cn':
            source = html.select('.pages-date > span:nth-child(2)')[1].get_text()[3:].strip()
        return source

    @staticmethod
    def get_content(html):
        try:
            try:
                content = html.select('.rule_main')[0]
            except:
                content = html.select('.mt15.details-box.detail-file')[0]
        except:
            content = html.select('.mt15.details-box')[0]
            content.select('div.details-main > h6 > em')[0].decompose()
            content.select('div.details-main > h6 > span')[0].decompose()



        xggjbox = content.find_all(id='xggjbox')
        output = content.find_all(id='output')
        output_c = content.find_all(class_='output')
        if xggjbox:
            xggjbox[0].decompose()
        if output:
            output[0].decompose()
        if output_c:
            output_c[0].decompose()
        content = str(content).replace(u'\xa0', u'&nbsp;')
        return content

    def to_db(self):
        import pandas as pd
        from sqlalchemy import create_engine
        engine = create_engine('mysql+pymysql://root:123456@localhost:3306/kfzmt')
        listdir = os.listdir(f'./{self.file_name_csv}')
        db_info = {
            'user': 'root',
            'password': '123456',
            'host': 'localhost',
            'port': '3306',
            'database': 'kfzmt',
            # 'table': 'article'
            'table': 'article_221129'
        }
        count = 0
        for ls in listdir:
            df = pd.read_csv(f'./{self.file_name_csv}/{ls}')
            df['url'] = ''
            df_source = len(df)
            print(f'去重前的数据有{df_source}条')
            df.drop_duplicates(keep='first',inplace=True)
            print(f'去重后的数据有{len(df)}条')
            print(f'去重的数据有{df_source-len(df)}条')
            df.to_sql(db_info["table"], con=engine, index=False, if_exists='append')
            print(f'{ls}导入数据库{db_info["database"]}下的表{db_info["table"]}成功{len(df)}条。')
            count += len(df)
        print(f'此次共导入{count}条')


if __name__ == '__main__':
    # 开启多线程
    Task = Task()
    pool = ThreadPoolExecutor(max_workers=3)
    for task in Task.tasks_list:
        pool.submit(Task.config_init, task)

    # Task.config_init(Task.tasks_list[1])
    # 写入数据库
    # Task.to_db()
    # for task in Task.tasks_list:
    #     Task.config_init(task)
