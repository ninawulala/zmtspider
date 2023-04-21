import csv
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from datetime import datetime
from math import ceil
from typing import List
from urllib import parse

import requests
from bs4 import BeautifulSoup
from retrying import retry


class Task:
    def __init__(self):
        self.name_csv = None
        self.file_name_csv = 'Task_2867_csv'  # + '_update'
        self.url = 'http://xxgk.www.gov.cn/search-zhengce'
        self.page_size = 10
        self.retry_times = 1  # 重试次数
        self.fieldnames = ['id', 'cate_id', 'title', 'descript', 'file_wh', 'images', 'content', 'createdate', 'status',
                           'sort', 'type',
                           'attach', 'views', 'systemdate', 'url',
                           'attach_editor', 'laiyuan']
        self.guoling = {
            'tag': '国令',
            'cate_id': '68',
            'laiyuan': '国务院',
            'name': 'task_2867_guoling'  # + '_update',
        }
        self.guofa = {
            'tag': '国发',
            'cate_id': '68',
            'laiyuan': '国务院',
            'name': 'task_2867_guofa'  # + '_update',
        }
        self.guofamd = {
            'tag': '国发明电',
            'cate_id': '68',
            'laiyuan': '国务院',
            'name': 'task_2867_guofamd'  # + '_update',
        }
        self.guobanfa = {
            'tag': '国办发',
            'cate_id': '68',
            'laiyuan': '国务院办公厅',
            'name': 'task_2867_guobanfa'  # + '_update',
        }
        self.guobanfamd = {
            'tag': '国办发明电',
            'cate_id': '68',
            'laiyuan': '国务院办公厅',
            'name': 'task_2867_guobanfamd'  # + '_update',
        }
        self.tasks_list = [self.guoling, self.guofa, self.guofamd, self.guobanfa, self.guobanfamd]

    def config_init(self, task: dict):
        res = self.get_res(page_index=1, task=task)
        count = res['count']
        total_page: int = ceil(count / self.page_size)
        task_name = task['name']
        print(f'{task_name} 总页数为：{total_page}，总条数为：{count}')

        cp = ConfigParser()
        try:
            cp.read(f"./config/{task_name}_config.ini", encoding='UTF-8-sig')
            count_db = int(cp.get('db', 'count'))
            print(count_db)
            if count_db == count:
                print('网站目前未更新')
            elif count > count_db:
                add_count = count - count_db
                print(f'网站更新了{add_count}条数据')
                cp.set('db', 'count', str(count))  # 添加 count 到 db下\
                oc = open(f"./config/{task_name}_config.ini", 'w')
                cp.write(oc)  # 写入文件并保存
                self.name_csv = 'add_' + task['name'] + '.csv'
                self.file_name_csv = 'add_' + self.file_name_csv
                add_page = ceil(add_count / self.page_size)
                print(add_page)
                self.get_csv_data(total_page=add_page, task=task)
                oc.close()
                #
                # self.concat(task=task)
            else:
                print(f'网站数据条目减少，具体以网站为准，接下来将重新爬取整个网站。。。。')
                self.name_csv = task['name'] + '.csv'
                self.get_csv_data(total_page=count, task=task)
        except:
            # 添加section
            cp.add_section('db')  # 创建section db
            cp.set('db', 'count', str(count))  # 添加 count 到 db下
            try:
                cp.write(open(f"./config/{task_name}_config.ini", 'w'))  # 写入文件并保存
            except:
                os.makedirs('./config/')
            # 开始爬取数据
            self.name_csv = task['name'] + '.csv'
            self.get_csv_data(task, total_page)

    # 获取列表，发起请求并设置重试次数为50次，最小等待时间为5秒，最大为20秒
    @retry(stop_max_attempt_number=50, wait_random_min=10000, wait_random_max=20000)
    def get_res(self, task, page_index, ):
        params = {
            'callback': 'jQuery112404784786152297411_1662687615617',
            'mode': 'smart',
            'sort': 'relevant',
            'page_index': str(page_index),
            'page_size': self.page_size,
            'title': '',
            'tag': task['tag'],
            '_': '1662687615629',
        }
        response = requests.get(self.url, params=params, verify=False)
        response.encoding = response.apparent_encoding
        res = response.text
        # 获取（）里面的内容
        rs = res[res.find('('):res.rfind(')')][1:]
        rs = json.loads(rs, strict=False)

        return rs

    # 获取内容，发起请求并设置重试次数为50次，最小等待时间为5秒，最大为20秒
    def get_content_res(self, content_url):
        url = content_url
        response = requests.get(url)
        response.encoding = response.apparent_encoding

        if response.status_code != 200:
            print(f'请求出现异常，正在进行第{self.retry_times}次重试')
            self.retry_times += 1
        else:
            if self.retry_times != 1:
                print('可以正常请求了，中途休息20秒')
                time.sleep(20)
            self.retry_times = 1

        return response.text

    # 数据持久化到csv文件中
    def get_csv_data(self, task, total_page):
        try:
            fw = open(f'./{self.file_name_csv}/{self.name_csv}', 'w', encoding='UTF8', newline='')
        except:
            os.makedirs(f'./{self.file_name_csv}/')
            fw = open(f'./{self.file_name_csv}/{self.name_csv}', 'w', encoding='UTF8', newline='')
        writer = csv.DictWriter(fw, fieldnames=self.fieldnames)
        writer.writeheader()
        for page_index in range(1, total_page + 1):
            task_tag = task['tag']
            print(f'{task_tag}————正在爬取第{page_index}页')
            res = self.get_res(task=task, page_index=page_index)
            data: list = res['data']
            for item in data:
                title = item['title']
                file_wh = item['tagno']
                pubtime = item['pubtime']
                createdate = int(self.get_timestamp(date=pubtime))
                if createdate < 0:  # 1970年之后的数据不要
                    continue

                url = item['url']
                print(url)

                content_res = self.get_content_res(content_url=url)
                html = BeautifulSoup(content_res, 'lxml')
                try:
                    content = html.select(
                        '.w1100 > div.wrap > table:nth-child(3) > tbody > tr > td:nth-child(1) > table > tbody > tr > td > table:nth-child(2) > tbody')[
                        0]
                except:
                    # http://www.gov.cn/zhengce/content/2005-05/23/content_8134.htm
                    content = html.select(
                        'body > div.w1100 > div.wrap > table:nth-child(3) > tbody > tr > td > table:nth-child(2) > tbody')[
                        0]
                content = str(content).replace(u'\xa0', u'&nbsp;')
                # 有的标题图片是相对路径
                img = html.select('#UCAP-CONTENT > div:nth-child(2) > img')
                if img:
                    src = img[0].attrs['src']
                    img[0]['src'] = parse.urljoin(url, src)

                rows: List[dict] = [
                    {
                        'id': '',
                        'cate_id': '68',
                        'title': title,
                        'descript': '',
                        'file_wh': file_wh,
                        'images': '',
                        'content': content,
                        'createdate': createdate,  # 创建时间，取爬取文章中的创建时间，转为时间戳格式
                        'status': 1,
                        'sort': 50,
                        'type': 1,
                        'attach': '',
                        'views': '',
                        'systemdate': '',
                        # 'url': url,
                        'url': '',
                        'attach_editor': '',
                        'laiyuan': task['laiyuan'],
                    },
                ]
                writer.writerows(rows)

        return

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
            createdate = '0' * 13

        return createdate

    # 合并新增的数据到csv
    def concat(self, task):
        import pandas as pd
        task_name = task['name']
        try:
            add_csv = pd.read_csv(f'./{self.file_name_csv}/{self.name_csv}')
            file_name_csv = self.file_name_csv.strip('add_')
            name_csv = self.name_csv.strip('add_')
            _csv = pd.read_csv(f'./{file_name_csv}/{name_csv}')
            df = pd.concat([add_csv, _csv])
            df.drop_duplicates(inplace=True)
            df.to_csv(f'./{file_name_csv}/{name_csv}', index=False)
            print(f'{task_name} 已经完成更新')
        except:
            print(f'{task_name} 没有找到需要更新的csv文件')

    def to_db(self):
        import pandas as pd
        from sqlalchemy import create_engine
        engine = create_engine('mysql+pymysql://root:123456@localhost:3306/kfzmt?charset=utf8')
        listdir = os.listdir(f'./{self.file_name_csv}')
        db_info = {
            'user': 'root',
            'password': '123456',
            'host': 'localhost',
            'port': '3306',
            'database': 'kfzmt',
            'table': 'article_221129'
            # 'table': 'article'
        }
        count = 0
        for ls in listdir:
            df = pd.read_csv(f'./{self.file_name_csv}/{ls}')
            df.to_sql(db_info['table'], con=engine, index=False, if_exists='append')
            print(f'{ls}导入数据库{db_info["database"]}下的表{db_info["table"]}成功{len(df)}条。')
            count += len(df)
        print(f'此次共导入{count}条')


if __name__ == '__main__':
    Task = Task()
    pool = ThreadPoolExecutor(max_workers=30)
    for task in Task.tasks_list:
        pool.submit(Task.config_init, task)

    # Task.to_db()
