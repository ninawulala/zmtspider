import csv
import os
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from datetime import datetime
from typing import List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from retrying import retry
import pandas as pd


class Task:
    def __init__(self):
        # 页的倍数，n为几 每页就拿n条数据，最多100条，那边做限制了。
        self.n = 100
        self.file_name_csv = 'Task_1666_csv'  + '_update'
        self.url = 'http://sousuo.gov.cn/s.htm?q=&t=zhengcelibrary_bm&orpro='
        # 获取内容列表url
        self.list_url = 'http://sousuo.gov.cn/data'
        self.retry_times = 1  # 重试次数
        self.fieldnames = ['id', 'cate_id', 'title', 'descript', 'file_wh', 'images', 'content', 'createdate', 'status',
                           'sort', 'type',
                           'attach', 'views', 'systemdate', 'url',
                           'attach_editor', 'laiyuan']

        self.task_1666_cate_id = '68'
        self.task_1666_name = 'task_1666'   + '_update'


    def config_init(self):
        res = self.get_res(0)
        total_page = res['searchVO']['totalpage']
        total_count = res['searchVO']['totalCount']
        page_size = res['searchVO']['pageSize']

        task_name = self.task_1666_name
        print(f'{task_name}的总条数为：{total_count}，总页数为：{total_page}')
        cp = ConfigParser()
        cp.add_section('db')  # 创建section db
        cp.set('db', 'total_count', str(total_count))  # 添加 total_page 到 db下
        cp.set('db', 'page_size', str(page_size))  # 添加 total_page 到 db下
        try:
            os.listdir('./config/')
        except:
            os.makedirs('./config/')

        cp.write(open(f"./config/{task_name}_config.ini", 'w'))  # 写入文件并保存
        # 开始爬取数据
        # pool = ThreadPoolExecutor(max_workers=3)
        # task_list = [pool.submit(self.get_csv_data, i) for i in range(total_page)]
        # for task in task_list:
        #     try:
        #         print(task.result())
        #     except Exception as e:
        #         traceback.print_exc(e)
        self.get_csv_data(total_page)

    def get_csv_data(self, pages):
        task_name = self.task_1666_name
        try:
            fw = open(f'./{self.file_name_csv}/{task_name}.csv', 'w', encoding='UTF8', newline='')
        except:
            os.makedirs(f'./{self.file_name_csv}/')
            fw = open(f'./{self.file_name_csv}/{task_name}.csv', 'w', encoding='UTF8', newline='')

        # f = open('contents_add.csv', 'w', encoding='UTF-8-sig', newline='')
        writer = csv.DictWriter(fw, fieldnames=self.fieldnames)
        writer.writeheader()

        for page in range(int(pages)):
            print(f'{task_name}正在爬取第{page}页')
            res = self.get_res(page)
            list_data = res['searchVO']['listVO']
            for li in list_data:
                title = li['title']
                # 用外面的发布日期
                try:
                    pubtime = li.get('pubtime')
                    createdate = str(pubtime)[:-3]
                except:
                    continue
                url = li['url']
                print(f'正在爬：{url}')
                response = self.get_content_res(url)
                response.encoding = response.apparent_encoding
                html = BeautifulSoup(response.text, "lxml")
                laiyuan = html.select('.policyLibraryOverview_header > table >  tr:nth-child(3) > td:nth-child(4)')[0].get_text().strip('网站')
                # if 'update' not in self.task_1666_name:
                #     content = self.get_content(url, html)
                # else:
                #     content = ''
                content = self.get_content(url, html)
                file_wh = li.get('pcode')
                rows: List[dict] = [
                    {
                        'id': '',
                        'cate_id': self.task_1666_cate_id,
                        'title': title,
                        'descript': '',
                        'file_wh': file_wh,
                        'images': '',
                        'content': content,
                        'createdate': createdate,  # 创建时间，取爬取文章中的发布时间，转为时间戳格式
                        'status': 1,
                        'sort': 50,
                        'type': 1,
                        'attach': '',
                        'views': '',
                        'systemdate': '',
                        'url': url,
                        'attach_editor': '',
                        'laiyuan': laiyuan,
                    },
                ]
                writer.writerows(rows)

    # 发起请求并设置重试次数为10次，最小等待时间为5秒，最大为20秒
    @retry(stop_max_attempt_number=50, wait_random_min=10000, wait_random_max=20000)
    def get_res(self, page):
        params = {
            't': 'zhengcelibrary_bm',
            'q': '',
            'timetype': 'timeqb',
            'mintime': '',
            'maxtime': '',
            'sort': 'pubtime',
            'sortType': '1',
            'searchfield': 'title',
            'pcodeJiguan': '',
            'childtype': '',
            'subchildtype': '',
            'tsbq': '',
            'pubtimeyear': '',
            'puborg': '',
            'pcodeYear': '',
            'pcodeNum': '',
            'filetype': '',
            'p': str(page),
            'n': self.n,
            'inpro': '',
            'bmfl': '',
            'dup': '',
            'orpro': '',
        }

        response = requests.get(self.list_url, params=params, verify=False)
        response.encoding = response.apparent_encoding

        if response.status_code != 200:
            print(f'请求出现异常，正在进行第{self.retry_times}次重试')
            self.retry_times += 1
        else:
            if self.retry_times != 1:
                print('可以正常请求了，中途休息20秒')
                time.sleep(20)
            self.retry_times = 1

        return response.json()

    # 发起请求并设置重试次数为10次，最小等待时间为5秒，最大为20秒
    @retry(stop_max_attempt_number=50, wait_random_min=10000, wait_random_max=20000)
    def get_content_res(self, url):
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

        return response


    @staticmethod
    def get_content(url, html):
        content = html.select('.pages_content')[0]

        href = content.select('p:nth-child(5) > span > a')
        if href:
            href = href[0]['href']
            content.select('p:nth-child(5) > span > a')[0]['href'] = urljoin(url, href)
        elif content.select('p:nth-child(6) > a'):
            try:
                content.select('p:nth-child(6) > a')[0]['href'] = urljoin(url, str(content.select('p:nth-child(6) > a')[0]['href']))
            except:
                print(f'url: {url}')
                pass
        elif content.select('p:nth-child(5) > a'):
            try:
                content.select('p:nth-child(5) > a')[0]['href'] = urljoin(url, str(content.select('p:nth-child(5) > a')[0]['href']))
            except:
                print(f'url: {url}')
                pass


        content = str(content).replace(u'\xa0', u'&nbsp;')
        return content

    # 更新表，包含url,不包含content
    def update(self):
        self.task_1666_name = 'task_1666_update'
        res = self.get_res(0)
        total_page = res['searchVO']['totalpage']
        self.get_csv_data(total_page)

    def get_update_content(self,url):
        print(f'正在更新:{url}')
        res = self.get_content_res(url)
        html = BeautifulSoup(res.text,'lxml')
        content = self.get_content(url,html)
        return content

    # 通过两次数量的差集判断是否更新
    def get_difference(self):
        # self.update()
        self.task_1666_name = 'task_1666_update'

        df_up = pd.read_csv('./Task_1666_csv/task_1666_update.csv')
        df = pd.read_csv('./Task_1666_csv/task_1666.csv')
        diff = pd.concat(objs=[df_up, df, df]).drop_duplicates(keep=False)
        len_diff = len(diff)
        if len_diff == 0:
            print('目前网站没有更新')
        else:
            print(f'此次有{len_diff}条更新')
            diff.content = diff.apply(lambda x: self.get_update_content(x.url), axis=1)
            diff.to_csv('./Task_1666_csv/task_1666_update.csv', index=False)
            df = pd.concat([diff, df])
            df.to_csv('./Task_1666_csv/task_1666.csv')




    def to_db(self):
        from sqlalchemy import create_engine
        db_info = {
            'user': 'root',
            'password': '123456',
            'host': 'localhost',
            'port': '3306',
            'database': 'kfzmt',
            'table': 'article_221129'
            # 'table': 'article'
        }
        engine = create_engine(f'mysql+pymysql://{db_info["user"]}:{db_info["password"]}@{db_info["host"]}:{db_info["port"]}/{db_info["database"]}')
        listdir = os.listdir(f'./{self.file_name_csv}')
        count = 0
        for ls in listdir:
            df = pd.read_csv(f'./{self.file_name_csv}/{ls}')
            df.to_sql(db_info["table"], con=engine, index=False, if_exists='append')
            print(f'{ls}导入数据库{db_info["database"]}下的表{db_info["table"]}成功{len(df)}条。')
            count += len(df)
        print(f'此次共导入{count}条')


if __name__ == '__main__':
    # 开启多线程
    Task = Task()
    Task.config_init()
    for task in Task.tasks_list:
        Task.config_init(task)
    # Task.get_difference()
    # Task.to_db()
